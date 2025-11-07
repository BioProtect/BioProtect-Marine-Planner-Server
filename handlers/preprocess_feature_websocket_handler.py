import os

import pandas as pd
from handlers.websocket_handler import SocketHandler
from services.service_error import ServicesError


class PreprocessFeature(SocketHandler):
    """
    REST WebSocket Handler. Preprocesses features by intersecting them with planning units.
    Conservation algorithms need to know the amount of a feature within a polygon to work.
    This function intersects the features with the planning units and stores the results in the database.
    Reduces the need for this processing to be done in priotizr.

    Populates `pu_feature_amounts` and
    `feature_preprocessing` tables.

    Required args:
      - project_id (int)
      - feature_id (int)
      - feature_class_name (text)
      - planning_grid_id

    Returns:
        dict: WebSocket messages with keys:
            - "info": Detailed progress messages.
            - "feature_class_name": The name of the preprocessed feature class.
            - "id": The feature id.
            - "pu_area": Total area of the feature in the planning grid.
            - "pu_count": Total number of planning grids intersecting the feature.
    """

    def initialize(self, pg):
        super().initialize()
        self.pg = pg

    async def open(self):
        try:
            feature_id = int(self.get_argument("feature_id"))
            await super().open({'info': f"Preprocessing feature {feature_id}.."})
        except ServicesError:
            return  # Authentication/authorization error

        self.validate_args(self.request.arguments, [
                           'project_id', 'feature_id', 'feature_class_name', 'planning_grid_id'])

        project_id = int(self.get_argument("project_id"))
        feature_id = int(self.get_argument("feature_id"))
        feature_class = self.get_argument("feature_class_name")
        planning_grid_id = self.get_argument("planning_grid_id")

        try:
            # Determine geometry type
            geometry_type = await self.pg.get_geometry_type(feature_class)
            if geometry_type not in ("ST_Point",):
                geometry_type = "ST_Polygon"

                # 1. Clear existing data
                await self.pg.execute(
                    "SELECT bioprotect.clear_feature_data(%s, %s)", [project_id, feature_id])

                # 2. Insert intersections
                await self.pg.execute(
                    "SELECT bioprotect.insert_feature_pu_amounts(%s, %s, %s, %s, %s)",
                    [project_id, feature_id, planning_grid_id, feature_class, geometry_type])

                # 3. Aggregate Stats
                await self.pg.execute(
                    "SELECT bioprotect.aggregate_feature_stats(%s, %s)", [project_id, feature_id])

                # 4. Fetch summary
                stats = await self.pg.execute(
                    """
                    SELECT pu_area, pu_count
                    FROM bioprotect.feature_preprocessing
                    WHERE project_id = %s AND feature_unique_id = %s
                    """,
                    [project_id, feature_id],
                    return_format="Dict"
                )
                pu_area = stats[0]["pu_area"] if stats else 0
                pu_count = stats[0]["pu_count"] if stats else 0

                # Final response
                self.send_response({
                    'info': f"Feature '{feature_class}' preprocessed",
                    'feature_class': feature_class,
                    'id': feature_id,
                    'pu_area': pu_area,
                    'pu_count': pu_count
                })

        except ServicesError as e:
            self.send_response({'error': e.args[0]})
