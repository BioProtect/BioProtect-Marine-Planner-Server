import os

import pandas as pd
from handlers.websocket_handler import SocketHandler
from services.service_error import ServicesError


# Batch sizes keep every query well under the pool's 120 s cap (prod is ~3x
# slower than dev). Sublittoral Sediments (58k polygons) on dev: see
# migrations/0019_feature_pieces.sql.
PIECE_BATCH_POINTS = 200_000
CELL_BATCH_HITS = 20000


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
        super().initialize(pg=pg)

    async def run(self):
        try:
            feature_id = int(self.get_argument("feature_id"))
            await self.start({'info': f"Preprocessing feature {feature_id}.."})
        except ServicesError:
            return  # Authentication/authorization error

        self.validate_args(self.request.arguments, [
                           'project_id', 'feature_id', 'planning_grid_id'])

        project_id = int(self.get_argument("project_id"))
        feature_id = int(self.get_argument("feature_id"))
        planning_grid_id = int(self.get_argument("planning_grid_id"))

        row = await self.pg.execute(
            """
            SELECT feature_class_name
            FROM bioprotect.metadata_interest_features
            WHERE unique_id = %s
            """,
            [feature_id],
            return_format="Dict"
        )

        if not row:
            raise ServicesError(f"No feature found for id {feature_id}")

        feature_class = row[0]["feature_class_name"]

        try:
            geometry_type = await self.pg.get_geometry_type(feature_class)
            if geometry_type == "ST_Point":
                # the frontend resolves on the Finished message, so the result rides on it
                self.close({
                    'info': f"Feature '{feature_class}' is a point layer – no preprocessing required",
                    'feature_class': feature_class,
                    'id': feature_id,
                    'pu_area': 0,
                    'pu_count': 0,
                })
                return

            await self.pg.execute(
                "SELECT bioprotect.clear_feature_data(%s, %s)", [project_id, feature_id])
            try:
                await self._build_pieces(feature_id, feature_class)
                await self._insert_amounts(
                    project_id, feature_id, planning_grid_id, feature_class)
            except ServicesError:
                # partial amounts would look like a finished preprocess
                await self.pg.execute(
                    "SELECT bioprotect.clear_feature_data(%s, %s)", [project_id, feature_id])
                raise

            await self.pg.execute(
                "SELECT bioprotect.aggregate_feature_stats(%s, %s)", [project_id, feature_id])
            stats = await self.pg.execute(
                """
                SELECT pu_area, pu_count
                FROM bioprotect.feature_preprocessing
                WHERE project_id = %s AND feature_unique_id = %s
                """,
                [project_id, feature_id],
                return_format="Dict"
            )
            self.close({
                'info': f"Feature '{feature_class}' preprocessed",
                'feature_class': feature_class,
                'id': feature_id,
                'pu_area': stats[0]["pu_area"] if stats else 0,
                'pu_count': stats[0]["pu_count"] if stats else 0,
            })

        except ServicesError as e:
            self.send_response({'error': e.args[0]})
            self.close(clean=False)

    async def _build_pieces(self, feature_id, feature_class):
        """Cut the feature into shared pieces once; reused by every project."""
        batches = await self.pg.execute(
            "SELECT from_id, to_id FROM bioprotect.feature_piece_batches(%s, %s, %s)",
            [feature_id, feature_class, PIECE_BATCH_POINTS], return_format="Array")
        for i, b in enumerate(batches, 1):
            self.send_response({
                'status': 'Preprocessing',
                'info': f"Preparing feature geometry ({i}/{len(batches)})..",
            })
            await self.pg.execute(
                "SELECT bioprotect.build_feature_pieces(%s, %s, %s, %s)",
                [feature_id, feature_class, b["from_id"], b["to_id"]])

    async def _insert_amounts(self, project_id, feature_id, planning_grid_id, feature_class):
        batches = await self.pg.execute(
            "SELECT lo, hi FROM bioprotect.feature_cell_batches(%s, %s, %s)",
            [feature_id, planning_grid_id, CELL_BATCH_HITS], return_format="Array")
        for i, b in enumerate(batches, 1):
            self.send_response({
                'status': 'Preprocessing',
                'info': f"Intersecting with planning units ({i}/{len(batches)})..",
            })
            await self.pg.execute(
                "SELECT bioprotect.insert_feature_pu_amounts(%s, %s, %s, %s, 'ST_Polygon', %s, %s)",
                [project_id, feature_id, planning_grid_id, feature_class, b["lo"], b["hi"]])
