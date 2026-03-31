# handlers/drawing_handlers.py

import json
import uuid
import logging

from tornado.escape import json_decode
from passlib.hash import bcrypt
from psycopg2 import sql

from handlers.base_handler import BaseHandler
from services.service_error import ServicesError, raise_error
from classes.postgis_class import get_pg
from services.martin_service import restart_martin
from handlers.notification_handler import broadcast


MAPBOX_USER = "craicerjack"  # keep consistent with app.py

_pg = None


class BioProtectEngageHandler(BaseHandler):
    def initialize(self, pg):
        super().initialize(pg=pg)

    async def post(self):
        """
        Handles POST requests for planning unit updates.
        """
        try:
            action = self.get_argument('action', None)
            if action == 'create-feature':
                await self.create_feature()
            else:
                raise ServicesError("Invalid action specified.")

        except ServicesError as e:
            raise_error(self, e.args[0])

    async def create_feature(self):
        """
        POST /server/engage?action=create-feature
        Body:
        {
            "user": "...",
            "userGroup": "...",
            "name": "...",
            "description": "...",
            "density": 1,
            "geometry": { ... valid Polygon/MultiPolygon GeoJSON ... }
        }
        """
        try:
            body = json_decode(self.request.body or b"{}")

            user = body.get("user")
            usergroup = body.get("userGroup")
            name = body.get("name")
            description = body.get("description", "")
            density = body.get("density", 1)
            geometry = body.get("geometry")

            if not name or not geometry:
                self.set_status(400)
                self.send_response({
                    "error": "Missing required fields",
                    "info": "Required fields: name, geometry",
                    "type": "error"
                })
                return

            geometry_json = json.dumps(geometry)

            # Same pattern as Flask: create dedicated feature table
            feature_class_name = "f_" + uuid.uuid4().hex[:30]
            tileset_id = f"{MAPBOX_USER}.{feature_class_name}"

            euniscombd = None
            msfd_bbht = None
            unique_eun = None

            # CREATE TABLE bioprotect.<feature_class_name> ...
            # create_sql = sql.SQL(
            #     """
            #     CREATE TABLE bioprotect.{table} AS
            #     SELECT
            #         %s::text   AS euniscombd,
            #         %s::text   AS msfd_bbht,
            #         %s::text   AS unique_eun,
            #         %s::decimal AS density,
            #         ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326) AS geometry;
            #     """
            # ).format(table=sql.Identifier(feature_class_name))
            create_sql = sql.SQL(
                """
                CREATE TABLE bioprotect.{table} AS
                SELECT
                    %s::text   AS euniscombd,
                    %s::text   AS msfd_bbht,
                    %s::text   AS unique_eun,
                    %s::decimal AS density,
                    ST_SetSRID(
                        ST_GeomFromGeoJSON(%s),
                        4326
                    ) AS geometry
                """
            ).format(table=sql.Identifier(feature_class_name))

            res = await self.pg.execute(
                create_sql,
                [euniscombd, msfd_bbht, unique_eun, density, geometry_json],
            )

            ##########
            ##########
            ##########

            # Spatial index
            index_name = f"idx_{uuid.uuid4().hex}"
            await self.pg.execute(
                sql.SQL(
                    "CREATE INDEX {} ON bioprotect.{} USING GIST (geometry);"
                ).format(
                    sql.Identifier(index_name),
                    sql.Identifier(feature_class_name),
                )
            )

            # Make sure table has a SERIAL id
            await self.pg.execute(
                sql.SQL(
                    "ALTER TABLE bioprotect.{} "
                    "DROP COLUMN IF EXISTS id, DROP COLUMN IF EXISTS ogc_fid;"
                ).format(sql.Identifier(feature_class_name))
            )
            await self.pg.execute(
                sql.SQL(
                    "ALTER TABLE bioprotect.{} "
                    "ADD COLUMN id SERIAL PRIMARY KEY;"
                ).format(sql.Identifier(feature_class_name))
            )

            # Insert into metadata_interest_features in one statement

            meta_sql = sql.SQL("""
                INSERT INTO bioprotect.metadata_interest_features (
                    feature_class_name,
                    alias,
                    description,
                    creation_date,
                    _area,
                    tilesetid,
                    extent,
                    source,
                    created_by
                )
                SELECT                    
                    %s AS feature_class_name,
                    %s AS alias,
                    %s AS description,
                    now(),
                    ST_Area(ST_Transform(geometry, 3410)) AS _area,
                    %s AS tilesetid,
                    box2d(geometry) AS extent,
                    %s AS source,
                    %s AS created_by                    
                    FROM bioprotect.{table}
                        LIMIT 1
                        RETURNING unique_id;
            """).format(table=sql.Identifier(feature_class_name))

            meta_rows = await self.pg.execute(
                meta_sql,
                [feature_class_name, name, description,
                 tileset_id, usergroup, user],
                return_format="Array",
            )

            unique_id = meta_rows[0]["unique_id"] if meta_rows else None

            restart_martin()

            broadcast("feature-created", {
                "featureClassName": feature_class_name,
                "metadataId": unique_id,
            })

            self.send_response(
                {
                    "info": "Feature created successfully",
                    "message": "Feature saved successfully",
                    "featureClassName": feature_class_name,
                    "metadataId": unique_id,
                    "type": "success"
                }
            )
        except ServicesError as e:
            print('e: ', e)
            print('e.args[0]: ', e.args[0])
            raise_error(self, e.args[0])
        except Exception as e:
            print('e: ', e)
            logging.exception("Error saving polygon")
            self.set_status(500)
            self.send_response({"error": str(e)})
