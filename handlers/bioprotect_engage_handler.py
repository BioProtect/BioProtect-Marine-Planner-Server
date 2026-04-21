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

    async def get(self):
        try:
            action = self.get_argument('action', None)
            if action == 'list-features':
                await self.list_features()
            else:
                raise ServicesError("Invalid action specified.")
        except ServicesError as e:
            raise_error(self, e.args[0])

    async def list_features(self):
        rows = await self.pg.execute(
            "SELECT feature_class_name, alias, tilesetid FROM bioprotect.metadata_interest_features ORDER BY creation_date DESC",
            return_format="Array",
        )
        self.send_response({"features": rows or []})

    async def post(self):
        try:
            action = self.get_argument('action', None)
            if action == 'create-feature':
                await self.create_feature()
            elif action == 'create-activity':
                await self.create_activity()
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

            euniscombd = body.get("euniscombd", None)
            msfd_bbht = None
            unique_eun = None

            # Check if a feature with the same name already exists for this user
            existing = await self.pg.execute(
                """
                SELECT feature_class_name, unique_id
                FROM bioprotect.metadata_interest_features
                WHERE alias = %s AND created_by = %s
                LIMIT 1
                """,
                [name, user],
                return_format="Array",
            )

            if existing:
                existing_table = existing[0]["feature_class_name"]
                unique_id = existing[0]["unique_id"]

                await self.pg.execute(
                    sql.SQL(
                        """
                        INSERT INTO bioprotect.{table}
                            (euniscombd, msfd_bbht, unique_eun, density, geometry)
                        VALUES (%s, %s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))
                        """
                    ).format(table=sql.Identifier(existing_table)),
                    [euniscombd, msfd_bbht, unique_eun, density, geometry_json],
                )

                await self.pg.execute(
                    sql.SQL(
                        """
                        UPDATE bioprotect.metadata_interest_features SET
                            _area = (SELECT SUM(ST_Area(ST_Transform(geometry, 3410)))
                                     FROM bioprotect.{table}),
                            extent = (SELECT box2d(ST_Collect(geometry))
                                      FROM bioprotect.{table})
                        WHERE feature_class_name = %s
                        """
                    ).format(table=sql.Identifier(existing_table)),
                    [existing_table],
                )

                restart_martin()
                broadcast("feature-updated", {
                    "featureClassName": existing_table,
                    "metadataId": unique_id,
                })
                self.send_response({
                    "info": "Feature merged successfully",
                    "message": "Polygon merged into existing feature",
                    "featureClassName": existing_table,
                    "metadataId": unique_id,
                    "type": "success",
                })
                return

            # No existing feature — create a new dedicated table
            feature_class_name = "f_" + uuid.uuid4().hex[:30]
            tileset_id = f"{MAPBOX_USER}.{feature_class_name}"

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

    async def create_activity(self):
        """
        POST /server/engage?action=create-activity
        Saves a drawn activity polygon into metadata_activities and creates
        pressures via the PostGIS function, making it available in the main app.
        Body: { user, userGroup, name, description, density, timestamp, geometry }
        """
        try:
            body = json_decode(self.request.body or b"{}")

            user = body.get("user")
            usergroup = body.get("userGroup")
            name = body.get("name")
            description = body.get("description", "")
            geometry = body.get("geometry")

            if not name or not geometry:
                self.set_status(400)
                self.send_response({
                    "error": "Missing required fields",
                    "info": "Required fields: name, geometry",
                    "type": "error",
                })
                return

            geometry_json = json.dumps(geometry)

            # Check if an activity with the same name already exists for this user
            existing = await self.pg.execute(
                """
                SELECT activity_name, id
                FROM bioprotect.metadata_activities
                WHERE activity = %s AND created_by = %s
                LIMIT 1
                """,
                [name, user],
                return_format="Array",
            )

            if existing:
                existing_table = existing[0]["activity_name"]
                activity_id = existing[0]["id"]

                await self.pg.execute(
                    sql.SQL(
                        "INSERT INTO bioprotect.{table} (geometry) "
                        "VALUES (ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))"
                    ).format(table=sql.Identifier(existing_table)),
                    [geometry_json],
                )

                await self.pg.execute(
                    sql.SQL(
                        """
                        UPDATE bioprotect.metadata_activities SET
                            extent = (SELECT Box2D(ST_Extent(geometry))
                                      FROM bioprotect.{table})
                        WHERE id = %s
                        """
                    ).format(table=sql.Identifier(existing_table)),
                    [activity_id],
                )

                await self.pg.execute(
                    "SELECT bioprotect.create_pressures_from_activity(%s);",
                    [activity_id],
                )

                broadcast("activity-updated", {"activityId": activity_id})
                self.send_response({
                    "info": "Activity merged successfully",
                    "message": "Polygon merged into existing activity",
                    "activityName": existing_table,
                    "activityId": activity_id,
                    "type": "success",
                })
                return

            # No existing activity — create a new table
            activity_table = "activity_" + uuid.uuid4().hex[:23]

            await self.pg.execute(
                sql.SQL(
                    "CREATE TABLE bioprotect.{table} AS "
                    "SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326) AS geometry"
                ).format(table=sql.Identifier(activity_table)),
                [geometry_json],
            )

            # Spatial index + primary key
            index_name = f"idx_{uuid.uuid4().hex}"
            await self.pg.execute(
                sql.SQL(
                    "CREATE INDEX {} ON bioprotect.{} USING GIST (geometry);"
                ).format(
                    sql.Identifier(index_name),
                    sql.Identifier(activity_table),
                )
            )
            await self.pg.execute(
                sql.SQL(
                    "ALTER TABLE bioprotect.{} ADD COLUMN id SERIAL PRIMARY KEY;"
                ).format(sql.Identifier(activity_table))
            )

            # Insert metadata
            meta_rows = await self.pg.execute(
                sql.SQL("""
                    INSERT INTO bioprotect.metadata_activities
                        (description, creation_date, source, created_by,
                         filename, activity, activity_name, extent)
                    SELECT %s, now(), %s, %s, %s, %s, %s,
                           Box2D(ST_Extent(geometry))
                    FROM bioprotect.{table}
                    RETURNING id;
                """).format(table=sql.Identifier(activity_table)),
                [description, usergroup, user,
                 activity_table + ".draw", name, activity_table],
                return_format="Array",
            )

            activity_id = meta_rows[0]["id"] if meta_rows else None

            # Create pressures from PAD table
            if activity_id:
                await self.pg.execute(
                    "SELECT bioprotect.create_pressures_from_activity(%s);",
                    [activity_id],
                )

            broadcast("activity-created", {
                "activityName": activity_table,
                "activityId": activity_id,
            })

            self.send_response({
                "info": "Activity created successfully",
                "message": "Activity saved and pressures created",
                "activityName": activity_table,
                "activityId": activity_id,
                "type": "success",
            })

        except ServicesError as e:
            raise_error(self, e.args[0])
        except Exception as e:
            logging.exception("Error saving activity")
            self.set_status(500)
            self.send_response({"error": str(e)})
