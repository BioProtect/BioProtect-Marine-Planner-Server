import h3
import geopandas as gpd
from shapely.geometry import Polygon
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from classes.db_config import DBConfig

# Database setup
config = DBConfig()
db_url = f"postgresql://{config.DATABASE_USER}:{config.DATABASE_PASSWORD}@{config.DATABASE_HOST}/{config.DATABASE_NAME}"
engine = create_engine(db_url)


def normalize_name(name):
    return name.lower().replace(" ", "_").replace("-", "_").replace("/", "_")


def get_scale_level(res):
    return "basin" if res <= 6 else "regional" if res == 7 else "local"


def create_project_from_shapefile(shapefile, project_name, resolution=8):
    print(f"📍 Creating project: {project_name} (res {resolution})")

    # Get the first user ID
    with engine.begin() as conn:
        result = conn.execute(
            text("SELECT id FROM bioprotect.users ORDER BY id LIMIT 1"))
        user_id = result.scalar()
        if not user_id:
            raise Exception("❌ No user found in database.")

    # Generate H3 cells
    df = gpd.read_file(shapefile)
    scale_level = get_scale_level(resolution)
    records = []

    for _, row in df.iterrows():
        geom = row["geometry"]
        name = row.get("name", project_name)  # fallback
        parts = [geom] if geom.geom_type == "Polygon" else list(geom.geoms)
        for part in parts:
            for h in h3.geo_to_cells(part.__geo_interface__, resolution):
                coords = [(lon, lat) for lat, lon in h3.cell_to_boundary(h)]
                records.append({
                    "h3_index": h,
                    "resolution": resolution,
                    "scale_level": scale_level,
                    "project_area": project_name,
                    "geometry": Polygon(coords),
                    "cost": 1.0,
                    "status": 0
                })

    gdf_out = gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:4326")
    gdf_out.to_postgis("h3_cells", engine, schema="bioprotect",
                       if_exists="append", index=False)

    # Create view
    view_name = f"v_h3_{normalize_name(project_name)}_res{resolution}"
    with engine.begin() as conn:
        conn.execute(
            text(f"DROP VIEW IF EXISTS bioprotect.{view_name} CASCADE"))
        conn.execute(text(f"""
            CREATE VIEW bioprotect.{view_name} AS
            SELECT h3_index, resolution, scale_level, project_area, geometry, cost, status
            FROM bioprotect.h3_cells
            WHERE project_area = :area AND resolution = :res;
        """), {"area": project_name, "res": resolution})

        # Insert metadata for planning units
        conn.execute(text(f"""
            INSERT INTO bioprotect.metadata_planning_units (
                feature_class_name, alias, description, domain, _area,
                envelope, creation_date, source, created_by, tilesetid, planning_unit_count
            )
            SELECT
                :view_name,
                :alias,
                :description,
                'marine',
                0,
                ST_Multi(ST_Envelope(ST_Collect(geometry))),
                NOW(),
                'h3_cells',
                'system',
                :view_name,
                COUNT(*)
            FROM bioprotect.{view_name}
            ON CONFLICT (feature_class_name) DO NOTHING;
        """), {
            "view_name": view_name,
            "alias": f"{project_name} (Res {resolution})",
            "description": f"Auto-generated H3 grid for {project_name}"
        })

        # Fetch inserted planning_unit_id
        planning_unit_id = conn.execute(text("""
            SELECT unique_id FROM bioprotect.metadata_planning_units
            WHERE feature_class_name = :view_name
        """), {"view_name": view_name}).scalar()

        # Insert project
        conn.execute(text("""
            INSERT INTO bioprotect.projects (
                user_id, name, description, date_created,
                planning_unit_id, old_version, iucn_category, is_private,
                costs, default_resolution
            ) VALUES (
                :user_id, :name, :desc, NOW(),
                :planning_unit_id, NULL, NULL, FALSE,
                'Equal Area', :resolution
            )
        """), {
            "user_id": user_id,
            "name": project_name,
            "desc": f"Project for {project_name}",
            "planning_unit_id": planning_unit_id,
            "resolution": resolution
        })

        # Get the new project ID
        project_id = conn.execute(text("""
            SELECT id FROM bioprotect.projects
            WHERE name = :name AND planning_unit_id = :planning_unit_id
            ORDER BY id DESC LIMIT 1
        """), {"name": project_name, "planning_unit_id": planning_unit_id}).scalar()

        # Link project to user
        conn.execute(text("""
            INSERT INTO bioprotect.user_projects (user_id, project_id)
            VALUES (:user_id, :project_id)
        """), {"user_id": user_id, "project_id": project_id})

        # Populate project_pus
        conn.execute(text("""
            INSERT INTO bioprotect.project_pus (project_id, h3_index, cost, status)
            SELECT :project_id, h3_index, cost, status
            FROM bioprotect.h3_cells
            WHERE project_area = :area AND resolution = :res
        """), {"project_id": project_id, "area": project_name, "res": resolution})

    print(f"✅ Project '{project_name}' created successfully.")


# Example usage
if __name__ == "__main__":
    create_project_from_shapefile(
        "./data/custom/tralee_bay.shp",
        project_name="Tralee Bay",
        resolution=8
    )
