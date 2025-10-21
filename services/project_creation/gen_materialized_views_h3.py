import pandas as pd
from sqlalchemy import create_engine, text
from classes.db_config import DBConfig

# Load DB config
config = DBConfig()
db_url = (
    f"postgresql://{config.DATABASE_USER}:"
    f"{config.DATABASE_PASSWORD}@"
    f"{config.DATABASE_HOST}/{config.DATABASE_NAME}"
)
engine = create_engine(db_url)

"""
Creates materialized views for each project area and resolution in bioprotect.h3_cells,
and inserts metadata into bioprotect.metadata_planning_units.
"""
print("🔄 Creating materialized views and metadata for H3 cells...")

with engine.begin() as conn:  # ensures transaction
    result = conn.execute(text("""
        SELECT DISTINCT project_area, resolution
        FROM bioprotect.h3_cells
        ORDER BY project_area, resolution;
    """))
    rows = result.fetchall()

    for project_area, resolution in rows:
        scale_safe = project_area.lower().replace(
            " ", "_").replace("-", "_").replace("/", "_")
        view_name = f"v_h3_{scale_safe}_res{resolution}"

        print(f"🧱 Creating materialized view: {view_name}")

        # 1. Drop existing materialized view if it exists

        conn.execute(
            text(
                f"DROP VIEW IF EXISTS bioprotect.{view_name} CASCADE")
        )

        # 2. Create the new materialized view
        conn.execute(text(f"""
            CREATE MATERIALIZED VIEW bioprotect.{view_name} AS
            SELECT h3_index, resolution, scale_level, project_area, geometry
            FROM bioprotect.h3_cells
            WHERE project_area = :area AND resolution = :res;
        """), {"area": project_area, "res": resolution})

        # 3. Create spatial index
        conn.execute(text(f"""
            CREATE INDEX idx_{view_name}_geom
            ON bioprotect.{view_name}
            USING GIST (geometry);
        """))

        # 4. Check if metadata already exists
        exists = conn.execute(text("""
            SELECT 1 FROM bioprotect.metadata_planning_units
            WHERE feature_class_name = :view_name
        """), {"view_name": view_name}).fetchone()

        if exists:
            print(
                f"ℹ️ Metadata for {view_name} already exists. Skipping insert.")
            continue

        print(f"📥 Inserting metadata for: {project_area} (res {resolution})")
        conn.execute(text(f"""
            INSERT INTO bioprotect.metadata_planning_units (
                feature_class_name,
                alias,
                description,
                country_id,
                aoi_id,
                domain,
                _area,
                envelope,
                creation_date,
                source,
                created_by,
                tilesetid,
                planning_unit_count
            )
            SELECT
                :view_name,
                :alias,
                :description,
                1,
                1,
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
            "alias": f"{project_area} (Res {resolution})",
            "description": f"Auto-generated materialized H3 grid at res {resolution} for {project_area}"
        })

    print("✅ Done creating materialized views and metadata entries.")
