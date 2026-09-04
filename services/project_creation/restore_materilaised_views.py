from sqlalchemy import create_engine, text
from classes.db_config import DBConfig

config = DBConfig()
db_url = (
    f"postgresql://{config.DATABASE_USER}:"
    f"{config.DATABASE_PASSWORD}@"
    f"{config.DATABASE_HOST}/{config.DATABASE_NAME}"
)
engine = create_engine(db_url)

print("🔄 Recreating materialized views from h3_cells...")

with engine.begin() as conn:
    rows = conn.execute(text("""
        SELECT DISTINCT project_area, resolution
        FROM bioprotect.h3_cells
        ORDER BY project_area, resolution;
    """)).fetchall()

    for project_area, resolution in rows:
        safe = project_area.lower().replace(" ", "_").replace("-", "_").replace("/", "_")
        view_name = f"v_h3_{safe}_res{resolution}"

        # 🔎 Check if materialized view already exists
        exists = conn.execute(text("""
            SELECT 1
            FROM pg_matviews
            WHERE schemaname = 'bioprotect'
            AND matviewname = :view_name
        """), {"view_name": view_name}).fetchone()

        if exists:
            print(f"⏭ Skipping {view_name} (already exists)")
            continue

        print(f"🧱 Creating materialized view {view_name}")

        conn.execute(text(f"""
            CREATE MATERIALIZED VIEW bioprotect.{view_name} AS
            SELECT h3_index, resolution, scale_level, project_area, geometry
            FROM bioprotect.h3_cells
            WHERE project_area = :area AND resolution = :res;
        """), {"area": project_area, "res": resolution})

        conn.execute(text(f"""
            CREATE INDEX idx_{view_name}_geom
            ON bioprotect.{view_name}
            USING GIST (geometry);
        """))

print("✅ Done.")
