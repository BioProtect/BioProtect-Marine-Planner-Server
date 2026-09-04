from sqlalchemy import create_engine, text
from classes.db_config import DBConfig
from datetime import datetime

# Load DB config and connect
config = DBConfig()
db_url = (
    f"postgresql://{config.DATABASE_USER}:"
    f"{config.DATABASE_PASSWORD}@"
    f"{config.DATABASE_HOST}/{config.DATABASE_NAME}"
)
engine = create_engine(db_url)

# Get all unique ICES regions
regions = engine.execute("""
    SELECT DISTINCT project_area
    FROM bioprotect.h3_cells
    WHERE scale_level = 'regional'
""").fetchall()

for (region,) in regions:
    safe_name = region.lower().replace(" ", "_").replace("-", "_").replace("/", "_")
    planning_unit_table = f"pu_{safe_name}"

    print(f"Creating project for: {region}")

    # 1. Create planning unit table
    engine.execute(text(f"""
        DROP TABLE IF EXISTS bioprotect.{planning_unit_table};
        CREATE TABLE bioprotect.{planning_unit_table} AS
        SELECT row_number() OVER () AS puid, geometry
        FROM bioprotect.h3_cells
        WHERE project_area = :region AND scale_level = 'regional';
    """), {'region': region})

    # 2. Insert into metadata_planning_units
    metadata_result = engine.execute(text(f"""
        INSERT INTO bioprotect.metadata_planning_units (
            feature_class_name, alias, description, domain, _area, envelope,
            creation_date, source, created_by, planning_unit_count
        )
        SELECT
            :feature_class_name, :alias, :description, 'marine',
            SUM(ST_Area(geometry::geography)) / 1000000.0,
            ST_Envelope(ST_Collect(geometry)), :created,
            'ICES ecoregions', 'system', COUNT(*)
        FROM bioprotect.{planning_unit_table}
        RETURNING unique_id
    """), {
        'feature_class_name': planning_unit_table,
        'alias': region,
        'description': f"Planning units for {region}",
        'created': datetime.utcnow()
    }).fetchone()

    planning_unit_id = metadata_result[0]

    # 3. Insert into projects
    project_result = engine.execute(text("""
        INSERT INTO bioprotect.projects (
            user_id, name, description, date_created,
            planning_unit_id, old_version, iucn_category,
            is_private, costs
        ) VALUES (
            :user_id, :name, :description, :created,
            :pu_id, NULL, NULL, FALSE, 'Equal Area'
        ) RETURNING id
    """), {
        'user_id': 2,
        'name': region,
        'description': f"Project for ICES region {region}",
        'created': datetime.utcnow(),
        'pu_id': planning_unit_id
    }).fetchone()

    project_id = project_result[0]

    # 4. Link to user_projects
    engine.execute(text("""
        INSERT INTO bioprotect.user_projects (user_id, project_id)
        VALUES (:user_id, :project_id)
    """), {'user_id': 2, 'project_id': project_id})

    print(
        f"  ✔ Project '{region}' created with planning unit table '{planning_unit_table}'")
