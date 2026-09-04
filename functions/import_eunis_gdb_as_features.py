import asyncio
import os
import uuid
import csv
from datetime import datetime
import geopandas as gpd
import fiona
from classes.postgis_class import get_pg
from services.service_error import ServicesError


async def import_eunis_gdb(gdb_path, schema="bioprotect", created_by="bioprotect_admin"):
    """
    Imports EUSeaMap_2023.gdb into the BioProtect database as distinct EUNIS features,
    and logs progress to a CSV file.
    """

    # ----------------------------------------------------------------
    # INITIAL SETUP
    # ----------------------------------------------------------------
    pg = await get_pg()
    log_file = "eunis_import_log.csv"
    log_fields = ["timestamp", "eunis_code",
                  "feature_class_name", "area_m2", "status", "message"]

    with open(log_file, "w", newline="", encoding="utf-8") as lf:
        writer = csv.DictWriter(lf, fieldnames=log_fields)
        writer.writeheader()

    print(f"📘 Log file created: {os.path.abspath(log_file)}")

    try:
        # ----------------------------------------------------------------
        # 1️⃣ Read the GDB and get EUNIS codes
        # ----------------------------------------------------------------
        layers = fiona.listlayers(gdb_path)
        if not layers:
            raise ServicesError("No layers found in the GDB.")

        layer = layers[0]
        print(f"🔹 Using layer: {layer}")

        gdf = gpd.read_file(gdb_path, layer=layer, rows=500000)
        eunis_codes = sorted(gdf["EUNIS2019D"].dropna().unique().tolist())
        print(f"✅ Found {len(eunis_codes)} EUNIS codes to process.\n")

        # ----------------------------------------------------------------
        # 2️⃣ Loop through each code
        # ----------------------------------------------------------------
        for idx, code in enumerate(eunis_codes, start=1):
            timestamp = datetime.now().isoformat(timespec="seconds")
            feature_class_name = "f_" + uuid.uuid4().hex[:30]
            print(
                f"[{idx}/{len(eunis_codes)}] 🧩 Importing EUNIS {code} → {feature_class_name}")

            try:
                # 2.1 Import this subset using ogr2ogr (PostGIS method)
                await pg.import_file_GDBFeatureClass(
                    folder=os.path.dirname(gdb_path),
                    fileGDB=os.path.basename(gdb_path),
                    sourceFeatureClass=layer,
                    destFeatureClass=feature_class_name,
                    s_epsg_code="EPSG:4326",
                    t_epsg_code="EPSG:4326",
                    splitAtDateline=False,
                    where_clause=f"EUNIS2019D = '{code}'",
                )

                # 2.2 Validate geometries
                await pg.is_valid(feature_class_name)

                # 2.3 Compute area + extent + metadata fields
                q = f"""
                    SELECT
                        ST_Area(ST_Transform(ST_Union(geometry), 3410)) AS area,
                        box2d(ST_Union(geometry)) AS extent,
                        MAX(eunis2019d) AS eunis_2019d,
                        MAX(euniscomb)  AS eunis_comb,
                        MAX(euniscombd) AS eunis_combd
                    FROM {schema}.{feature_class_name};
                """
                df = await pg.execute(q, return_format="DataFrame")
                if df.empty:
                    raise ServicesError(
                        f"No geometry found for EUNIS code {code}")

                area = float(df.iloc[0]["area"] or 0)
                extent = df.iloc[0]["extent"]
                desc = df.iloc[0]["eunis_2019d"] or "Imported EUNIS feature"
                comb = df.iloc[0]["eunis_comb"] or ""
                combd = df.iloc[0]["eunis_combd"] or ""

                # 2.4 Register feature metadata
                ins_q = f"""
                    INSERT INTO {schema}.metadata_interest_features
                        (feature_class_name, alias, description, creation_date, _area, extent, source, created_by)
                    VALUES
                        ('{feature_class_name}', '{code}', '{desc.replace("'", "''")}',
                         NOW(), {area}, '{extent}', 'EUSeaMap 2023', '{created_by}');
                """
                await pg.execute(ins_q)

                print(f"✅ Imported {code} ({area:,.0f} m²)")
                with open(log_file, "a", newline="", encoding="utf-8") as lf:
                    writer = csv.DictWriter(lf, fieldnames=log_fields)
                    writer.writerow({
                        "timestamp": timestamp,
                        "eunis_code": code,
                        "feature_class_name": feature_class_name,
                        "area_m2": round(area, 2),
                        "status": "success",
                        "message": "imported successfully",
                    })

            except ServicesError as e:
                print(f"❌ Error importing {code}: {e}")
                with open(log_file, "a", newline="", encoding="utf-8") as lf:
                    writer = csv.DictWriter(lf, fieldnames=log_fields)
                    writer.writerow({
                        "timestamp": timestamp,
                        "eunis_code": code,
                        "feature_class_name": feature_class_name,
                        "area_m2": 0,
                        "status": "failed",
                        "message": str(e),
                    })
            except Exception as e:
                print(f"💥 Unexpected error for {code}: {e}")
                with open(log_file, "a", newline="", encoding="utf-8") as lf:
                    writer = csv.DictWriter(lf, fieldnames=log_fields)
                    writer.writerow({
                        "timestamp": timestamp,
                        "eunis_code": code,
                        "feature_class_name": feature_class_name,
                        "area_m2": 0,
                        "status": "error",
                        "message": str(e),
                    })

        # ----------------------------------------------------------------
        print(
            f"\n🎉 Import complete. Log saved to: {os.path.abspath(log_file)}")

    finally:
        await pg.close_pool()


if __name__ == "__main__":
    GDB_PATH = "./data/EUSeaMap_2023_2019/EUSeaMap_2023.gdb"
    asyncio.run(import_eunis_gdb(GDB_PATH))
