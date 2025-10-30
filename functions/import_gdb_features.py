import fiona
import geopandas as gpd
import os

GDB_PATH = "./data/EUSeaMap_2023_2019/EUSeaMap_2023.gdb"

print(f"📘 Inspecting Geodatabase: {GDB_PATH}")

# List all layers
layers = fiona.listlayers(GDB_PATH)
print(f"\n📂 Found {len(layers)} layers:")
for layer in layers:
    print(f"  • {layer}")

# Preview each layer’s structure (lightweight — only metadata)
for layer in layers:
    print(f"\n🔹 Layer: {layer}")
    with fiona.open(GDB_PATH, layer=layer) as src:
        print(f"   ├─ Geometry type: {src.schema['geometry']}")
        print(f"   ├─ Feature count (approx.): {len(src)}")
        print(f"   ├─ CRS: {src.crs}")
        print(f"   └─ Fields:")
        for k, v in src.schema["properties"].items():
            print(f"       - {k}: {v}")


LAYER_NAME = "EUSeaMap_2023"

# Read only a small subset for speed
# adjust sample size if needed
gdf = gpd.read_file(GDB_PATH, layer=LAYER_NAME, rows=2000)

cols_of_interest = ["EUNIScomb", "EUNIScombD", "EUNIS2019C", "EUNIS2019D"]

print("✅ Columns found:")
for c in cols_of_interest:
    if c in gdf.columns:
        print(f"  • {c}")
    else:
        print(f"  ⚠️ Missing: {c}")

# Show a few sample records
print("\n📊 Sample rows:")
print(gdf[cols_of_interest].head(10))

# Show the unique values (trimmed for readability)
print("\n🔎 Unique sample values per column:")
for col in cols_of_interest:
    if col in gdf.columns:
        vals = gdf[col].dropna().unique()
        print(f"\n{col} — {len(vals)} unique values:")
        print(list(vals[:20]))  # show first 20 unique codes

gdf = gpd.read_file(GDB_PATH, layer="EUSeaMap_2023", rows=50000)
print(gdf["EUNIS2019C"].dropna().unique())
print("Count:", gdf["EUNIS2019C"].nunique())
print(gdf["EUNIS2019D"].dropna().unique())
print("Count:", gdf["EUNIS2019D"].nunique())
print(gdf["EUNIScomb"].dropna().unique())
print("Count:", gdf["EUNIScomb"].nunique())
print(gdf["EUNIScombD"].dropna().unique())
print("Count:", gdf["EUNIScombD"].nunique())
