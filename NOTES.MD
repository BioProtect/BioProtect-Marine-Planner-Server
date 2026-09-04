| Table                                 | Purpose                                                                    |
| ------------------------------------- | -------------------------------------------------------------------------- |
| `h3_cells`                            | Stores **master list of H3 hexes** for each region/scale                   |
| `project_pus`                         | Stores **which H3 cells are used** in a specific project (cost + status)   |
| `pu_{region}` (e.g., `pu_baltic_sea`) | Geometry-only tilesets for each area (used for rendering)                  |
| `metadata_planning_units`             | Describes each planning unit set (region name, envelope, tileset ID, etc.) |
| `projects`                            | Links to planning unit grids (via `planning_unit_id`)                      |
| `gaul_2015_simplified_1km`            | Countries metadata used for enrichment                                     |

Each project → project_pus.project_id

Each project is linked to a planning grid via:
→ projects.planning_unit_id → metadata_planning_units.unique_id

project_pus.h3_index is a subset of h3_cells.h3_index

pu\_{region} tables are used for tile rendering via Martin

These link to metadata_planning_units.feature_class_name (used as Martin source-layer)

| Function                        | What it does                                           |
| ------------------------------- | ------------------------------------------------------ |
| `getplanningunits()`            | Lists all grids from `metadata_planning_units`         |
| `get_planning_units_metadata()` | Gets enriched metadata for a grid by ID                |
| `get_project_pus()`             | Gets the project’s planning units (from `project_pus`) |

---

ROLES
'owner','editor','viewer'
