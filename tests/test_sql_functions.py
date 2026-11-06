"""SQL function tests on tiny hand-built cases with hand-computed values.

Everything runs on the `db` cursor and is rolled back, so the functions'
own DDL (prepare_prioritizr_input's CREATE TABLE) disappears too.
Geometry is plain boxes, not real hexagons: these functions only care about
h3_index keys and containment, and boxes make the expected numbers exact.
"""

import uuid
from decimal import Decimal

import psycopg2
import pytest


def one(cur, sql, args=()):
    cur.execute(sql, args)
    return cur.fetchone()[0]


def box(x0, y0, x1, y1, srid=4326):
    return f"ST_MakeEnvelope({x0}, {y0}, {x1}, {y1}, {srid})"


def project(cur, crs_epsg=4326, planning_unit_id=None):
    return one(cur, "INSERT INTO bioprotect.projects (name, crs_epsg, planning_unit_id) "
                    "VALUES (%s, %s, %s) RETURNING id",
               [f"pytest {uuid.uuid4().hex[:8]}", crs_epsg, planning_unit_id])


def grid_meta(cur, table=None):
    return one(cur, "INSERT INTO bioprotect.metadata_planning_units "
                    "(feature_class_name, alias) VALUES (%s, %s) RETURNING unique_id",
               [table, f"pytest grid {uuid.uuid4().hex[:8]}"])


def feature(cur):
    name = f"pytest_feat_{uuid.uuid4().hex[:8]}"
    return one(cur, "INSERT INTO bioprotect.metadata_interest_features "
                    "(feature_class_name, alias) VALUES (%s, %s) RETURNING unique_id",
               [name, name]), name


def pus(cur, project_id, h3s):
    """project_pus rows; returns {h3_index: project_pu_id}."""
    out = {}
    for h in h3s:
        out[h] = one(cur, "INSERT INTO bioprotect.project_pus (project_id, h3_index) "
                          "VALUES (%s, %s) RETURNING id", [project_id, h])
    return out


def h3_cell(cur, h3, geom_sql, area="pytest area"):
    cur.execute("INSERT INTO bioprotect.h3_cells "
                "(h3_index, resolution, scale_level, project_area, geometry) "
                f"VALUES (%s, 7, 'pytest', %s, {geom_sql})", [h3, area])


def amount(cur, project_id, feature_id, h3, value):
    cur.execute("INSERT INTO bioprotect.pu_feature_amounts "
                "(project_id, feature_unique_id, h3_index, amount) VALUES (%s, %s, %s, %s)",
                [project_id, feature_id, h3, value])


def amounts(cur, project_id, feature_id):
    cur.execute("SELECT h3_index, amount FROM bioprotect.pu_feature_amounts "
                "WHERE project_id = %s AND feature_unique_id = %s", [project_id, feature_id])
    return dict(cur.fetchall())


# --- insert_feature_pu_amounts ---------------------------------------------

@pytest.fixture
def square_grid(db):
    """Three 1 km squares A|B|C in EPSG:3035 (metres), so areas are exact km²."""
    table = f"pytest_grid_{uuid.uuid4().hex[:8]}"
    db.execute(f"CREATE TABLE bioprotect.{table} (h3_index text, geometry geometry)")
    for h, x in (("A", 0), ("B", 1000), ("C", 2000)):
        db.execute(f"INSERT INTO bioprotect.{table} VALUES (%s, {box(x, 0, x + 1000, 1000, 3035)})", [h])
    grid_id = grid_meta(db, table)
    return project(db, crs_epsg=3035, planning_unit_id=grid_id), grid_id


def test_polygon_amounts_are_overlap_km2_and_invalid_rings_are_repaired(db, square_grid):
    project_id, grid_id = square_grid
    feature_id, table = feature(db)
    db.execute(f"CREATE TABLE bioprotect.{table} (geometry geometry)")
    # half of A plus 0.9 of B
    db.execute(f"INSERT INTO bioprotect.{table} VALUES ({box(500, 0, 1900, 1000, 3035)})")
    # a self-intersecting bowtie over C: ST_MakeValid gives two triangles of
    # 1000 x 500 / 2 each, so 0.5 km²
    db.execute(f"INSERT INTO bioprotect.{table} VALUES (ST_GeomFromText("
               "'POLYGON((2000 0, 3000 1000, 3000 0, 2000 1000, 2000 0))', 3035))")

    db.execute("SELECT bioprotect.insert_feature_pu_amounts(%s, %s, %s, %s, 'ST_Polygon')",
               [project_id, feature_id, grid_id, table])

    got = amounts(db, project_id, feature_id)
    assert got == pytest.approx({"A": 0.5, "B": 0.9, "C": 0.5})


def test_point_amounts_sum_the_value_column(db, square_grid):
    project_id, grid_id = square_grid
    feature_id, table = feature(db)
    db.execute(f"CREATE TABLE bioprotect.{table} (value double precision, geometry geometry)")
    for value, x in ((2, 100), (3, 900), (5, 1500)):
        db.execute(f"INSERT INTO bioprotect.{table} VALUES (%s, ST_SetSRID(ST_MakePoint({x}, 500), 3035))",
                   [value])

    db.execute("SELECT bioprotect.insert_feature_pu_amounts(%s, %s, %s, %s, 'ST_Point')",
               [project_id, feature_id, grid_id, table])

    assert amounts(db, project_id, feature_id) == {"A": 5, "B": 5}


# --- populate_grid_boundary_edges / get_project_boundary_edges --------------

def test_boundary_edges_for_a_seven_cell_disk(db):
    """A cell and its 6 neighbours: 6 spokes + 6 ring edges = 12 undirected
    edges. A second project on the same grid sharing cells must not add
    duplicates, and a far-away cell has no edges."""
    centre = one(db, "SELECT h3_lat_lng_to_cell(point(-9, 53), 7)::text")
    db.execute("SELECT h3_grid_disk(%s::h3index, 1)::text", [centre])
    disk = [r[0] for r in db.fetchall()]
    far = one(db, "SELECT h3_lat_lng_to_cell(point(-6, 55), 7)::text")
    grid_id = grid_meta(db)
    p1 = project(db, planning_unit_id=grid_id)
    p2 = project(db, planning_unit_id=grid_id)
    pus(db, p1, disk + [far])
    pus(db, p2, disk[:2])

    assert one(db, "SELECT bioprotect.populate_grid_boundary_edges(%s)", [grid_id]) == 12

    db.execute("SELECT pu_id, nbr_id, boundary FROM bioprotect.get_project_boundary_edges(%s)", [p1])
    edges = db.fetchall()
    assert len(edges) == len({(a, b) for a, b, _ in edges}) == 12
    assert all(a < b and w == 1.0 for a, b, w in edges)
    assert far not in {c for a, b, _ in edges for c in (a, b)}
    assert sum(centre in (a, b) for a, b, _ in edges) == 6


# --- prepare_prioritizr_input ----------------------------------------------

def _run(cur, project_id):
    return one(cur, "INSERT INTO bioprotect.prioritizr_runs (project_id) VALUES (%s) RETURNING id",
               [project_id])


def _cost_profile(cur, project_id, values):
    """values: {project_pu_id: (cost, status)}; makes it the active profile."""
    cp = one(cur, "INSERT INTO bioprotect.cost_profiles (project_id, name) "
                  "VALUES (%s, 'pytest') RETURNING id", [project_id])
    for pu_id, (cost, status) in values.items():
        cur.execute("INSERT INTO bioprotect.cost_profile_values "
                    "(cost_profile_id, project_pu_id, cost, status) VALUES (%s, %s, %s, %s)",
                    [cp, pu_id, cost, status])
    cur.execute("UPDATE bioprotect.projects SET active_cost_profile_id = %s WHERE id = %s",
                [cp, project_id])
    return cp


def test_prepare_input_one_row_per_pu_with_costs_locks_and_features(db):
    p = project(db)
    ids = pus(db, p, ["A", "B", "C"])
    _cost_profile(db, p, {ids["A"]: (2, 0), ids["B"]: (3, 1), ids["C"]: (4, 2)})
    for i, h in enumerate("ABC"):
        h3_cell(db, h, box(i, 0, i + 1, 1))
    # Regression (prod run 252): A also sits in two overlapping grids, once
    # with float-noise geometry. That used to give A two rows (duplicate key
    # on the results) or, with identical copies, multiply A's feature sums.
    h3_cell(db, "A", box(0, 0, 1, 1), area="pytest overlap")
    h3_cell(db, "A", box(0, 0, 1.0000000001, 1), area="pytest overlap 2")
    f1, _ = feature(db)
    f2, _ = feature(db)
    for fid in (f1, f2):
        db.execute("INSERT INTO bioprotect.project_features (project_id, feature_unique_id) "
                   "VALUES (%s, %s)", [p, fid])
    amount(db, p, f1, "A", 0.5)
    amount(db, p, f1, "B", 1.0)
    amount(db, p, f2, "C", 2.0)
    run = _run(db, p)

    db.execute("SELECT bioprotect.prepare_prioritizr_input(%s)", [run])

    db.execute("SELECT input_table, feature_cols, status FROM bioprotect.prioritizr_runs "
               "WHERE id = %s", [run])
    table, cols, status = db.fetchone()
    assert table == f"bioprotect.prioritizr_input_run_{run}"
    assert cols == [f"f_{f}" for f in sorted([f1, f2])]  # numeric order, not text
    assert status == "preparing"
    db.execute(f"SELECT pu_id, cost, locked_in, locked_out, f_{f1}, f_{f2} FROM {table} ORDER BY 1")
    assert db.fetchall() == [
        ("A", 2.0, 0, 0, 0.5, 0.0),
        ("B", 3.0, 1, 0, 1.0, 0.0),
        ("C", 4.0, 0, 1, 0.0, 2.0),
    ]


def test_prepare_input_refuses_a_project_without_cost_profile(db):
    p = project(db)
    with pytest.raises(psycopg2.errors.RaiseException, match="no active_cost_profile_id"):
        db.execute("SELECT bioprotect.prepare_prioritizr_input(%s)", [_run(db, p)])


# --- run_cumulative_impact --------------------------------------------------

def test_cumulative_impact_normalises_into_floor_to_one(db):
    """impact = coverage x rppscore x feature amount x sensitivity, summed.

    Hexes A|B|C (unit boxes). Activity 1 (rpp 2, sensitivity 3) covers A's
    centroid; activity 2 (rpp 1, sensitivity 1) covers A's and B's.
    Feature amounts A 0.5, B 1.0, C none.
      A = 2*0.5*3 + 1*0.5*1 = 3.5  -> max -> 1.0
      B = 1*1.0*1           = 1.0  -> 0.001 + 0.999 * 1/3.5
      C = no impact                -> floor 0.001
    """
    p = project(db)
    ids = pus(db, p, ["A", "B", "C"])
    for i, h in enumerate("ABC"):
        h3_cell(db, h, box(i, 0, i + 1, 1))
    # overlapping grid: a second copy of A must not double A's impact
    h3_cell(db, "A", box(0, 0, 1, 1), area="pytest overlap")
    fid, alias = feature(db)
    amount(db, p, fid, "A", 0.5)
    amount(db, p, fid, "B", 1.0)
    tag = uuid.uuid4().hex[:8]
    for act, rpp, sens, geom in ((1, 2, 3, box(0.1, 0.1, 0.9, 0.9)),
                                 (2, 1, 1, box(0.1, 0.1, 1.9, 0.9))):
        pressure = f"pytest pressure {act} {tag}"
        db.execute(f"INSERT INTO bioprotect.pressures (activity_id, pressuretitle, rppscore, geometry) "
                   f"VALUES (%s, %s, %s, {geom})", [-act, pressure, rpp])
        db.execute("INSERT INTO bioprotect.sensitivity_matrix (eunis_code, pressure, sensitivity_score) "
                   "VALUES (%s, %s, %s)", [alias, pressure, sens])

    cp = one(db, "SELECT bioprotect.run_cumulative_impact(%s, ARRAY[-1, -2], 'pytest ci')", [p])

    db.execute("SELECT project_pu_id, cost, status FROM bioprotect.cost_profile_values "
               "WHERE cost_profile_id = %s", [cp])
    got = {pu: (float(cost), status) for pu, cost, status in db.fetchall()}
    assert got == {
        ids["A"]: (1.0, 0),
        ids["B"]: (pytest.approx(0.001 + 0.999 / 3.5), 0),
        ids["C"]: (0.001, 0),
    }


@pytest.mark.parametrize("floor", [0, 1, -0.5])
def test_cumulative_impact_rejects_floor_outside_0_1(db, floor):
    p = project(db)
    pus(db, p, ["A"])
    with pytest.raises(psycopg2.errors.RaiseException, match="strictly between 0 and 1"):
        db.execute("SELECT bioprotect.run_cumulative_impact(%s, ARRAY[-1], 'x', '', 'pytest', %s)",
                   [p, Decimal(floor)])
