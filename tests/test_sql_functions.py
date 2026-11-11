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


def build_pieces(cur, feature_id, table, batch_points=10_000):
    cur.execute("SELECT from_id, to_id FROM bioprotect.feature_piece_batches(%s, %s, %s)",
                [feature_id, table, batch_points])
    batches = cur.fetchall()
    for lo, hi in batches:
        cur.execute("SELECT bioprotect.build_feature_pieces(%s, %s, %s, %s)",
                    [feature_id, table, lo, hi])
    return batches


def polygon_feature(cur):
    """half of A plus 0.9 of B, and a self-intersecting bowtie over C:
    ST_MakeValid gives two triangles of 1000 x 500 / 2 each, so 0.5 km²."""
    feature_id, table = feature(cur)
    cur.execute(f"CREATE TABLE bioprotect.{table} (id serial PRIMARY KEY, geometry geometry)")
    cur.execute(f"INSERT INTO bioprotect.{table} (geometry) VALUES ({box(500, 0, 1900, 1000, 3035)})")
    cur.execute(f"INSERT INTO bioprotect.{table} (geometry) VALUES (ST_GeomFromText("
                "'POLYGON((2000 0, 3000 1000, 3000 0, 2000 1000, 2000 0))', 3035))")
    return feature_id, table


def test_polygon_amounts_are_overlap_km2_and_invalid_rings_are_repaired(db, square_grid):
    project_id, grid_id = square_grid
    feature_id, table = polygon_feature(db)
    build_pieces(db, feature_id, table)

    db.execute("SELECT bioprotect.insert_feature_pu_amounts(%s, %s, %s, %s, 'ST_Polygon', NULL, NULL)",
               [project_id, feature_id, grid_id, table])

    got = amounts(db, project_id, feature_id)
    assert got == pytest.approx({"A": 0.5, "B": 0.9, "C": 0.5})


def test_batched_amounts_match_and_pieces_are_reused_until_rows_are_added(db, square_grid):
    project_id, grid_id = square_grid
    feature_id, table = polygon_feature(db)
    # 1 vertex per batch: every row in its own batch
    assert len(build_pieces(db, feature_id, table, batch_points=1)) == 2
    assert build_pieces(db, feature_id, table) == []  # complete: reused

    # 1 hit per batch: every cell in its own h3 range
    db.execute("SELECT lo, hi FROM bioprotect.feature_cell_batches(%s, %s, 1)", [feature_id, grid_id])
    ranges = db.fetchall()
    assert ranges == [("A", "A"), ("B", "B"), ("C", "C")]
    for lo, hi in ranges:
        db.execute("SELECT bioprotect.insert_feature_pu_amounts(%s, %s, %s, %s, 'ST_Polygon', %s, %s)",
                   [project_id, feature_id, grid_id, table, lo, hi])
    assert amounts(db, project_id, feature_id) == pytest.approx({"A": 0.5, "B": 0.9, "C": 0.5})

    # a new row (Engage merge) makes the pieces stale: all rebuilt, no duplicates
    db.execute(f"INSERT INTO bioprotect.{table} (geometry) VALUES ({box(0, 0, 100, 1000, 3035)})")
    assert build_pieces(db, feature_id, table) != []
    assert one(db, "SELECT count(DISTINCT source_id) = 3 AND count(*) = 4 "  # bowtie = 2 triangles
                   "FROM bioprotect.feature_pieces WHERE feature_unique_id = %s", [feature_id])


def test_point_amounts_sum_the_value_column(db, square_grid):
    project_id, grid_id = square_grid
    feature_id, table = feature(db)
    db.execute(f"CREATE TABLE bioprotect.{table} (value double precision, geometry geometry)")
    for value, x in ((2, 100), (3, 900), (5, 1500)):
        db.execute(f"INSERT INTO bioprotect.{table} VALUES (%s, ST_SetSRID(ST_MakePoint({x}, 500), 3035))",
                   [value])

    db.execute("SELECT bioprotect.insert_feature_pu_amounts(%s, %s, %s, %s, 'ST_Point', NULL, NULL)",
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
    centroid; activity 2 (rpp 1, sensitivity 1) covers A's and B's, as two
    polygons whose shared edge runs through A's centroid - it still counts.
    Pressures come from PAD by activity title; an rppscore 0 row is ignored.
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
    act_ids = []
    for act, rpp, sens, geoms in ((1, 2, 3, [box(0.1, 0.1, 0.9, 0.9)]),
                                  (2, 1, 1, [box(0.1, 0.1, 0.5, 0.9), box(0.5, 0.1, 1.9, 0.9)])):
        title, pressure, table = (f"pytest activity {act} {tag}", f"pytest pressure {act} {tag}",
                                  f"pytest_act_{act}_{tag}")
        db.execute(f"CREATE TABLE bioprotect.{table} AS "
                   + " UNION ALL ".join(f"SELECT {g} AS geometry" for g in geoms))
        act_ids.append(one(db, "INSERT INTO bioprotect.metadata_activities (activity, activity_name) "
                               "VALUES (%s, %s) RETURNING id", [title, table]))
        db.execute("INSERT INTO bioprotect.pad (activitytitle, pressuretitle, rppscore) "
                   "VALUES (%s, %s, %s), (%s, %s, 0)", [title, pressure, rpp, title, pressure + " zero"])
        db.execute("INSERT INTO bioprotect.sensitivity_matrix (eunis_code, pressure, sensitivity_score) "
                   "VALUES (%s, %s, %s), (%s, %s, 100)", [alias, pressure, sens, alias, pressure + " zero"])

    cp = one(db, "SELECT bioprotect.run_cumulative_impact(%s, %s, 'pytest ci')", [p, act_ids])

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


def test_run_config_defaults_and_null_feature_target_is_30_percent(db):
    """A NULL project_features.target_value means "not set" and becomes 0.30;
    an empty params object gets every documented default."""
    pid = project(db)
    (set_f, _), (unset_f, _) = feature(db), feature(db)
    db.execute("INSERT INTO bioprotect.project_features (project_id, feature_unique_id, target_value) "
               "VALUES (%s, %s, 45), (%s, %s, NULL)", [pid, set_f, pid, unset_f])
    run = one(db, "INSERT INTO bioprotect.prioritizr_runs (project_id) VALUES (%s) RETURNING id", [pid])
    db.execute("SELECT target_prop, mode, boundary_penalty, linear_cost_penalty, gap, time_limit_sec, "
               "feature_targets_json::jsonb FROM bioprotect.get_prioritizr_run_config(%s)", [run])
    *defaults, targets = db.fetchone()
    assert defaults == [0.30, "area", 0.0, 0.1, 0.04, 1200]
    assert targets == {f"f_{set_f}": 0.45, f"f_{unset_f}": 0.30}
