-- Shared, per-feature cut-up pieces used by insert_feature_pu_amounts.
-- See migrations/0019_feature_pieces.sql for why.

-- Returns the source-id ranges still to build, after deleting any partial or
-- stale pieces. No rows = pieces are complete and can be reused.
-- Batches hold ~p_batch_points vertices (repair + subdivide cost tracks
-- vertices, not rows). A row starts in the batch where its vertices start,
-- so a giant polygon shares its batch only with the small rows before it.
-- ponytail: one polygon is the floor: CSO's 496k-vertex Sand Plains row is
-- ~12 s on dev on its own. Splitting a single row would need ST_Dump batching.
-- ponytail: a feature whose highest-id row subdivides to nothing (empty after
-- repair) never looks complete, so it is rebuilt each time; slower, not wrong.
CREATE OR REPLACE FUNCTION bioprotect.feature_piece_batches(
    p_feature_id integer, p_feature_class text, p_batch_points integer)
 RETURNS TABLE(from_id integer, to_id integer)
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_max integer;
BEGIN
    EXECUTE format('SELECT max(id) FROM bioprotect.%I', p_feature_class) INTO v_max;
    IF v_max IS NULL THEN
        RETURN;
    END IF;
    IF (SELECT max(source_id) FROM bioprotect.feature_pieces
         WHERE feature_unique_id = p_feature_id) = v_max THEN
        RETURN;
    END IF;
    DELETE FROM bioprotect.feature_pieces WHERE feature_unique_id = p_feature_id;
    RETURN QUERY EXECUTE format($f$
        WITH v AS (
            SELECT id, floor((sum(ST_NPoints(geometry)) OVER (ORDER BY id)
                              - ST_NPoints(geometry)) / $1) AS batch
              FROM bioprotect.%I
        )
        SELECT min(id), max(id) FROM v GROUP BY batch ORDER BY batch
    $f$, p_feature_class) USING p_batch_points;
END;
$function$;

-- Repairs then subdivides the feature rows with id in [from, to].
CREATE OR REPLACE FUNCTION bioprotect.build_feature_pieces(
    p_feature_id integer, p_feature_class text, p_from_id integer, p_to_id integer)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_repaired integer := 0;
BEGIN
    -- GEOS aborts the whole overlay with "TopologyException: ... Ring
    -- Self-intersection" on one bad ring, and shapefiles routinely carry them.
    -- ST_MakeValid can return a GeometryCollection (polygon plus dangling
    -- lines), so ST_CollectionExtract keeps just the polygonal parts.
    EXECUTE format($f$
        UPDATE bioprotect.%I
           SET geometry = ST_CollectionExtract(ST_MakeValid(geometry), 3)
         WHERE id BETWEEN $1 AND $2 AND NOT ST_IsValid(geometry)
    $f$, p_feature_class) USING p_from_id, p_to_id;
    GET DIAGNOSTICS v_repaired = ROW_COUNT;
    IF v_repaired > 0 THEN
        RAISE NOTICE 'Repaired % invalid geometr(y|ies) in %', v_repaired, p_feature_class;
    END IF;

    -- <=256-vertex pieces: without this every cell intersects the whole
    -- (often huge) polygon; a 33k-vertex feature over 18k cells took 50 s vs
    -- 1.5 s subdivided, same amounts.
    EXECUTE format($f$
        INSERT INTO bioprotect.feature_pieces (feature_unique_id, source_id, geometry)
        SELECT $3, id, ST_Subdivide(geometry, 256)
          FROM bioprotect.%I
         WHERE id BETWEEN $1 AND $2
    $f$, p_feature_class) USING p_from_id, p_to_id, p_feature_id;
END;
$function$;

-- Splits the grid cells a feature's pieces touch into h3_index ranges of
-- roughly p_batch_hits piece/cell bbox hits each (the per-cell union's cost).
-- && overcounts against ST_Intersects but never misses a cell.
CREATE OR REPLACE FUNCTION bioprotect.feature_cell_batches(
    p_feature_id integer, p_planning_unit_id integer, p_batch_hits integer)
 RETURNS TABLE(lo text, hi text)
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_grid_table text;
BEGIN
    SELECT feature_class_name INTO v_grid_table
      FROM bioprotect.metadata_planning_units WHERE unique_id = p_planning_unit_id;
    IF v_grid_table IS NULL THEN
        RAISE EXCEPTION 'No planning unit table found for ID %', p_planning_unit_id;
    END IF;

    RETURN QUERY EXECUTE format($f$
        WITH hits AS (
            SELECT grid.h3_index::text AS h3, count(*) AS n
              FROM bioprotect.feature_pieces p
              JOIN bioprotect.%I grid ON grid.geometry && p.geometry
             WHERE p.feature_unique_id = $1
             GROUP BY 1
        ), numbered AS (
            SELECT h3, floor((sum(n) OVER (ORDER BY h3) - 1) / $2) AS batch FROM hits
        )
        SELECT min(h3), max(h3) FROM numbered GROUP BY batch ORDER BY batch
    $f$, v_grid_table) USING p_feature_id, p_batch_hits;
END;
$function$;
