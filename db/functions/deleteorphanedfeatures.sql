CREATE OR REPLACE FUNCTION bioprotect.deleteorphanedfeatures()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    row     record;
BEGIN
    FOR row IN 
        SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'bioprotect' AND table_name ILIKE ('f_%') AND table_name NOT IN (SELECT feature_class_name FROM bioprotect.metadata_interest_features) ORDER BY 1
    LOOP
        EXECUTE 'DROP TABLE bioprotect.' || quote_ident(row.table_name);
        RAISE INFO 'Dropped table: %', quote_ident(row.table_schema) || '.' || quote_ident(row.table_name);
    END LOOP;
    FOR row IN 
        SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'bioprotect' AND table_name ILIKE ('pu_%') AND table_name NOT IN (SELECT feature_class_name FROM bioprotect.metadata_planning_units) ORDER BY 1
    LOOP
        EXECUTE 'DROP TABLE bioprotect.' || quote_ident(row.table_name);
        RAISE INFO 'Dropped table: %', quote_ident(row.table_schema) || '.' || quote_ident(row.table_name);
    END LOOP;
END;
$function$

