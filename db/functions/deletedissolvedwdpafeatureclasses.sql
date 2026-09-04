CREATE OR REPLACE FUNCTION bioprotect.deletedissolvedwdpafeatureclasses()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    row     record;
BEGIN
    FOR row IN 
        SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'bioprotect' AND table_name ILIKE ('wdpa_%') ORDER BY 1
    LOOP
        EXECUTE 'DROP TABLE ' || quote_ident(row.table_schema) || '.' || quote_ident(row.table_name);
        RAISE INFO 'Dropped table: %', quote_ident(row.table_schema) || '.' || quote_ident(row.table_name);
    END LOOP;
END;
$function$

