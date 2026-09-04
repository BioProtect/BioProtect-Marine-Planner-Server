CREATE OR REPLACE FUNCTION bioprotect.square_grid(areakm2 double precision, xmin double precision, ymin double precision, xmax double precision, ymax double precision)
 RETURNS SETOF geometry
 LANGUAGE plpgsql
AS $function$
DECLARE
	minpnt GEOMETRY;
	maxpnt GEOMETRY;
	minx FLOAT;
	miny FLOAT;
	maxx FLOAT;
	maxy FLOAT;
	xdistance FLOAT;
	sideLength FLOAT;
	xspacing FLOAT;
	yspacing FLOAT;
	xvertexlo FLOAT;
	xvertexhi FLOAT;
	rows INTEGER;
	columns INTEGER;
BEGIN
	-- Convert input coords to points in the 3410 projection
	minpnt = ST_Transform(ST_SetSRID(ST_MakePoint(xmin, ymin), 4326), 3410);
	maxpnt = ST_Transform(ST_SetSRID(ST_MakePoint(xmax, ymax), 4326), 3410);
	-- Get grid extents in 3410 projection
	minx = ST_X(minpnt);
	RAISE DEBUG 'minx: (%)', minx;
	miny = ST_Y(minpnt);
	RAISE DEBUG 'miny: (%)', miny;
	maxx = ST_X(maxpnt);
	RAISE DEBUG 'maxx: (%)', maxx;
	maxy = ST_Y(maxpnt);
	RAISE DEBUG 'maxy: (%)', maxy;
	
	-- Get the length of the square side
	sideLength = sqrt(areakm2 * 1000000.0);
	RAISE DEBUG 'sideLength: (%)', sideLength;

	--get the number of rows/columns
	rows = FLOOR((maxy - miny) / sideLength)::INTEGER;
	RAISE DEBUG 'crosses dateline: (%)', maxx<minx;
	IF (maxx<minx) THEN
		--get the x distance by adding the distance from minx to the dateline to the distance from the dateline to maxx
		xdistance := ST_Distance(minpnt, ST_Transform(ST_SetSRID(ST_Point(180,ymin),4326),3410)) + ST_Distance(ST_Transform(ST_SetSRID(ST_Point(-180,ymax),4326),3410),maxpnt);
		RAISE DEBUG 'xdistance: (%)', xdistance;
		columns = FLOOR(xdistance / sideLength)::INTEGER;
	ELSE
		columns = FLOOR((maxx - minx) / sideLength)::INTEGER;
	END IF;
	
	RAISE DEBUG 'rows: (%)', rows;
	RAISE DEBUG 'columns: (%)', columns;

	--create the squares/hexagons and return them
	RETURN QUERY 
		SELECT ST_SetSRID(ST_GeomFromText(format('POLYGON((%s %s, %s %s, %s %s, %s %s, %s %s))',x1,y1,x1,y2,x2,y2,x2,y1,x1,y1)), 3410) 
			FROM (SELECT minx + (c * sideLength) x1, minx + ((c + 1) * sideLength) x2, miny + (r * sideLength) y1, miny + ((r + 1) * sideLength) y2 FROM (SELECT c, r FROM generate_series(0, columns) AS c, generate_series(0, rows) AS r) AS sub) as points;
END
$function$

