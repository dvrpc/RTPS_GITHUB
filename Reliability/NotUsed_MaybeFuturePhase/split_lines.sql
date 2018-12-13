--Create table with endpoints
CREATE TABLE pts_test AS(
	WITH tblA AS(
					SELECT r.*, d.direction AS direc
					FROM route_dir_shape r
					INNER JOIN direction_crosswalk d
					ON r.route = d.route_id AND r.direction = d.direction_id::text
					),
	tblB AS (				
		SELECT l.*, r.route, r.direc
		FROM loaded_shapes l
		INNER JOIN tblA r
		ON l.shape_id = r.shape
		),
	tbl48 AS(
		SELECT *
		FROM tblB
		WHERE route = '48' OR route = '38' OR route = 'MFO'
		),
	startpts AS(
		SELECT 
			st_startpoint(geom) AS geo
		FROM tbl48
		),
	endpts AS(
		SELECT 
					st_endpoint(geom) AS geo
		FROM tbl48)
	SELECT geo
	FROM startpts
	UNION 
	SELECT geo
	FROM endpts
	);

--create lines
CREATE TABLE lines_test AS(
	WITH tblA AS(
					SELECT r.*, d.direction AS direc
					FROM route_dir_shape r
					INNER JOIN direction_crosswalk d
					ON r.route = d.route_id AND r.direction = d.direction_id::text
					),
	tblB AS (				
		SELECT l.*, r.route, r.direc
		FROM loaded_shapes l
		INNER JOIN tblA r
		ON l.shape_id = r.shape
		)
		SELECT *
		FROM tblB
		WHERE route = '48' OR route = '38' OR route = 'MFO'
		);

--add id column to pts and lines
ALTER TABLE pts_test ADD COLUMN id SERIAL PRIMARY KEY;
COMMIT;
ALTER TABLE lines_test ADD COLUMN id SERIAL PRIMARY KEY;
COMMIT;

--create function
CREATE OR REPLACE FUNCTION ST_AsMultiPoint(geometry) RETURNS geometry AS
'SELECT ST_Union((d).geom) FROM ST_DumpPoints($1) AS d;'
LANGUAGE sql IMMUTABLE STRICT COST 10;

--split lines
WITH temp_table1 AS (
	SELECT a.id,ST_ClosestPoint(ST_Union(b.geom), a.geo)::geometry(POINT,4326) AS geom 
	FROM pts_test a, lines_test b GROUP BY a.geo,a.id),
temp_table2 AS (
	SELECT 1 AS id, ST_Union(ST_AsMultiPoint(st_segmentize(geom,1)))::geometry(MULTIPOINT,4326) AS geom 
	FROM lines_test),
temp_table3 AS (
	SELECT b.id, ST_snap(ST_Union(b.geom),a.geom, ST_Distance(a.geom,b.geom)*1.01)::geometry(POINT,4326) AS geom 
	FROM temp_table2 a, temp_table1 b
	GROUP BY a.geom, b.geom, b.id)
SELECT a.id, (ST_Dump(ST_split(st_segmentize(a.geom,1),ST_Union(b.geom)))).geom::geometry(LINESTRING,4326) AS geom 
FROM lines_test a, temp_table3 b
GROUP BY a.id;


source: https://mygisnotes.wordpress.com/2017/01/01/split-lines-with-points-the-postgis-way/


WITH pts AS(
	SELECT
		st_startpoint(geometry) AS geo
	FROM dissolve_38
	UNION
	SELECT
		st_endpoint(geometry) AS geo
	FROM dissolve_38
	UNION
	SELECT
		st_startpoint(geometry) AS geo
	FROM dissolve_48
	UNION
	SELECT
		st_endpoint(geometry) AS geo
	FROM dissolve_48
	UNION
	SELECT
		st_startpoint(geometry) AS geo
	FROM dissolve_mfo
	UNION
	SELECT
		st_endpoint(geometry) AS geo
	FROM dissolve_mfo
	),
lines AS(
	SELECT geometry
	FROM dissolve_38
	UNION
	SELECT geometry
	FROM dissolve_48
	UNION
	SELECT geometry
	FROM dissolve_mfo
	)
SELECT
	(ST_Dump(ST_Split(lines.geometry, pts.geo))).geom::geometry(LINESTRING,4326) AS geom
FROM lines, pts


---maybe try importing to postgis and using these functions
--it could be mad that it is just quereying on QGIS layers


-----------------------------------------------------------
---trying to identify overlapping segments on 3 routes along market
WITH tblA AS(
	SELECT *
	FROM loaded_shapes_wroutes
	WHERE route = '38'
	OR route = '48'
	OR route = 'MFO'
	),
tblZ AS(
	SELECT a.id AS aid, b.id AS bid, a.route, a.geom
	FROM tblA a, tblA b
	WHERE a.id <> b.id
	AND ST_Intersects(a.geom, b.geom)
	AND Upper(ST_GeometryType(ST_Intersection(a.geom, b.geom))) LIKE '%LINE%'
	),
tblX AS(
	SELECT aid, COUNT(*) AS cnt, route, geom
	FROM tblZ
	GROUP BY aid, route, geom
	),
tblW AS(
	SELECT *
	FROM tblX
	WHERE cnt > 1
)
SELECT d.aid, d.route, f.aid, f.route, d.geom
FROM tblW d, tblW f
WHERE d.aid <> f.aid 
AND ST_Equals(d.geom, f.geom)

--what abount inverse pairs (doubled)
--what about the opposite direction?
--it repeats for segments that partially overlap...oh boy




geom
LineString (-75.175254999918252 39.96055000028235327, -75.17519199971098942 39.96085700005056651, -75.17516400021838763 39.96096699972730448, -75.1751359998264661 39.96109300014188648, -75.17510800033392115 39.96120299981862445, -75.17509499973442644 39.96126499997978954, -75.17507899989590214 39.96132400000266216, -75.17503299957326135 39.96155399981728351, -75.17488199980442687 39.96229399976834884, -75.17485500035792256 39.96240699958343612, -75.17483799957398105 39.96249099985982411)
