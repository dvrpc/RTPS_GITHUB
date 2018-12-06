--combine bus and trolley ridership/load data into single table to join to later
CREATE TABLE surfacetransit_loads AS(
	SELECT
		stop_id,
		stop_name,
		route,
		direction,
		sign_up,
		mode_,
		weekday_lo,
		source,
		geom
	FROM busstops_wloads
	UNION ALL
	SELECT
		stop_id,
		stop_name,
		route::text,
		direction,
		sign_up,
		mode_,
		weekday_lo,
		source,
		geom
	FROM trolleystops_wloads
	);

---draw stops from GTFS fror line of interest 
--returns all stops on all patterns
--need to match up with correct shape_id
--download closest GTFS to loads data
SELECT
	stop_id,
	st_setsrid(st_makepoint(stop_lat, stop_lon), 26918) AS stoppt
FROM gtfs_stops
WHERE stop_id IN(
	SELECT
		stop_id
	FROM gtfs_stop_times
	WHERE trip_id IN(
		SELECT 
			DISTINCT(trip_id)
		FROM gtfs_trips
		WHERE shape_id = '214711'
		)
	)
    

    
--which routes to run it on
SELECT
	DISTINCT(route)
FROM surfacetransit_loads
WHERE route IN (
	SELECT route_id
	FROM gtfs_routes
	)
    


--create as intermediate table showing the length of each pattern
CREATE TABLE shape_lengths AS(
	WITH tblA AS(
		SELECT
			shape_id,
			ST_MakeLine(
				ST_SetSRID(
					ST_MakePoint(shape_pt_lon, shape_pt_lat),
					4326
				)
			) geom
		FROM (
			SELECT * FROM gtfs_shapes ORDER BY shape_id, shape_pt_sequence
		) _q GROUP BY shape_id
	)	
	SELECT 
		shape_id,
		geom,
		ST_Length(geom)
	FROM tblA
	);
    
--for route of interest, which shape_id to use?
--choose longest shape
--repeat for each direction
WITH tblA AS(
	SELECT 
		g.route_id,
		g.direction_id,
		s.*
	FROM shape_lengths s
	INNER JOIN gtfs_trips g
	ON s.shape_id = g.shape_id
	WHERE route_id = '44' --iterate
	AND direction_id = '0' --iterate
)
SELECT 
	route_id,
	direction_id,
	shape_id,
	geom
FROM tblA
WHERE st_length = 
	(SELECT MAX(st_length)
	FROM tblA)
LIMIT 1

--maybe create table showing which shapeid is used for each route/direction (in python)


---create intermediate table with the limits of sequence id's between stops - where load values will need to be assigned
--need to add to this result table for each subsequent iteration
--pull results into python then insert table at end as sequence_loads

WITH _trips AS (
    SELECT * FROM gtfs_trips WHERE shape_id = '214878'
    ),
_stop_times AS (
    SELECT * FROM gtfs_stop_times WHERE trip_id IN (SELECT trip_id FROM _trips)
    ),
_stops AS (
    SELECT * FROM gtfs_stops WHERE stop_id IN (SELECT stop_id FROM _stop_times)
    ),
_stops_xt AS (
    SELECT _stops.stop_id, _stop_times_agg.stop_sequence, ST_SetSRID(ST_MakePoint(stop_lon, stop_lat), 4326) geom 
    FROM _stops 
    INNER JOIN (
    SELECT stop_id, MIN(stop_sequence) stop_sequence 
    FROM _stop_times 
    GROUP BY stop_id) _stop_times_agg 
    ON _stops.stop_id = _stop_times_agg.stop_id 
    ORDER BY stop_sequence
    ),
_shapes AS (
    SELECT *, ST_SetSRID(ST_MakePoint(shape_pt_lon, shape_pt_lat), 4326) geom FROM gtfs_shapes WHERE shape_id IN (SELECT shape_id FROM _trips) ORDER BY shape_pt_sequence
    ),
_shapes_xt1 AS (
    SELECT *, LEAD(geom) OVER (ORDER BY shape_pt_sequence) geom_next FROM _shapes
    ),
_shapes_xt2 AS (
    SELECT shape_id, shape_pt_sequence, ST_MakeLine(geom, geom_next) geom FROM _shapes_xt1 ORDER BY shape_pt_sequence
    ),
_shapes_xt11 AS (
    SELECT _stops_xt.stop_id, _shapes_xt2.shape_id, _shapes_xt2.shape_pt_sequence, ST_ShortestLine(_stops_xt.geom, _shapes_xt2.geom) geom FROM _stops_xt, _shapes_xt2
    ),
_shapes_xt12 AS (
    SELECT *, ST_Length(geom) length FROM _shapes_xt11
    ),
_shapes_xt13 AS (
    SELECT *, RANK() OVER (PARTITION BY stop_id ORDER BY length ASC) FROM _shapes_xt12
    ),
sequence_limits AS(
    SELECT * FROM _shapes_xt13 WHERE rank = 1 ORDER BY shape_pt_sequence
    )
SELECT 
    s.*, 
    sum(b.weekday_lo) as tot_loads
FROM sequence_limits s
INNER JOIN busstops_wloads b
ON s.stop_id = b.stop_id::text
GROUP BY s.stop_id, s.shape_id, s.shape_pt_sequence, s.geom, s.length, s.rank
ORDER BY shape_pt_sequence
    
    
--draw lines and join to loads between appropriate sequence ids
WITH tblA AS(
	SELECT
		shape_id,
		st_setsrid(st_makepoint(shape_pt_lon, shape_pt_lat), 4326) AS startpt, 
		st_setsrid(st_makepoint((LEAD(shape_pt_lon) OVER(ORDER BY shape_pt_sequence ASC)), (LEAD(shape_pt_lat) OVER(ORDER BY shape_pt_sequence ASC))), 4326) AS endpt,
		shape_pt_sequence
	FROM gtfs_shapes
	WHERE shape_id = '214878'
	--route 44, single pattern, single direction
	),
tblB AS(
	SELECT 
		shape_id,
		shape_pt_sequence,
		st_makeline(startpt, endpt) as geom
	FROM tblA 
),
tblC AS(
	SELECT
		stop_id,
		shape_pt_sequence,
		LEAD(shape_pt_sequence) OVER() AS seq_next,
		tot_loads
	FROM sequence_loads
	)

SELECT 
	b.shape_id,
	c.stop_id,
	b.shape_pt_sequence,
	c.tot_loads,
	b.geom
FROM tblB b
INNER JOIN tblC c
ON b.shape_pt_sequence BETWEEN c.shape_pt_sequence AND c.seq_next


---maybe dissolve on the load values to simplify and make the table/shp smaller?
