import numpy
import os
import csv
import scipy
import pandas as pd
import psycopg2 as psql 
import sys
import itertools
import numpy
import time

#connect to SQL DB in python
con = psql.connect(dbname = "GTFS", host = "localhost", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()

#which routes to use/do we have data for
cur.execute("""
    SELECT
        DISTINCT(route)
    FROM surfacetransit_loads
    WHERE route IN (
        SELECT route_id
        FROM gtfs_routes
        )""")
routes = cur.fetchall()

directions = ['0', '1']

route_list = []
direction_list = []
selected_shapes = []
for i in xrange(len(routes)):
    for direct in directions:
        cur.execute("""
            WITH tblA AS(
                SELECT 
                    g.route_id,
                    g.direction_id,
                    s.*
                FROM shape_lengths s
                INNER JOIN gtfs_trips g
                ON s.shape_id = g.shape_id
                WHERE route_id::text = '{0}'
                AND direction_id = '{1}'
            )
            SELECT 
                shape_id
            FROM tblA
            WHERE st_length = 
                (SELECT MAX(st_length)
                FROM tblA)
            LIMIT 1
        """.format(routes[i][0], direct))
        route_list.append(routes[i][0])
        direction_list.append(direct)
        shape = cur.fetchall()
        if len(shape) == 0:
            selected_shapes.append('NA')
        else:
            selected_shapes.append(shape[0][0])
        
#table linking route/direction with shapes used
route_dir_shape = zip(route_list, direction_list, selected_shapes)

for i in xrange(0,len(route_dir_shape)):
    cur.execute("""
        INSERT INTO route_dir_shape (route, direction, shape)
        VALUES (%s, %s, %s)
        """,[route_dir_shape[i][0], route_dir_shape[i][1], route_dir_shape[i][2]])
    con.commit()

#create direction crosswalk table with first route/direction combo
cur.execute("""
    CREATE TABLE direction_crosswalk AS(
        with _idea AS (
            SELECT st.*, t.route_id,  t.direction_id, st_l.*
            FROM gtfs_stop_times st
            LEFT JOIN gtfs_trips t ON t.trip_id = st.trip_id
            LEFT JOIN surfacetransit_loads st_l ON st_l.stop_id::text = st.stop_id AND st_l.route = t.route_id
            WHERE st_l.route = '{0}'
            AND t.direction_id = '{1}'
        ),
        tblA AS(
            SELECT route_id, direction_id, direction, count(*) cnt 
            FROM _idea
            GROUP BY route_id, direction_id, direction
            )

        SELECT
            route_id,
            direction_id,
            direction
        FROM tblA
        WHERE cnt = (
            SELECT 
                MAX(cnt)
            FROM tblA)
    )""".format(route_dir_shape[0][0], route_dir_shape[0][1]))
con.commit()

#skip first to start iterating at second value after table is created
sub_routes = []
for s in route_dir_shape[1:]:
    sub_routes.append(s)
    
for i in xrange(len(sub_routes)):
    cur.execute("""
        with _idea AS (
            SELECT st.*, t.route_id,  t.direction_id, st_l.*
            FROM gtfs_stop_times st
            LEFT JOIN gtfs_trips t ON t.trip_id = st.trip_id
            LEFT JOIN surfacetransit_loads st_l ON st_l.stop_id::text = st.stop_id AND st_l.route = t.route_id
            WHERE st_l.route = '{0}'
            AND t.direction_id = '{1}'
        ),
        tblA AS(
            SELECT route_id, direction_id, direction, count(*) cnt 
            FROM _idea
            GROUP BY route_id, direction_id, direction
            )

        SELECT
            route_id,
            direction_id,
            direction
        FROM tblA
        WHERE cnt = (
            SELECT 
                MAX(cnt)
            FROM tblA)
    """.format(sub_routes[i][0], sub_routes[i][1]))
    print sub_routes[i][0]
    a = cur.fetchall()
    if a != []:
        cur.execute("""
            INSERT INTO direction_crosswalk (route_id, direction_id, direction)
            VALUES (%s, %s, %s)
            """,[a[0][0], a[0][1], a[0][2]])
    con.commit()

#create table with first shape results
cur.execute("""
        CREATE TABLE sequence_loads AS(
            WITH _trips AS (
                SELECT * FROM gtfs_trips WHERE shape_id = '{0}'
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
                ),
            direction_join AS(
                SELECT r.*, d.direction AS direc
                FROM route_dir_shape r
                INNER JOIN direction_crosswalk d
                ON r.route = d.route_id AND r.direction = d.direction_id::text
                ), 
            anotherjoin AS(
                SELECT s.stop_id, s.shape_id, s.shape_pt_sequence, d.route, d.direction, d.direc
                FROM sequence_limits s
                INNER JOIN direction_join d
                ON s.shape_id = d.shape
                )
            SELECT
            a.*,
            sum(s.weekday_lo) as tot_loads
            FROM anotherjoin a
            INNER JOIN surfacetransit_loads s
            ON a.stop_id = s.stop_id::text
            AND a.route = s.route
            AND a.direc = s.direction
            GROUP BY a.stop_id, a.shape_id, a.shape_pt_sequence, a.route, a.direction, a.direc
            ORDER BY a.shape_pt_sequence
            );
        """.format(selected_shapes[0]))
con.commit()

#skip first to start iterating at second value after table is created
sub_shapes = []
for s in selected_shapes[1:]:
    sub_shapes.append(s)
    
for i in xrange(len(sub_shapes)):
    if sub_shapes[i] != 'NA':
        cur.execute("""
        WITH _trips AS (
            SELECT * FROM gtfs_trips WHERE shape_id = '{0}'
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
            ),
        direction_join AS(
            SELECT r.*, d.direction AS direc
            FROM route_dir_shape r
            INNER JOIN direction_crosswalk d
            ON r.route = d.route_id AND r.direction = d.direction_id::text
            ), 
        anotherjoin AS(
            SELECT s.stop_id, s.shape_id, s.shape_pt_sequence, d.route, d.direction, d.direc
            FROM sequence_limits s
            INNER JOIN direction_join d
            ON s.shape_id = d.shape
            )
        SELECT
        a.*,
        sum(s.weekday_lo) as tot_loads
        FROM anotherjoin a
        INNER JOIN surfacetransit_loads s
        ON a.stop_id = s.stop_id::text
        AND a.route = s.route
        AND a.direc = s.direction
        GROUP BY a.stop_id, a.shape_id, a.shape_pt_sequence, a.route, a.direction, a.direc
        ORDER BY a.shape_pt_sequence
        """.format(sub_shapes[i]))
        print sub_shapes[i]
        results = cur.fetchall()
        for row in results:
            cur.execute("""
                INSERT INTO sequence_loads (stop_id, shape_id, shape_pt_sequence, route, direction, direc, tot_loads)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,[row[0], row[1], row[2], row[3], row[4], row[5], row[6]])
        con.commit()
        
        
#create table using first shape again
cur.execute("""
        CREATE TABLE loaded_shapes AS(
            WITH tblA AS(
                SELECT
                    shape_id,
                    st_setsrid(st_makepoint(shape_pt_lon, shape_pt_lat), 4326) AS startpt, 
                    st_setsrid(st_makepoint((LEAD(shape_pt_lon) OVER(ORDER BY shape_pt_sequence ASC)), (LEAD(shape_pt_lat) OVER(ORDER BY shape_pt_sequence ASC))), 4326) AS endpt,
                    shape_pt_sequence
                FROM gtfs_shapes
                WHERE shape_id = '{0}'
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
                    shape_id,
                    shape_pt_sequence,
                    LEAD(shape_pt_sequence) OVER() AS seq_next,
                    tot_loads
                FROM sequence_loads
                WHERE shape_id = '{0}'
                )

            SELECT 
                b.shape_id,
                c.stop_id,
                b.shape_pt_sequence,
                c.tot_loads,
                b.geom
            FROM tblB b
            INNER JOIN tblC c
            ON b.shape_pt_sequence >= c.shape_pt_sequence AND b.shape_pt_sequence < c.seq_next
            );
        """.format(selected_shapes[0]))
con.commit()

#then iterate over the rest of the shapes and insert
for i in xrange(len(sub_shapes)):
    if sub_shapes[i] != 'NA':
        cur.execute("""
            WITH tblA AS(
                SELECT
                    shape_id,
                    st_setsrid(st_makepoint(shape_pt_lon, shape_pt_lat), 4326) AS startpt, 
                    st_setsrid(st_makepoint((LEAD(shape_pt_lon) OVER(ORDER BY shape_pt_sequence ASC)), (LEAD(shape_pt_lat) OVER(ORDER BY shape_pt_sequence ASC))), 4326) AS endpt,
                    shape_pt_sequence
                FROM gtfs_shapes
                WHERE shape_id = '{0}'
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
                    shape_id,
                    shape_pt_sequence,
                    LEAD(shape_pt_sequence) OVER() AS seq_next,
                    tot_loads
                FROM sequence_loads
                WHERE shape_id = '{0}'
                )

            SELECT 
                b.shape_id,
                c.stop_id,
                b.shape_pt_sequence,
                c.tot_loads,
                ST_AsEWKT(b.geom) AS geom
            FROM tblB b
            INNER JOIN tblC c
            ON b.shape_pt_sequence >= c.shape_pt_sequence AND b.shape_pt_sequence < c.seq_next
        """.format(sub_shapes[i]))
        print sub_shapes[i]
        results = cur.fetchall()
        for i in xrange(0,len(results)):
            cur.execute("""
                INSERT INTO loaded_shapes (shape_id, stop_id, shape_pt_sequence, tot_loads, geom)
                VALUES (%s, %s, %s, %s, ST_GeomFromEWKT(%s))
                """,[results[i][0], results[i][1], results[i][2], results[i][3], results[i][4]])
            con.commit()
            
            
#in QGIS to view results with route/direction information
WITH tblA AS(
                SELECT r.*, d.direction AS direc
                FROM route_dir_shape r
                INNER JOIN direction_crosswalk d
                ON r.route = d.route_id AND r.direction = d.direction_id::text
				)
SELECT l.*, r.route, r.direc
FROM loaded_shapes l
INNER JOIN tblA r
ON l.shape_id = r.shape