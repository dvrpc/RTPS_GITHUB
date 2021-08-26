
import numpy
import csv
import scipy
import pandas as pd
import psycopg2 as psql 
from sqlalchemy import create_engine
import geopandas as gpd
import matplotlib.pyplot as plt



#import DVRPC transit score shapefile to postgres DB
#only needs to be done once - if troubleshooting, remove/comment out
engine = create_engine('postgresql://postgres:sergt@localhost:5432/rtsp')
transitscore_df = gpd.read_file('D:/BikePedTransit/RTPS/shapes/DVRPC_TransitScore_2015.shp')
transitscore_df.to_postgis('dvrpc_transitscore_2015',engine, index=True, index_label='Index')

#connect to SQL DB in python
con = psql.connect(dbname = "rtsp", host = "localhost", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()

#one time zonal updates to ensure transit score table aligns with zonal information pulled from model
#add zoo zone to transit score table (was combined with 1040)
cur.execute("""INSERT INTO dvrpc_transitscore_2015(tazn, tscategory, tscatnum)
            VALUES 
                (2218, 'Medium', 3);
            COMMIT;""")
#repeat for tavistock zone (was combined with 22429)
cur.execute("""INSERT INTO dvrpc_transitscore_2015(tazn, tscategory, tscatnum)
            VALUES 
                (22446, 'Medium', 3);
            COMMIT;""")


#Query to combine connection score, demand score, and zonal geometries into a single table
Q_combine = """
CREATE TABLE odgaps AS(
    WITH tblA AS(
        SELECT 
            c.*,
            d."DailyVols",
            d."DemScore" demandscore
        FROM "ConnectionScore" c
        INNER JOIN "DemandScore" d
        ON d."FromZone" = c."FromZone"
        AND d."ToZone" = c."ToZone")
        
	SELECT 
		a.*,
		z1.no fromzone,
		z1.geom fromgeom,
		z2.no tozone,
		z2.geom togeom
	FROM tblA a
	INNER JOIN "zone_geom" z1
	ON z1.no = a."FromZone"
	INNER JOIN "zone_geom" z2
	ON z2.no = a."ToZone");
	
COMMIT;
"""
cur.execute(Q_combine)


#join transit score to od pair table
#this table will be queried for local area analyses
Q_join_ts = """CREATE TABLE odgaps_ts AS(
    SELECT
        o2.*,
        (o_ts + d_ts) AS sum_ts
    FROM(
        SELECT
            o.*,
            d1.tscatnum AS o_ts,
            d2.tscatnum AS d_ts
        FROM odgaps o
        INNER JOIN dvrpc_transitscore_2015 d1
        ON o."FromZone" = d1.tazn
        INNER JOIN dvrpc_transitscore_2015 d2
        ON o."ToZone" = d2.tazn) o2
        );
COMMIT;
"""
cur.execute(Q_join_ts)


#test that all zones are included
Q_test = """
SELECT
   DISTINCT("FromZone")
FROM odgaps
WHERE "FromZone" NOT IN (
	SELECT
		DISTINCT("FromZone")
	FROM odgaps_ts)
    """
cur.execute(Q_test)
t = cur.fetchall()
print t


#create indices for faster processing
Q_index_a = """
CREATE INDEX odgaps_ts_idx_base_s
  ON public.odgaps_ts
  USING btree
  ("FromZone", "ToZone", "NumTransfers", "TrWait", "DistanceFlag", "TimeFlag", "TransferPoint", "TWTPoint", "ConnectionScore", "DailyVols", demandscore, o_ts, d_ts, sum_ts);
"""
Q_index_b = """
CREATE INDEX odgaps_ts_idx_g_s
  ON public.odgaps_ts
  USING gist
  (fromgeom, togeom);
  """
cur.execute(Q_index_a)
cur.execute(Q_index_b)


  
#add new column and calculate gap score which is the product of the sum_ts and the connection score
Q_addcol = """
ALTER TABLE odgaps_ts
ADD COLUMN gapscore double precision;
COMMIT;"""

Q_update = """
UPDATE odgaps_ts
SET gapscore = "ConnectionScore"*sum_ts;
COMMIT;"""

Q_index_c = """
CREATE INDEX odgaps_ts_idx_gap_s
  ON public.odgaps_ts
  USING btree
  (gapscore);
  """

cur.execute(Q_addcol)
cur.execute(Q_update)
cur.execute(Q_index_c)

#create summary table incorporating transit score (density) to be used as regional summary map
Q_summary_table = """
CREATE TABLE odgaps_ts_summary AS(
    WITH tblA AS(
        SELECT 
            "ToZone" AS zone,
            SUM("ConnectionScore"*demandscore)/SUM(demandscore) AS w_avg_con,
            SUM("gapscore"*demandscore)/SUM(demandscore) AS w_avg_score,
            AVG("DailyVols") AS avgvol,
            SUM("DailyVols") AS sumvol,
            togeom AS geom
        FROM odgaps_ts
        WHERE demandscore <> 0
        GROUP BY "ToZone", togeom
        ORDER BY "ToZone"),
     tblB AS(
        SELECT 
            "FromZone" AS zone,
            SUM("ConnectionScore"*demandscore)/SUM(demandscore) AS w_avg_con,
            SUM("gapscore"*demandscore)/SUM(demandscore) AS w_avg_score,
            AVG("DailyVols") AS avgvol,
            SUM("DailyVols") AS sumvol,
            fromgeom AS geom
        FROM odgaps_ts
        WHERE demandscore <> 0
        GROUP BY "FromZone", fromgeom
        ORDER BY "FromZone")

    SELECT 
        tblA.zone,
        ((tblA.w_avg_con*tblA.sumvol)+(tblB.w_avg_con*tblB.sumvol))/(tblA.sumvol+tblB.sumvol) AS w_avgcon,
        ((tblA.w_avg_score*tblA.sumvol)+(tblB.w_avg_score*tblB.sumvol))/(tblA.sumvol+tblB.sumvol) AS w_avgscore,
        ((tblA.avgvol*tblA.sumvol)+(tblB.avgvol*tblB.sumvol))/(tblA.sumvol+tblB.sumvol) AS w_avgvol,
        tblA.geom
    FROM tblA
    INNER JOIN tblB
    ON tblA.zone = tblB.zone);
    COMMIT;
    """
#index summary table  
Q_index_d = """
CREATE INDEX odgaps_ts_summary_idx_base_s
  ON public.odgaps_ts_summary
  USING btree
  (zone, w_avgcon, w_avgscore, w_avgvol);
  """
Q_index_e = """
CREATE INDEX odgaps_ts_summary_idx_geo_s
  ON public.odgaps_ts_summary
  USING gist
  (geom);
  """

cur.execute(Q_summary_table)
cur.execute(Q_index_d)
cur.execute(Q_index_e)