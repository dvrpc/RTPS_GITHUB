
--combine conneciton score and demand score tables; get geometries from zonal_geom table
CREATE TABLE odgaps AS(
    WITH tblA AS(
        SELECT 
            c.*,
            d."DailyVols",
            d."DemScore" demandscore
        FROM "ConnectionScore2" c
        INNER JOIN "DemandScore2" d
        ON d."FromZone" = c."FromZone"
        AND d."ToZone" = c."ToZone")
        
	SELECT 
		a.*,
		z1.no fromzone,
		z1.geom fromgeom,
		z2.no tozone,
		z2.geom togeom
	FROM tblA a
	INNER JOIN "zonal_geom" z1
	ON z1.no = a."FromZone"
	INNER JOIN "zonal_geom" z2
	ON z2.no = a."ToZone");
	
COMMIT;

-- add zoo zone to transit score table (was combined with 1040)
INSERT INTO dvrpc_transitscore_2015(tazn, tscategory, tscatnum)
VALUES 
	(2218, 'Medium', 3);
COMMIT;
-- repeat for tavistock zone (was combined with 22429)
INSERT INTO dvrpc_transitscore_2015(tazn, tscategory, tscatnum)
VALUES 
	(22446, 'Medium', 3);
COMMIT;


-- join transit score to od pair table for method 1 of incorporating transit score/normalizing by pop/emp den
CREATE TABLE odgaps_ts AS(
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
        
-- check to see if 2218 is in table
SELECT
    *
FROM odgaps_ts
WHERE "FromZone" = 2218 --(22446)

--test that all are included
SELECT
   DISTINCT("FromZone")
FROM odgaps
WHERE "FromZone" NOT IN (
	SELECT
		DISTINCT("FromZone")
	FROM odgaps_ts)


--create indices
CREATE INDEX odgaps_ts_idx_base
  ON public.odgaps_ts
  USING btree
  ("FromZone", "ToZone", "NumTransfers", "TrWait", "DistanceFlag", "TimeFlag", "TransferPoint", "TWTPoint", "ConnectionScore", "DailyVols", demandscore, o_ts, d_ts, sum_ts);

CREATE INDEX odgaps_ts_idx_g
  ON public.odgaps_ts
  USING gist
  (fromgeom, togeom);
  
--add new gap score which is the product of the sum_ts and the connection score
ALTER TABLE odgaps_ts
ADD COLUMN gapscore double precision;

UPDATE odgaps_ts
SET gapscore = "ConnectionScore"*sum_ts;

--create indices
CREATE INDEX odgaps_ts_idx_gap
  ON public.odgaps_ts
  USING btree
  (gapscore);



--summary table incorporating ts
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
    
    
--create indices
CREATE INDEX odgaps_ts_summary_idx_base
  ON public.odgaps_ts_summary
  USING btree
  (zone, w_avgcon, w_avgscore, w_avgvol);


CREATE INDEX odgaps_ts_summary_idx_geo
  ON public.odgaps_ts_summary
  USING gist
  (geom);
    
---small area selection with new TS1 method---
-- to UMT
    SELECT
        "FromZone",
        SUM("ConnectionScore"*demandscore)/SUM(demandscore) AS w_avg_con,
        SUM("gapscore"*demandscore)/SUM(demandscore) AS w_avg_gap,
        AVG("ConnectionScore") AS avgcon,
        AVG(gapscore) AS avggap,
        --AVG("DailyVols") AS avgvol,
        fromgeom
    FROM odgaps_ts
    WHERE "ToZone" IN (10225, 10220, 10219, 10222)
    AND demandscore <> 0
    GROUP BY "FromZone", fromgeom
    
-- to Center City
    SELECT
        "FromZone",
        SUM("ConnectionScore"*demandscore)/SUM(demandscore) AS w_avg_con,
        SUM("gapscore"*demandscore)/SUM(demandscore) AS w_avg_gap,
        AVG("ConnectionScore") AS avgcon,
        AVG(gapscore) AS avggap,
        --AVG("DailyVols") AS avgvol,
        fromgeom
    FROM odgaps_ts
    WHERE "ToZone" < 200
    AND demandscore <> 0
    GROUP BY "FromZone", fromgeom
    
-- to University City
    SELECT
        "FromZone",
        SUM("ConnectionScore"*demandscore)/SUM(demandscore) AS w_avg_con,
        SUM("gapscore"*demandscore)/SUM(demandscore) AS w_avg_gap,
        AVG("ConnectionScore") AS avgcon,
        AVG(gapscore) AS avggap,
        --AVG("DailyVols") AS avgvol,
        fromgeom
    FROM odgaps_ts
    WHERE "ToZone" < 1100 
    AND "ToZone" > 1000
    AND demandscore <> 0
    GROUP BY "FromZone", fromgeom
    
    
-- Greater Center City (Center City and University City Combined)
    SELECT
        "FromZone",
        SUM("ConnectionScore"*demandscore)/SUM(demandscore) AS w_avg_con,
        SUM("gapscore"*demandscore)/SUM(demandscore) AS w_avg_gap,
        AVG("ConnectionScore") AS avgcon,
        AVG(gapscore) AS avggap,
        --AVG("DailyVols") AS avgvol,
        fromgeom
    FROM odgaps_ts
    WHERE "ToZone" < 1100 
    AND "ToZone" > 1000
    AND demandscore <> 0
    OR "ToZone" < 200
    GROUP BY "FromZone", fromgeom
    
    
    
-- from Glassboro
    SELECT
        "ToZone",
        SUM("ConnectionScore"*demandscore)/SUM(demandscore) AS w_avg_con,
        SUM("gapscore"*demandscore)/SUM(demandscore) AS w_avg_gap,
        AVG("ConnectionScore") AS avgcon,
        AVG(gapscore) AS avggap,
        --AVG("DailyVols") AS avgvol,
        togeom
    FROM odgaps_ts
    WHERE "FromZone" IN(
        SELECT
            no
        FROM zonemcd_join_region_wpnr
        WHERE mun_name = 'Glassboro Borough' )
    AND demandscore <> 0
    GROUP BY "ToZone", togeom
    
    
--from washington township in gloucester county
    SELECT
        "ToZone",
        SUM("ConnectionScore"*demandscore)/SUM(demandscore) AS w_avg_con,
        SUM("gapscore"*demandscore)/SUM(demandscore) AS w_avg_gap,
        AVG("ConnectionScore") AS avgcon,
        AVG(gapscore) AS avggap,
        --AVG("DailyVols") AS avgvol,
        togeom
    FROM odgaps_ts
    WHERE "FromZone" IN(
        SELECT no
        FROM zonemcd_join_region_wpnr
        WHERE mun_name = 'Washington Township'
        AND co_name_1 = 'Gloucester' )
    AND demandscore <> 0
    GROUP BY "ToZone", togeom

--from gloucester township
    SELECT
        "ToZone",
        SUM("ConnectionScore"*demandscore)/SUM(demandscore) AS w_avg_con,
        SUM("gapscore"*demandscore)/SUM(demandscore) AS w_avg_gap,
        AVG("ConnectionScore") AS avgcon,
        AVG(gapscore) AS avggap,
        --AVG("DailyVols") AS avgvol,
        togeom
    FROM odgaps_ts
    WHERE "FromZone" IN(
        SELECT no
        FROM zonemcd_join_region_wpnr
        WHERE mun_name = 'Gloucester Township' )
    AND demandscore <> 0
    GROUP BY "ToZone", togeom
    
    
--to marlton
    SELECT
        "FromZone",
        SUM("ConnectionScore"*demandscore)/SUM(demandscore) AS w_avg_con,
        SUM("gapscore"*demandscore)/SUM(demandscore) AS w_avg_gap,
        AVG("ConnectionScore") AS avgcon,
        AVG(gapscore) AS avggap,
        --AVG("DailyVols") AS avgvol,
        fromgeom
    FROM odgaps_ts
    WHERE "ToZone" IN (20216, 20214, 20212, 20213, 20208, 20210)
    AND demandscore <> 0
    GROUP BY "FromZone", fromgeom
    
    
--from marlton
    SELECT
        "ToZone",
        SUM("ConnectionScore"*demandscore)/SUM(demandscore) AS w_avg_con,
        SUM("gapscore"*demandscore)/SUM(demandscore) AS w_avg_gap,
        AVG("ConnectionScore") AS avgcon,
        AVG(gapscore) AS avggap,
        --AVG("DailyVols") AS avgvol,
        togeom
    FROM odgaps_ts
    WHERE "FromZone" IN (20216, 20214, 20212, 20213, 20208, 20210)
    AND demandscore <> 0
    GROUP BY "ToZone", togeom
    
-- UMT zones: 10225, 10220, 10219, 10222
-- Center City: < 200
-- University City: 1000-1100
-- Glassboro:24604-24613

    
   




