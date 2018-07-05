--preceded by ScheduledSpeed_fromVisum.py

CREATE EXTENSION postgis;

-- create tables to hold imported data from visum/python
CREATE TABLE public.StopPoints (
	num integer,
	stopname character varying(100),
	stoptype float,
	stoptsys character varying(50),
	geom geometry(Geometry)
	);
COMMIT;

CREATE TABLE public.vehjourneyitems
(
  linename character varying(50),
  lineroute character varying(50),
  direction character varying(5),
  vehjourno integer,
  vjindex integer,
  prevdep float,
  arrival float,
  prelength float,
  prevlri integer,
  curlri integer,
  prevstop integer,
  currentstop integer  
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.vehjourneyitems
  OWNER TO postgres;

CREATE TABLE public.LineRouteItems (
	LRIndex integer,
	linename character varying(50),
    lineroute character varying(50),
	direction character varying(5),
	LRIstop integer,
	LRInode integer
	);
COMMIT;

-- add primary key
ALTER TABLE linerouteitems
ADD COLUMN id SERIAL;

ALTER TABLE linerouteitems
ADD CONSTRAINT pk
PRIMARY KEY (id);
	
-- import csv files into these tables through pgadmin GUI

--change geometry of imported data to UTM 18N
SELECT UpdateGeometrySRID('stoppoints','geom', 26918);
--check that it worked
SELECT Find_SRID('public', 'stoppoints','geom');


--older troubleshooting
-- SELECT 
    -- concat(linename, lineroute, direction) AS combo,
    -- lrindex,
    -- lristop,
    -- lrinode,
    -- lag(lrinode) OVER (ORDER BY id) prevnode,
    -- lead(lrinode,2) OVER (ORDER BY id) postnode
-- FROM linerouteitems
-- WHERE linename = '23'
-- AND lineroute = 'sepb_4660026'
-- AND direction = '>'
--look at LRIndex = 123

-- for reference - speed calculation
	-- SELECT 
		-- concat(linename, lineroute, direction) AS combo,
		-- vehjourno,
		-- Min(prevlri) MinLRI,
		-- Max(curlri) MaxLRI,
		-- SUM(prelength) AS sumlength,
		-- MAX(arrival)-MIN(prevdep) AS elapsedsec,
		-- (((MAX(CAST(arrival AS FLOAT))-MIN(CAST(prevdep AS FLOAT)))/60)/60) AS elapsedhr,
		-- SUM(prelength)/(((MAX(CAST(arrival AS FLOAT))-MIN(CAST(prevdep AS FLOAT)))/60)/60) AS speed
	-- FROM vehjourneyitems
	-- WHERE vehjourno = 61836
	-- GROUP BY linename, lineroute, direction, vehjourno, prevdep
	-- ORDER BY MinLRI



--CREATE RANGE SPEED TABLE with speed calculation
CREATE TABLE public.rangespeedtbl AS 
 WITH tbla AS (
         SELECT concat(vehjourneyitems.linename, vehjourneyitems.lineroute, vehjourneyitems.direction) AS comboa,
            vehjourneyitems.linename,
            vehjourneyitems.lineroute,
            vehjourneyitems.direction,
            vehjourneyitems.vehjourno AS vjno,
            vehjourneyitems.vjindex,
            vehjourneyitems.prevdep,
            vehjourneyitems.arrival,
            vehjourneyitems.prelength,
            vehjourneyitems.prevstop,
            vehjourneyitems.currentstop,
            vehjourneyitems.prevlri,
            vehjourneyitems.curlri
           FROM vehjourneyitems
          --WHERE vehjourneyitems.vehjourno = 61836
        ), 
        tblb AS (
         SELECT concat(vehjourneyitems.linename, vehjourneyitems.lineroute, vehjourneyitems.direction) AS combo,
            vehjourneyitems.vehjourno,
            min(vehjourneyitems.prevlri) AS minlri,
            max(vehjourneyitems.curlri) AS maxlri,
            sum(vehjourneyitems.prelength) AS sumlength,
            max(vehjourneyitems.arrival) - min(vehjourneyitems.prevdep) AS elapsedsec,
            (max(vehjourneyitems.arrival::double precision) - min(vehjourneyitems.prevdep::double precision)) / 60::double precision / 60::double precision AS elapsedhr,
            sum(vehjourneyitems.prelength) / ((max(vehjourneyitems.arrival::double precision) - min(vehjourneyitems.prevdep::double precision)) / 60::double precision / 60::double precision) AS speed
           FROM vehjourneyitems
          WHERE vehjourneyitems.vehjourno NOT IN(19193, 19194, 19195, 19196, 19197, 19198, 19199, 19200,19201,19202,19203,19204,19205,19206,19207,19209,19210,19211,19212,19213,19221,19222,19223,19224,19225,19227,19229,19230,19231,19232,19233,19234,19235,19236,19237,19240,19243,19244,19245,19246,19247,12460,12461,16826,12894,814)          GROUP BY vehjourneyitems.linename, vehjourneyitems.lineroute, vehjourneyitems.direction, vehjourneyitems.vehjourno, vehjourneyitems.prevdep
          -- these 46 vehicle journeys cause a division by zero error
          ORDER BY (min(vehjourneyitems.prevlri))
        )
 SELECT tbla.comboa,
    tbla.linename,
    tbla.lineroute,
    tbla.direction,
    tbla.vjno,
    tbla.vjindex,
    tbla.prevdep,
    tbla.arrival,
    tbla.prelength,
    tbla.prevstop,
    tbla.currentstop,
    tbla.prevlri,
    tbla.curlri,
    tblb.combo,
    tblb.vehjourno,
    tblb.minlri,
    tblb.maxlri,
    tblb.sumlength,
    tblb.elapsedsec,
    tblb.elapsedhr,
    tblb.speed
   FROM tbla
     JOIN tblb 
     ON tbla.comboa = tblb.combo 
     AND tbla.vjno = tblb.vehjourno 
     AND tbla.prevlri = tblb.minlri
  ORDER BY tblb.combo, tblb.vehjourno, tblb.minlri;
  
-- CREATE FROMTORANGE TABLE to join to links

CREATE TABLE fromtorange AS(
	WITH lri AS (
		SELECT 
			concat(linename, lineroute, direction) AS combo,
			lrindex,
			lristop,
			lrinode,
			id,
            row_number() OVER (
                PARTITION BY linename, lineroute, direction 
                ORDER BY linename, lineroute, direction, lrindex
            ) AS nodeindex
		FROM linerouteitems
        WHERE lrinode IS NOT NULL
        ORDER BY linename, lineroute, direction, lrindex
		)

        SELECT 
            lri1.*,
            lri2.lrinode AS prevnode,
            r.combo AS combo2,
            r.linename,
            r.lineroute,
            r.direction,
            r.vehjourno,
            r.minlri,
            r.maxlri,
            r.speed
        FROM lri lri1
        LEFT JOIN rangespeedtbl r
         ON r.combo = lri1.combo
         AND r.minlri < lri1.lrindex
         AND lri1.lrindex < r.maxlri
        LEFT JOIN lri lri2
         ON lri1.combo = lri2.combo
         AND lri1.nodeindex = lri2.nodeindex + 1
        ORDER BY combo2, vehjourno, lrindex
        );

-- CREATE TABLE with join to links
CREATE TABLE linkspeed AS(
	SELECT
		l.no,
		l.fromnodeno,
		l.tonodeno,
		f.combo,
        f.linename,
		f.lrinode,
		f.prevnode,
		f.vehjourno,
		f.speed,
		l.geom
	FROM base_link l
	INNER JOIN fromtorange f
	ON f.prevnode = l.fromnodeno
	AND f.lrinode = l.tonodeno
	);

-- CREATE TABLE to summarize link speed by line
CREATE TABLE linkspeed_byline AS (
	SELECT 
		a.*,
		b.geom
	FROM (
		SELECT
			linename,
			no,
			fromnodeno,
			tonodeno,
			count(*) AS cnt,
			avg(speed) AS avgspeed
		FROM linkspeed
		GROUP BY linename, no, fromnodeno, tonodeno
	) a
	LEFT JOIN base_link b
	ON a.no = b.no
	AND a.fromnodeno = b.fromnodeno
	AND a.tonodeno = b.tonodeno
);




 
