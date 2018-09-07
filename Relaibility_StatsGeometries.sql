--insert missing values into stats by line table from SEPTA 2017 route statistics report
INSERT INTO statsbyline_original(
	linename,
	ampeak_f,
	base_freq,
	routemiles,
	dailyrider,
	otp,
	division)
VALUES
	('311',30,60,8.5, 725, 71, 'Suburban'),
	('45',6,10,4.3,4299,84,'City'),
	('91',0,0,14.2,90,87,'Suburban');

--create table with attributes from original stats by line and geometries from NJT and SEPTA bus route GIS files
CREATE TABLE statsbyline_allgeom AS(
    SELECT 
        s.gid,
        s.linename::text,
        s.name,
        s.ampeak_f,
        s.base_freq,
        s.routemiles,
        s.dailyrider,
        s.riderrank,
        s.otp,
        s.division,
        n.geom
    FROM (SELECT 
            *
        FROM njt_bus_dissolve
        WHERE line::integer > 300
        AND line::integer < 700) n
    INNER JOIN statsbyline_original s
    ON s.linename::text = n.line::integer::text

    UNION

    
SELECT 
        s.gid,
        s.linename::text,
        s.name,
        s.ampeak_f,
        s.base_freq,
        s.routemiles,
        s.dailyrider,
        s.riderrank,
        s.otp,
        s.division,
        t.geom
    FROM septa_transitroutes_spring2017 t
    INNER JOIN statsbyline_original s
    ON s.linename::text = t.route::text);
    
    
--create table joining frequency results to NJT and SEPTA bus route GIS data
--use other file for trolleys, RR, subway (joined to simplified line routes)

    
-------------------------------------------------------------------------------------------------------------
---troubleshooting
WITH tblA AS(
    SELECT 
        DISTINCT(foo.linename)
    FROM(SELECT 
            s.gid,
            s.linename::text,
            s.name,
            s.ampeak_f,
            s.base_freq,
            s.routemiles,
            s.dailyrider,
            s.riderrank,
            s.otp,
            s.division,
            n.geom
        FROM (SELECT 
                    *
                FROM njt_bus_dissolve
                WHERE line::integer > 300
                AND line::integer < 700) n
        INNER JOIN statsbyline_original s
        ON s.linename::text = n.line::integer::text) foo
    ORDER BY linename ASC
    ),
    
tblB AS(
    SELECT 
        DISTINCT(faa.linename)
    FROM(SELECT 
            s.gid,
            s.linename::text,
            s.name,
            s.ampeak_f,
            s.base_freq,
            s.routemiles,
            s.dailyrider,
            s.riderrank,
            s.otp,
            s.division,
            t.geom
        FROM septa_transitroutes_spring2017 t
        INNER JOIN statsbyline_original s
        ON s.linename::text = t.route::text) faa
    ORDER BY linename ASC
    )

SELECT linename
FROM tblA
WHERE linename IN (
	SELECT linename
	FROM tblB)
---want this to return nothing

---more troubleshooting
---which lines are inclided in linkspeedbyline that are not included in these results
---turnes out its just trolleys, RR, subway, and shuttles!
WITH tblA AS(
    SELECT
        DISTINCT(linename)
    FROM (SELECT 
            s.gid,
            s.linename::text,
            s.name,
            s.ampeak_f,
            s.base_freq,
            s.routemiles,
            s.dailyrider,
            s.riderrank,
            s.otp,
            s.division,
            n.geom
        FROM (SELECT 
                *
            FROM njt_bus_dissolve
            WHERE line::integer > 300
            AND line::integer < 700) n
        INNER JOIN statsbyline_original s
        ON s.linename::text = n.line::integer::text

        UNION

        SELECT 
            s.gid,
            s.linename::text,
            s.name,
            s.ampeak_f,
            s.base_freq,
            s.routemiles,
            s.dailyrider,
            s.riderrank,
            s.otp,
            s.division,
            t.geom
        FROM septa_transitroutes_spring2017 t
        INNER JOIN statsbyline_original s
        ON s.linename::text = t.route::text) foo)
        
SELECT 
	DISTINCT(linename)::text
FROM linkspeed_byline
WHERE linename NOT IN(
    SELECT linename 
    FROM tblA);

