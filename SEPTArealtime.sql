-- make points in qgis

WITH tblA AS(
	SELECT trainview.*, trainnos.trainno, consists.consist FROM trainview
	LEFT JOIN trainnos ON trainview.trainnoid = trainnos.id
	LEFT JOIN consists ON trainview.consistid = consists.id)
, tblB AS(SELECT
        *
    FROM tblA
    WHERE lineid = 13
    --April
    AND EXTRACT(MONTH FROM tstz) = 4
    --Weekday
    AND EXTRACT(DOW FROM tstz) = ANY(ARRAY[1,2,3,4,5]::INT[])
    --test day
    AND EXTRACT(DAY FROM tstz) = 2)
SELECT 
    *,
	ST_SetSRID(ST_MakePoint(lon, lat), 26918) as geom
FROM tblB

--need to extract hour to show sequentially throughout day - but really not all that useful

--how about averaging the late field for weekdays over a given month by line?

SELECT
	lineid,
	EXTRACT(DAY FROM tstz),
        AVG(late) AS avg,
        MAX(late) AS max
FROM trainview
--April
WHERE EXTRACT(MONTH FROM tstz) = 4
--Weekday
AND EXTRACT(DOW FROM tstz) = ANY(ARRAY[1,2,3,4,5]::INT[])
AND lineid = 13
--grou by day for testing purposes?
GROUP BY lineid, EXTRACT(DAY FROM tstz)
ORDER BY EXTRACT(DAY FROM tstz)


SELECT
	lineid,
	l.line,
	--EXTRACT(DAY FROM tstz),
        AVG(late) AS avg,
        MAX(late) AS max
FROM trainview
INNER JOIN lines l
ON trainview.lineid = l.id
--April
WHERE EXTRACT(MONTH FROM tstz) = 4
--Weekday
AND EXTRACT(DOW FROM tstz) = ANY(ARRAY[1,2,3,4,5]::INT[])
GROUP BY lineid, l.line

--need to make sure line is moving to count lateness field
--next need to figure out how to do that
--what if we just look at average for peak hours (AM and PM?) - probably not good because we aren't just looking at commuters
-- and this doesnt avoid the actual problem





--from will re: late field
--the late field seems to increase as long as the train is in the system after the last stop
--if you look at some of the trains in the wee hours of the morning, they just keep reporting and the lateness counter increases to like 999
--if the train is 'actively moving' and somewhat following the scheduled route, I'd trust it