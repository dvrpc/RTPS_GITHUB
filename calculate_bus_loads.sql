--calculate bus loads from stop level boards/leaves

WITH tblA AS (
SELECT
	route,
	direction,
	sequence,
	stop_id,
	weekday_bo,
	weekday_le,
	weekday_to,
	weekday_bo - weekday_le change
FROM septa__bus_stops
),
tblB AS (
SELECT
	*,
	SUM(weekday_bo) OVER (PARTITION BY route, direction ORDER BY sequence) weekday_tbo,
	SUM(weekday_le) OVER (PARTITION BY route, direction ORDER BY sequence) weekday_tle
FROM tblA
ORDER BY sequence
)
SELECT
	*,
	weekday_tbo - weekday_tle loads
FROM tblB
ORDER BY route, direction, sequence


--how much data is missing by route /direction?

SELECT 
	route,
	direction,
	count(sequence)
FROM septa__bus_stops
WHERE weekday_bo is NULL
GROUP BY route, direction
ORDER BY count desc