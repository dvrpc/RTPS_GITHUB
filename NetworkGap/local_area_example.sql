WITH tblA AS(
	SELECT
		"FromZone",
		SUM("ConnectionScore"*demandscore)/SUM(demandscore) AS w_avg_con,
		SUM("gapscore"*demandscore)/SUM(demandscore) AS w_avg_gap
	FROM odgaps_ts_s_trim
	WHERE "ToZone" IN (10225, 10220, 10219, 10222)
	AND demandscore <> 0
	GROUP BY "FromZone"
	)
SELECT 
	a.*,
	z.geom
FROM tblA a
INNER JOIN zonemcdm z
ON a."FromZone" = z.no