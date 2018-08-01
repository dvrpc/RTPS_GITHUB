--What is made inaccessible by these inaccessible stations
WITH tblA AS(
	SELECT
		c."ZoneNo",
		c."NodeNo",
		r.base_nb,
		r.notacc_nb,
		r.notaccorprog_nb,
		r.td_base_nb,
		r.td_notacc_nb,
		r.td_notaccorprog_nb
	FROM "60minRIT_delSA" r
	INNER JOIN "Acc_Twk_Connectors" c
	ON r.no = c."ZoneNo")

SELECT
	a."Station",
	--ARRAY_AGG(DISTINCT tblA."ZoneNo") AS zones,
	--ARRAY_AGG(DISTINCT tblA."td_base_nb") AS accessible,
	AVG(tblA.base_nb) AS avg_base_nb,
	AVG(tblA.notacc_nb) AS avg_notacc_nb,
	AVG(tblA.notaccorprog_nb) AS avg_notaccorprog_nb,
	AVG(tblA.td_base_nb) AS avg_td_base_nb,
	AVG(tblA.td_notacc_nb) AS avg_td_notacc_nb,
	AVG(tblA.td_notaccorprog_nb) AS avg_notaccorprog_nb
FROM "Acc_NotAccORProg" a
INNER JOIN tblA
ON a."NodeNo" = tblA."NodeNo"
GROUP BY a."Station"

