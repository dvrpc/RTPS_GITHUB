WITH tblA AS(
	SELECT 
		no num,
		fromnodeno fnn,
		tonodeno tnn,
		geom,
		CASE
			WHEN z."distinct~1" IS NULL THEN CONCAT('{',z."r_distin~3",'}')
			WHEN z."r_distin~3" IS NULL THEN CONCAT('{',z."distinct~1",'}')
			ELSE CONCAT('{',z."distinct~1",',', z."r_distin~3",'}')
		END AS combination
	FROM all_model_nondirected_link z
),

tblB AS(
	SELECT 
		num, fnn, tnn,geom,
		ARRAY(SELECT DISTINCT UNNEST(combination::TEXT[]) ORDER BY 1) lineids
	FROM tblA
)

SELECT
	num, fnn, tnn,
	CONCAT(fnn, tnn) combo,
	ARRAY_TO_STRING(lineids,',') lines,
	geom
FROM tblB