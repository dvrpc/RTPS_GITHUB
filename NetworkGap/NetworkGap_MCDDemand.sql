--select by municipality
--toggle to/from
Ç

--example sentence...
--There is an estimated # of trops made FROM MUN_NAME per day

--select by zone
--toggle to/from
	
SELECT
	z.mun_name,
	SUM("DailyVols")
FROM "demand_trim"
INNER JOIN zonemcd_join_region_wpnr_trim z
ON z.no = "demand_trim"."FromZone"
WHERE "FromZone" IN (
	SELECT
		no
	FROM zonemcd_join_region_wpnr_trim
	WHERE mun_name IN (	
	SELECT
		DISTINCT(mun_name)
	FROM(SELECT
		mun_name
	     FROM zonemcd_join_region_wpnr_trim
	     WHERE no IN (5, 2011)
	     ) foo))
GROUP BY z.mun_name


---from will with speed in mind
SELECT mun_name, SUM("DailyVols")
FROM (

SELECT z.mun_name, z.no
FROM zonemcd_join_region_wpnr_trim z
WHERE z.mun_name IN (
SELECT
	z.mun_name
FROM
	zonemcd_join_region_wpnr_trim z
WHERE z.no = ANY(ARRAY[5, 2011])
)

) z

LEFT JOIN demand_trim dm
ON z.no = dm."FromZone"
GROUP BY mun_name