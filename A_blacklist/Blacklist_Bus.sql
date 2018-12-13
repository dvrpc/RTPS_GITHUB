WITH STOPAREAREALTSYS AS (
    -- Concatenate the TSYSCodes together by StopAreaNo
    SELECT STOPAREANO, group_concat(TSYSCODE, ",") TSYSSET
    FROM (
        -- Associate the StopPoint to a StopArea and remove duplicate StopArea-TSYS entries
        SELECT STOPPOINT.STOPAREANO, q.TSYSCODE
        FROM (
            -- From the LineRouteItem list, filter records for populated StopPointNo fields
            -- Then link to Line to grab the TSYS and finally consolidate the duplicates
            SELECT LINEROUTEITEM.STOPPOINTNO, LINE.TSYSCODE FROM LINEROUTEITEM
            LEFT JOIN LINE ON LINEROUTEITEM.LINENAME = LINE.NAME
            WHERE STOPPOINTNO <> ""
            GROUP BY STOPPOINTNO, TSYSCODE
        ) q
        LEFT JOIN STOPPOINT ON q.STOPPOINTNO = STOPPOINT.NO
        GROUP BY STOPAREANO, TSYSCODE
    ) s
    GROUP BY STOPAREANO
),
STOPAREABLACKLIST AS (
    SELECT * FROM STOPAREAREALTSYS
    -- SQLite string search syntax
    -- WHERE TSYSSET LIKE "%Bus%"
    -- Filter for Stop Areas with only Bus (Bus + Other is OK (?))
    WHERE TSYSSET = "Bus"
)

SELECT
    FROMSTOPAREANO,
    TOSTOPAREANO,
    TSYSCODE,
    TIME
FROM (

    SELECT
        twtsa.FROMSTOPAREANO,
        sartf.TSYSSET FROMTSYSSET,
        twtsa.TOSTOPAREANO,
        sartt.TSYSSET TOTSYSSET,
        -- twtsa.TIME
        twtsa.TSYSCODE,
        -- Visum maxmium transfer time is 86400 (1 Day) seconds
        '86399s' AS TIME
    FROM TRANSFERWALKTIMESTOPAREA twtsa
    LEFT JOIN STOPAREABLACKLIST sartf
    ON sartf.STOPAREANO = twtsa.FROMSTOPAREANO
    LEFT JOIN STOPAREABLACKLIST sartt 
    ON sartt.STOPAREANO = twtsa.TOSTOPAREANO
    WHERE
    -- Find SA-SA pairs where one end is 'Bus' but not and both ends are NULL
    ((FROMTSYSSET IS NULL OR TOTSYSSET IS NULL) AND NOT (FROMTSYSSET IS NULL AND TOTSYSSET IS NULL))
    OR
    -- Find SA-SA pairs where both ends are 'Bus'
    (FROMTSYSSET = "Bus" AND TOTSYSSET = "Bus")

)
