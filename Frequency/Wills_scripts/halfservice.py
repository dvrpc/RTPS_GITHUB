#ghiller memorial script - man loved him some in memory sql, still does probably
import VisumPy.helpers as h
import sqlite3
import time

SQL_CREATE_TABLE = """
CREATE TABLE vehjourn (
    no INTEGER,
    dep REAL,
    linename TEXT,
    lineroutename TEXT,
    directioncode TEXT,
    timeprofilename TEXT
)
"""

SQL_INSERT_TABLE = """
INSERT INTO vehjourn VALUES (?,?,?,?,?,?)
"""

SQL_GROUP = """
SELECT
    linename,
    directioncode
FROM vehjourn
GROUP BY linename, directioncode
"""

SQL_GET_GROUP = """
SELECT
    no,
    lineroutename,
    timeprofilename,
    dep
FROM vehjourn
WHERE linename = ?
AND directioncode = ?
ORDER BY dep
"""

SQL_GET_MAX_NO = """
SELECT
    MAX(no)
FROM vehjourn
"""

def ScaleIntDistribution(dist, coefficient, minimum = 1):
    def _loop(reverse = False):
        return filter(
            lambda (i, v):target_dist[i] > minimum,
            sorted(
                enumerate(target_dist_remainders),
                key = lambda (i, v): v,
                reverse = reverse
            )
        )
    dist_sum = float(sum(dist))
    dist_perc = [i / dist_sum for i in dist]
    target_sum = dist_sum * coefficient
    target_sum_int = int(target_sum)
    if (len(dist) > target_sum):
        return [1] * len(dist)
    target_dist = [i * target_sum for i in dist_perc]
    target_dist_remainders = [i % 1 for i in target_dist]
    new_dist = [max(minimum, int(i)) for i in target_dist]
    new_dist_sum = sum(new_dist)
    if (new_dist_sum > target_sum_int):
        for i, v in _loop():
            new_dist[i] -= 1
            if (sum(new_dist) == target_sum_int):
                break
    elif (new_dist_sum < target_sum_int):
        for i, v in _loop(True):
            new_dist[i] += 1
            if (sum(new_dist) == target_sum_int):
                break
    return new_dist

def DailySecondsToHHMMSS(seconds):
    h = seconds / 3600
    _s = seconds % 3600
    m = _s / 60
    s = _s % 60
    return "%02d:%02d:%02d" % (h, m, s)

def getHeadways(departure_times):
    dt = departure_times
    return [dt[i+1] - t for (i, t) in enumerate(dt[:-1])]

def scaleHeadways(headways, coeff):
    n = len(headways)
    _n = int(n * (1 + coeff))
    if n == _n:
        return headways
    avg_headway = sum(headways)/float(n)

def DoubleService():
    vehjourn = Visum.Net.VehicleJourneys.GetMultipleAttributes([
        "No",
        "Dep",
        "LineName",
        "LineRouteName",
        "DirectionCode",
        "TimeProfileName"
    ], OnlyActive = True)

    con = sqlite3.connect(":memory:")
    cur = con.cursor()
    cur.execute(SQL_CREATE_TABLE)
    cur.executemany(SQL_INSERT_TABLE, vehjourn)

    (counter,) = cur.execute(SQL_GET_MAX_NO).fetchone()
    counter += 1

    cur.execute(SQL_GROUP)
    linename_dircode = cur.fetchall()

    for ln, dc in linename_dircode:
        cur.execute(SQL_GET_GROUP, (ln, dc))
        payload = cur.fetchall()
        print ln,
        if len(payload) > 1:
            for i, (vjno, lrn, tpn, dep) in enumerate(payload[:-1]):
                headway = payload[i+1][3] - dep
                depaturetime = DailySecondsToHHMMSS(dep + (headway/2.0))
                TP = Visum.Net.TimeProfiles.ItemByKey(ln, dc, lrn, tpn)
                VJ = Visum.Net.AddVehicleJourney(No = counter, TimeProfile = TP)
                VJ.SetAttValue("DEP", depaturetime)
                counter += 1

def HalfService():
    vehjourn = Visum.Net.VehicleJourneys.GetMultipleAttributes([
        "No",
        "Dep",
        "LineName",
        "LineRouteName",
        "DirectionCode",
        "TimeProfileName"
    ], OnlyActive = True)

    con = sqlite3.connect(":memory:")
    cur = con.cursor()
    cur.execute(SQL_CREATE_TABLE)
    cur.executemany(SQL_INSERT_TABLE, vehjourn)

    cur.execute(SQL_GROUP)
    linename_dircode = cur.fetchall()

    for ln, dc in linename_dircode:
        cur.execute(SQL_GET_GROUP, (ln, dc))
        payload = cur.fetchall()
        print ln,
        if len(payload) > 1:
            for i in xrange(0, len(payload), 2):
                (vjno, lrn, tpn, dep) = payload[i]
                VJ = Visum.Net.VehicleJourneys.ItemByKey(No = vjno)
                VJ.SetAttValue("delflag", True)

if __name__ == "__main__":
    Visum.Graphic.StopDrawing = True
    HalfService()
    Visum.Graphic.StopDrawing = False


""" ye olde
_PERCENTAGES = [i/10.0 for i in xrange(10)]
splits = []
_len = float(len(vehjourn))
for i, (n, d, ln, lrn, dc) in enumerate(vehjourn):
    _start_time = time.time()

    counter += 1
    vj = Visum.Net.VehicleJourneys.ItemByKey(n)
    tp = vj.TimeProfile
    _vj = Visum.Net.AddVehicleJourney(counter, tp)
    _vj.SetAttValue("Dep", d)
    _vj.SetAttValue("FAKE", 1)

    splits.append(time.time() - _start_time)
    if (len(_PERCENTAGES) > 0) and ((i / _len) > _PERCENTAGES[0]):
        _p = _PERCENTAGES.pop(0)
        print "%.2f%%" % (100 * _p)
"""

