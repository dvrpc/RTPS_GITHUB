import argparse
import csv
import sqlite3

def _CastVar(v, dtype, nulls = True):
    v = v.strip(" ")
    if nulls and (v == ""):
        return True
    try:
        _ = dtype(v)
        return True
    except:
        return False

def ParseHeader(NET_tbl_header):
    optPrefix = {
        '-': "_del",
        '+': "_add",
        '*': "_mod",
        '#': "_att",
    }
    _ = NET_tbl_header[0].replace("$","").split(":")
    if len(_) <> 2:
        return None, None
    tableName, firstField = _
    if tableName[0] in optPrefix:
        tableName = tableName[1:] + optPrefix[tableName[0]]
    tableFields = [firstField] + NET_tbl_header[1:]
    return tableName, tableFields

def CreateTable(con, tblName, tblFields, tblDTypes = None):
    if tblDTypes is None:
        tblDTypes = ["TEXT" for _ in tblFields]
    cur = con.cursor()
    fieldDTypes = ",".join(map(lambda (f,d):"`%s` %s" % (f,d), zip(tblFields, tblDTypes)))
    cur.execute("CREATE TABLE %s (%s)" % (tblName, fieldDTypes))

def DictAdd(d1, d2):
    ks = set(d1.keys() + d2.keys())
    d = dict([(k, 0.0) for k in ks])
    d.update(map(lambda k:(k, d[k] + d1[k]), ks.intersection(d1.keys())))
    d.update(map(lambda k:(k, d[k] + d2[k]), ks.intersection(d2.keys())))
    return d

def AddData(con, tblName, data):
    cur = con.cursor()
    cur.execute("INSERT INTO %s VALUES (%s)" % (tblName, ",".join(["?" for i in xrange(len(data))])), data)

def GuessDTypes(path):
    j = 0
    tblNames = {}
    tblName_DTypes = {}
    tblName = None
    tblFields = None
    _dtype_template = {
        "INTEGER": lambda v:1 if _CastVar(v, int) else 0,
        "REAL": lambda v:1 if _CastVar(v, float) else 0,
        "TEXT": lambda v:1,
    }
    _dtype_priority = [
        "INTEGER",
        "REAL",
        "TEXT"
    ]
    _tbl_dtype = dict([(_dtype, 0) for _dtype in _dtype_priority])
    for i, row in enumerate(_RoughlyReadInNET(path)):
        if (len(row) > 0) and (not row[0].startswith("*")):
            if row[0].startswith("$"):
                if tblName is not None:
                    tblNames[tblName] = SelTblNameDTypes(tblName_DTypes, _dtype_priority)
                tblName, tblFields = ParseHeader(row)
                if tblName is not None:
                    tblName_DTypes = {}
                    for k, field in enumerate(tblFields):
                        tblName_DTypes[k] = {
                            "NAME": field,
                            "DTypes": dict(_tbl_dtype),
                            "DTYPE": "TEXT",
                            "LEN": 0,
                        }
                    j = 0
            else:
                if tblName is not None:
                    for k, v in enumerate(row):
                        if (j + 5) % 5000 == 0:
                            _trgt = max(tblName_DTypes[k]["DTypes"].values())
                            tblName_DTypes[k]["DTypes"] = dict(filter(lambda (k,v):v == _trgt, tblName_DTypes[k]["DTypes"].iteritems()))
                        _dtype_guesses = dict(map(lambda k:(k, _dtype_template[k](v)), tblName_DTypes[k]["DTypes"].keys()))
                        tblName_DTypes[k]["DTypes"] = DictAdd(tblName_DTypes[k]["DTypes"], _dtype_guesses)
                        tblName_DTypes[k]["LEN"] = len(v) if tblName_DTypes[k]["LEN"] < len(v) else tblName_DTypes[k]["LEN"]
                    j += 1
    tblNames[tblName] = SelTblNameDTypes(tblName_DTypes, _dtype_priority)

    return tblNames

def SelTblNameDTypes(tblName_DTypes, _dtype_priority):
    for _i in tblName_DTypes:
        _trgt = max(tblName_DTypes[_i]["DTypes"].values())
        for _dtype in _dtype_priority:
            if _dtype in tblName_DTypes[_i]["DTypes"] and _trgt == tblName_DTypes[_i]["DTypes"][_dtype]:
                tblName_DTypes[_i]["DTYPE"] = _dtype
                break
        # Could warn
        if tblName_DTypes[_i]["LEN"] > int(2**31-1):
            print "%s is stupidly long..." % tblName_DTypes[_i]["NAME"]
            tblName_DTypes[_i]["DTYPE"] = "BLOB"
    return [(tblName_DTypes[_i]["NAME"], tblName_DTypes[_i]["DTYPE"]) for _i in xrange(len(tblName_DTypes))]

def CreateIndex(con, tblName, tblFields):
    cur = con.cursor()
    idx_fields = filter(lambda f:f.endswith("NO"), tblFields)
    if len(idx_fields) > 0:
        cur.execute("CREATE INDEX `%s_IDX` ON `%s` (%s)" % (tblName, tblName, ",".join(map(lambda f:'`'+f+'`', idx_fields))))

def _RoughlyReadInNET(path):
    with open(path, "rb") as io:
        r = csv.reader(io, delimiter = ";")
        for row in r:
            yield row

def ProcessNET(path, con, guessDType = False, genIndex = False):
    tblNameFieldsDTypes = {}
    tblName = None
    tblFields = None
    tblDTypes = None
    if guessDType:
        tblNameFieldsDTypes = GuessDTypes(path)
    for row in _RoughlyReadInNET(path):
        if (len(row) > 0) and (not row[0].startswith("*")):
            if row[0].startswith("$"):
                tblName, tblFields = ParseHeader(row)
                if tblName is not None:
                    if tblName in tblNameFieldsDTypes:
                        tblFields, tblDTypes = zip(*tblNameFieldsDTypes[tblName])
                    else:
                        tblDTypes = None
                    CreateTable(con, tblName, tblFields, tblDTypes)
                    if genIndex:
                        CreateIndex(con, tblName, tblFields)
            else:
                if tblName is not None:
                    AddData(con, tblName, row)
    con.commit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Poopghost.')
    parser.add_argument("-i", "--input", required = True, help="Input path of Visum .NET file")
    parser.add_argument("-o", "--output", required = True, help="Output path to sqlite database")
    parser.add_argument("-g", "--guess", action = 'store_true', default = False, help="Guess field data types")
    parser.add_argument("-x", "--index", action = 'store_true', default = False, help="Auto-generate indicies")
    args = parser.parse_args()
    ProcessNET(args.input, sqlite3.connect(args.output), args.guess, args.index)
