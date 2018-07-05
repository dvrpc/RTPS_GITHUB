import numpy
import VisumPy.helpers as h
import os
import csv
import scipy
import pandas as pd
import matplotlib.pyplot as plt

#look for version files in run folder
runDir = r"D:\BikePedTransit\RTPS\ServiceFrequencyBase"
#AM and PM only to match with travel time index definition
#more like TTI, not PRI
TODs = ["AM", "PM"]

#append TOD to the file path
paths = []
for root, dirs, files in os.walk(runDir):
    for f in files: 
        if True in [TOD in f for TOD in TODs] and f.endswith(".ver"):
            paths.append(os.path.join(root, f))
            
            
#create dictionaries to hold data
v0 = {}
vCur = {}
HWY_TOD_VolSums = {}

Visum = h.CreateVisum(15)
#open version files to gather data
for versionFilePath in paths:
    Visum.LoadVersion(versionFilePath)
    TOD = Visum.Net.AttValue("TOD")
    
    #get values from link listing
    v0_list = h.GetMulti(Visum.Net.Links,"V0_PrTSys(Car)")
    vcur_list = h.GetMulti(Visum.Net.Links,"VCur_PrTSys(Car)")
   
    #add to dictionary
    v0[TOD] = v0_list
    vCur[TOD] = vcur_list

    #calculate total highway volume for time period
    HWY_TOD_Volume = h.GetMatrix(Visum, 2000)
    HWY_TOD_VolSum = HWY_TOD_Volume.sum()
    HWY_TOD_VolSums[TOD] = HWY_TOD_VolSum
   
no = h.GetMulti(Visum.Net.Links,"No")
fromnodeno = h.GetMulti(Visum.Net.Links,"FromNodeNo")
tonodeno = h.GetMulti(Visum.Net.Links,"ToNodeNo")
    
TotHwyVol = 0
for TOD in HWY_TOD_VolSums:
    print TOD, HWY_TOD_VolSums[TOD]
    TotHwyVol += HWY_TOD_VolSums[TOD]
#print to test
print TotHwyVol
    
    
print "calculate TOD weighted averages"

#calculate average and volume weighted averages for v0
#create lists to hold counts, sums, and the average
v0_counts = []
v0_sums = []
v0_w_sums = []
v0_avg = []
v0_w_avg = []

#iterate through all the items in each list of TOD v0
for i in xrange(0, len(v0["AM"])):
    count = 0.0
    _sum = 0.0
    w_sum = 0.0
    #iterate through the TODs
    for key in v0:
        count += 1
        _sum += v0[key][i]
        w_sum += (v0[key][i] * HWY_TOD_VolSums[key])
    v0_counts.append(count)
    v0_sums.append(_sum)
    v0_w_sums.append(w_sum)
    
#use the lists of counts and sums populated above to caululate the average across time periods
#cannot use 0 in place of a real value here to filter out because 0 is a legitimate value - use -1 isntead
for i in xrange(0, len(v0_counts)):
    if v0_counts[i] == 0:
        v0_avg.append(-1)
    else:
        v0_avg.append(v0_sums[i]/v0_counts[i])
        
for i in xrange(0, len(v0_counts)):
    if v0_counts[i] == 0:
        v0_w_avg.append(-1)
    else:
        v0_w_avg.append(v0_w_sums[i]/TotHwyVol)
        
del v0_counts
del v0_sums
del v0_w_sums
del v0_avg


#repeat for vCur
vcur_counts = []
vcur_sums = []
vcur_w_sums = []
vcur_avg = []
vcur_w_avg = []

#iterate through all the items in each list of TOD vcur
for i in xrange(0, len(vCur["AM"])):
    count = 0.0
    _sum = 0.0
    w_sum = 0.0
    #iterate through the TODs
    for key in vCur:
        count += 1
        _sum += vCur[key][i]
        w_sum += (vCur[key][i] * HWY_TOD_VolSums[key])
    vcur_counts.append(count)
    vcur_sums.append(_sum)
    vcur_w_sums.append(w_sum)
    
#use the lists of counts and sums populated above to caululate the average across time periods
#cannot use 0 in place of a real value here to filter out because 0 is a legitimate value - use -1 isntead
for i in xrange(0, len(vcur_counts)):
    if vcur_counts[i] == 0:
        vcur_avg.append(-1)
    else:
        vcur_avg.append(vcur_sums[i]/vcur_counts[i])
        
for i in xrange(0, len(vcur_counts)):
    if vcur_counts[i] == 0:
        vcur_w_avg.append(-1)
    else:
        vcur_w_avg.append(vcur_w_sums[i]/TotHwyVol)
        
del vcur_counts
del vcur_sums
del vcur_w_sums
del vcur_avg

#calculate travel time proxy ratio
TTratio = []
for i in xrange(0, len(v0_w_avg)):
    if vcur_w_avg[i] > 0:
        r = v0_w_avg[i]/vcur_w_avg[i]
        TTratio.append(r)
    else:
        TTratio.append(-1)
        
#create data frame
df = pd.DataFrame(
    {'no': no,
     'fromnodeno':   fromnodeno,
     'tonodeno': tonodeno,
     'v0_w_avg': v0_w_avg,
     'vcur_w_avg': vcur_w_avg,
     'TTratio': TTratio
    })

#add to sql db
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:sergt@localhost:5432/Reliability')
df.to_sql('Model_TTI', engine, chunksize = 10000)

import psycopg2 as psql # PostgreSQL connector
#connect to SQL DB in python
con = psql.connect(dbname = "Reliability", host = "localhost", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()

#join to previously imported links table with geoms (thru postgis)
#create indices in pgadmin
Q_jointolines = """
    CREATE TABLE tti_linkjoin AS(
        SELECT
            l.*,
            m.v0_w_avg,
            m.vcur_w_avg,
            m."TTratio" AS ttratio
        FROM all_model_link l
        INNER JOIN "Model_TTI" m
        ON m.fromnodeno = l.fromnodeno
        AND m.tonodeno = l.tonodeno)
    ;"""
cur.execute(Q_jointolines)
con.commit()

#export from pgadmin to shapefile thru qgis