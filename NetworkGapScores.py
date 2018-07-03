import numpy
import VisumPy.helpers as h
import os
import csv
import scipy
import pandas as pd
import matplotlib.pyplot as plt

#look for version files in run folder
runDir = r"D:\BikePedTransit\RTPS\ServiceFrequencyBase"
TODs = ["AM", "MD", "PM", "NT"]

#append TOD to the file path
paths = []
for root, dirs, files in os.walk(runDir):
    for f in files: 
        if True in [TOD in f for TOD in TODs] and f.endswith(".ver"):
            paths.append(os.path.join(root, f))
            
print "pull data from model"
            
#create dictionaries to hold data
Transfers = {}
Journeys = {}
JourTime = {}
CarDist = {}
CarTime = {}
PrTvol = {}
PuTvol = {}
TrWait = {}
TOD_VolSums = {}
HWY_TOD_VolSums = {}

#open version files to gather data
for versionFilePath in paths:
    Visum.LoadVersion(versionFilePath)
    TOD = Visum.Net.AttValue("TOD")
    
    #get values from OD Pairs listing
    FromZone = h.GetMulti(Visum.Net.ODPairs,"FromZoneNo")
    ToZone = h.GetMulti(Visum.Net.ODPairs,"ToZoneNo")
    NumTransfers = h.GetMulti(Visum.Net.ODPairs,"MatValue(480 NTR)")
    JourneyTime = h.GetMulti(Visum.Net.ODPairs,"MatValue(490 JRT)")
    JourneyDist = h.GetMulti(Visum.Net.ODPairs,"MatValue(100003 JRD)")
    HwyTime = h.GetMulti(Visum.Net.ODPairs,"MatValue(290 TTC)") #tsys specific time interval in loaded network
    PrTDist = h.GetMulti(Visum.Net.ODPairs,"MatValue(270 DIS)")
    HwyVol = h.GetMulti(Visum.Net.ODPairs,"MatValue(2000 Highway)")
    TransitVol = h.GetMulti(Visum.Net.ODPairs,"MatValue(2200 ToTotal)")
    TransferWait = h.GetMulti(Visum.Net.ODPairs,"MatValue(100002 TWT)")
    TotalVol = h.GetMulti(Visum.Net.ODPairs,"MatValue(100004 TotalVol_AllModes)")
    
    #add to dictionary
    Transfers[TOD] = NumTransfers
    Journeys[TOD] = JourneyDist
    JourTime[TOD] = JourneyTime
    CarDist[TOD] = PrTDist
    CarTime[TOD] = HwyTime
    PrTvol[TOD] = HwyVol
    PuTvol[TOD] = TransitVol
    TrWait[TOD] = TransferWait
        
    #calculate total transit volume for time period
    TOD_Volume = h.GetMatrix(Visum, 2200)
    TOD_VolSum = TOD_Volume.sum()
    TOD_VolSums[TOD] = TOD_VolSum
    
    #calculate total highway volume for time period
    HWY_TOD_Volume = h.GetMatrix(Visum, 2000)
    HWY_TOD_VolSum = HWY_TOD_Volume.sum()
    HWY_TOD_VolSums[TOD] = HWY_TOD_VolSum
    
    
del NumTransfers
del JourneyTime 
del JourneyDist
del HwyTime 
del PrTDist 
del HwyVol
del TransitVol 
del TransferWait


#calculate sum of transit volume
TotTransitVol = 0

print "Transit"

for TOD in TOD_VolSums:
    print TOD, TOD_VolSums[TOD]
    TotTransitVol += TOD_VolSums[TOD]
#print to test
print TotTransitVol

#calculate sum of highway volume
TotHwyVol = 0

print "Highway"

for TOD in HWY_TOD_VolSums:
    print TOD, HWY_TOD_VolSums[TOD]
    TotHwyVol += HWY_TOD_VolSums[TOD]
#print to test
print TotHwyVol

print "calculate TOD weighted averages"

#calculate average and volume weighted averages for Number of Transfers
#create lists to hold counts, sums, and the average
NTR_counts = []
NTR_sums = []
NTR_w_sums = []
NTR_avg = []
NTR_w_avg = []

#iterate through all the items in each list of TOD NTR
for i in xrange(0, len(Transfers["AM"])):
    count = 0.0
    _sum = 0.0
    w_sum = 0.0
    #iterate through the TODs
    for key in Transfers:
        #see if there is a path between O and D (JRD <> 999999)
        #threshold is set lower because of effects of volume weighting in JRD skimming calculations
        if Journeys[key][i] < 166665:
            count += 1
            _sum += Transfers[key][i]
            w_sum += (Transfers[key][i] * TOD_VolSums[key])
    NTR_counts.append(count)
    NTR_sums.append(_sum)
    NTR_w_sums.append(w_sum)
    
#use the lists of counts and sums populated above to caululate the average across time periods
#cannot use 0 in place of a real value here to filter out because 0 is a legitimate value - use -1 isntead
for i in xrange(0, len(NTR_counts)):
    if NTR_counts[i] == 0:
        NTR_avg.append(-1)
    else:
        NTR_avg.append(NTR_sums[i]/NTR_counts[i])
        
for i in xrange(0, len(NTR_counts)):
    if NTR_counts[i] == 0:
        NTR_w_avg.append(-1)
    else:
        NTR_w_avg.append(NTR_w_sums[i]/TotTransitVol)
        
del NTR_counts
del NTR_sums
del NTR_w_sums
del NTR_avg

#repeat for Journey Distance
#create lists to hold counts, sums, and the average
JRD_counts = []
JRD_sums = []
JRD_w_sums = []
JRD_avg = []
JRD_w_avg = []

#iterate through all the items in each list of TOD JRD
for i in xrange(0, len(Journeys["AM"])):
    count = 0.0
    _sum = 0.0
    w_sum = 0.0  
    #iterate through the TODs
    for key in Journeys:
        #see if there is a path between O and D (JRD <> 999999)
        #threshold is set lower because of effects of volume weighting in JRD skimming calculations
        if Journeys[key][i] < 166665:
            count += 1
            _sum += Journeys[key][i]
            w_sum += (Journeys[key][i] * TOD_VolSums[key])     
    JRD_counts.append(count)
    JRD_sums.append(_sum)
    JRD_w_sums.append(w_sum)
    
#use the lists of counts and sums populated above to caululate the average across time periods
for i in xrange(0, len(JRD_counts)):
#if there are no paths for any time period, append 0 to the list, so its still the right length
#if there is only a path in a single time period, that path distance will be used as the average
    if JRD_counts[i] == 0:
        JRD_avg.append(0)
    else:
        JRD_avg.append(JRD_sums[i]/JRD_counts[i])
    
for i in xrange(0, len(JRD_counts)):
    if JRD_counts[i] == 0:
        JRD_w_avg.append(0)
    else:
        JRD_w_avg.append(JRD_w_sums[i]/TotTransitVol)
        
del JRD_counts
del JRD_sums
del JRD_w_sums
del JRD_avg

#repeat for Journey Time
#create lists to hold counts, sums, and the average
JRT_counts = []
JRT_sums = []
JRT_w_sums = []
JRT_avg = []
JRT_w_avg = []

#iterate through all the items in each list of TOD JRT
for i in xrange(0, len(JourTime["AM"])):
    count = 0.0
    _sum = 0.0
    w_sum = 0.0  
    #iterate through the TODs
    for key in JourTime:
        #see if there is a path between O and D (JRT <> 999999)
        #threshold is set lower because of effects of volume weighting in JRT skimming calculations
        if JourTime[key][i] < 166665:
            count += 1
            _sum += JourTime[key][i]
            w_sum += (JourTime[key][i] * TOD_VolSums[key])     
    JRT_counts.append(count)
    JRT_sums.append(_sum)
    JRT_w_sums.append(w_sum)
    
#use the lists of counts and sums populated above to caululate the average across time periods
for i in xrange(0, len(JRT_counts)):
#if there are no paths for any time period, append 0 to the list, so its still the right length
#if there is only a path in a single time period, that path distance will be used as the average
    if JRT_counts[i] == 0:
        JRT_avg.append(0)
    else:
        JRT_avg.append(JRT_sums[i]/JRT_counts[i])
    
for i in xrange(0, len(JRT_counts)):
    if JRT_counts[i] == 0:
        JRT_w_avg.append(0)
    else:
        JRT_w_avg.append(JRT_w_sums[i]/TotTransitVol)
        
del JRT_counts
del JRT_sums
del JRT_w_sums
del JRT_avg

#repeat for Highway Distance
#create lists to hold counts, sums, and the average
HWY_counts = []
HWY_sums = []
HWY_w_sums = []
HWY_avg = []
HWY_w_avg = []

#iterate through all the items in each list of TOD JRD
for i in xrange(0, len(CarDist["AM"])):
    count = 0.0
    _sum = 0.0
    w_sum = 0.0
    #iterate through the TODs
    for key in CarDist:
        count += 1
        _sum += CarDist[key][i]
        w_sum += (CarDist[key][i] * HWY_TOD_VolSums[key])
    HWY_counts.append(count)
    HWY_sums.append(_sum)
    HWY_w_sums.append(w_sum)
    
#use the lists of counts and sums populated above to caululate the average across time periods
for i in xrange(0, len(HWY_counts)):
    HWY_avg.append(HWY_sums[i]/HWY_counts[i])
    
for i in xrange(0, len(HWY_counts)):
    HWY_w_avg.append(HWY_w_sums[i]/TotHwyVol)
    
del HWY_counts
del HWY_sums
del HWY_w_sums
del HWY_avg

#repeat for Highway Time
#create lists to hold counts, sums, and the average
HwyT_counts = []
HwyT_sums = []
HwyT_w_sums = []
HwyT_avg = []
HwyT_w_avg = []

#iterate through all the items in each list of TOD JRD
for i in xrange(0, len(CarTime["AM"])):
    count = 0.0
    _sum = 0.0
    w_sum = 0.0
    #iterate through the TODs
    for key in CarTime:
        count += 1
        _sum += CarTime[key][i]
        w_sum += (CarTime[key][i] * HWY_TOD_VolSums[key])
    HwyT_counts.append(count)
    HwyT_sums.append(_sum)
    HwyT_w_sums.append(w_sum)
    
#use the lists of counts and sums populated above to caululate the average across time periods
for i in xrange(0, len(HwyT_counts)):
    HwyT_avg.append(HwyT_sums[i]/HwyT_counts[i])
    
for i in xrange(0, len(HwyT_counts)):
    HwyT_w_avg.append(HwyT_w_sums[i]/TotHwyVol)
    
del HwyT_counts
del HwyT_sums
del HwyT_w_sums
del HwyT_avg

#repeat for Transfer Wait Times
#create lists to hold counts, sums, and the average

#Transfer Wait Time
TWT_counts = []
TWT_sums = []
TWT_w_sums = []
TWT_avg = []
TWT_w_avg = []

#iterate through all the items in each list of TOD JRD
for i in xrange(0, len(TrWait["AM"])):
    count = 0.0
    _sum = 0.0
    w_sum = 0.0
    #iterate through the TODs
    for key in TrWait:
        if TrWait[key][i] < 166665:
            count += 1
            _sum += TrWait[key][i]
            w_sum += (TrWait[key][i] * TOD_VolSums[key])
    TWT_counts.append(count)
    TWT_sums.append(_sum)
    TWT_w_sums.append(w_sum)
    
#use the lists of counts and sums populated above to caululate the average across time periods
for i in xrange(0, len(TWT_counts)):
    if TWT_counts[i] == 0:
        #leave open possiblity of 0 wait time; filter out later
        TWT_avg.append(-1)
    else:
        TWT_avg.append(TWT_sums[i]/TWT_counts[i])
    
for i in xrange(0, len(TWT_counts)):
    if TWT_counts[i] == 0:
        #leave open possiblity of 0 wait time; filter out later
        TWT_w_avg.append(-1)
    else:
        TWT_w_avg.append(TWT_w_sums[i]/TotTransitVol)
        
del TWT_counts
del TWT_sums
del TWT_w_sums
del TWT_avg

#Add TOD  volumes for Highway and Transit
#create lists to hold sums
#no need to weight by volume because it's already volumes
HWYvol_sums = []

#iterate through all the items in each list of TOD volumes
for i in xrange(0, len(PrTvol["AM"])):
    _sum = 0.0
    #iterate through the TODs
    for key in PrTvol:
        _sum += PrTvol[key][i]
    HWYvol_sums.append(_sum)

#Transit
PuTvol_sums = []

#iterate through all the items in each list of TOD volumes
for i in xrange(0, len(PuTvol["AM"])):
    _sum = 0.0
    #iterate through the TODs
    for key in PuTvol:
        _sum += PuTvol[key][i]
    PuTvol_sums.append(_sum)    
    
#combine for total volumes
TotVol = []
for i in xrange(0, len(HWYvol_sums)):
    TotVol.append(HWYvol_sums[i] + PuTvol_sums[i])
    
del HWYvol_sums
del PuTvol_sums


print "transfer and directness criteria"

#flag OD pairs with 1 or more transfer required
TransferFlag = []
for i in xrange(0, len(NTR_w_avg)):
    if NTR_w_avg[i] >= 1:
        TransferFlag.append(1)
    else:
        TransferFlag.append(0)
        
#flag OD pairs where transit distance is greater than highway distance
DistanceFlag = []
for i in xrange(0, len(JRD_w_avg)):
    if JRD_w_avg[i] > HWY_w_avg[i]:
        DistanceFlag.append(1) #yes
    else:
        DistanceFlag.append(0) #no
        
#flag OD pairs where transit time is greater than highway time
TimeFlag = []
for i in xrange(0, len(JRT_w_avg)):
    if JRT_w_avg[i] > HwyT_w_avg[i]:
        TimeFlag.append(1) #yes
    else:
        TimeFlag.append(0) #no
        
#criteria for points for number of transfers
TransferPoint = []
for i in xrange(0, len(NTR_w_avg)):
    if NTR_w_avg[i] < 1:
        TransferPoint.append(0)
    elif (NTR_w_avg[i] >=1 and NTR_w_avg[i] < 2.5):
        TransferPoint.append(1)
    else:
        TransferPoint.append(2)
        
#check
print len(DistanceFlag)
print len(TimeFlag)
print TransferPoint[200]
print TransferPoint[300]
print NTR_w_avg[200]
print NTR_w_avg[300]

#criteria for TWT point
TWTPoint = []
for i in xrange(0, len(TWT_w_avg)):
    if (TransferPoint[i] == 1 and TWT_w_avg >= 10):
        TWTPoint.append(1)
    elif (TransferPoint[i] == 1 and TWT_w_avg < 10):
        TWTPoint.append(0)
    elif (TransferPoint[i] == 2 and TWT_w_avg >= 20):
        TWTPoint.append(1)
    elif (TransferPoint[i] == 2 and TWT_w_avg < 20):
        TWTPoint.append(0)
    else: #Transfer < 1
        TWTPoint.append(0)   
        
#sum points for connection score
ConnectionScore = []
for i in xrange(0, len(FromZone)):
    x = DistanceFlag[i] + TimeFlag[i] + TransferPoint[i] + TWTPoint[i]
    ConnectionScore.append(x)
    
print "creating data frame"
    
#create dataframe from these lists
df = pd.DataFrame(
    {'FromZone': FromZone,
     'ToZone':   ToZone,
     'NumTransfers': NTR_w_avg,
     'TrWait': TWT_w_avg,
     'AvgDist_Hwy': HWY_w_avg,
     'AvgDist_JRD': JRD_w_avg,
     'AvgTime_JRT': JRT_w_avg,
     'AvgTime_CAR': HwyT_w_avg,
     'DistanceFlag': DistanceFlag,
     'TimeFlag': TimeFlag,
     'TransferPoint': TransferPoint,
     'TWTPoint': TWTPoint,
     'ConnectionScore': ConnectionScore
    })
    
    
#filter for zones only within the region
FromRegion = df['FromZone'] < 50000
ToRegion = df['ToZone'] < 50000

#create DF with just Region Zones
RegionDF = pd.DataFrame(df[FromRegion & ToRegion])

#filter for OD pairs with a valid connection or no connection
ValidConnectionT = RegionDF['NumTransfers'] >= 0
ValidConnectionJ = RegionDF['AvgDist_JRD'] > 0
NoConnection = RegionDF['AvgDist_JRD'] == 0

ValidRegionDF = pd.DataFrame(RegionDF[ValidConnectionT & ValidConnectionJ])

NoConRegionDF = pd.DataFrame(RegionDF[NoConnection])

print "assigning no connection score"

#overwrite connection score in no connection table
NoConRegionDF['ConnectionScore'] = 6


#update the parent DF with the child df (Nope) and test to make sure it updated
RegionDF.update(NoConRegionDF)

#test
#a = RegionDF['FromZone'] == 1 
#b = RegionDF['ToZone'] == 1810
#test = RegionDF[a&b]
#test.head()

#del test
del NoConRegionDF

#update full DF to then select columns to add back into Visum as UDAs
df.update(RegionDF)

#test
#a = df['FromZone'] == 1 
#b = df['ToZone'] == 1810
#test = df[a&b]
#test.head()

#del test

#delete weighted averages
del TWT_w_avg
del NTR_w_avg
del HwyT_w_avg
del JRD_w_avg
del HWY_w_avg
del JRT_w_avg

#delete dictionaries
del Transfers
del Journeys
del JourTime
del CarDist
del CarTime
del PrTvol
del PuTvol
del TrWait

SubRegionDF = RegionDF.loc[:,['FromZone', 'ToZone', 'NumTransfers', 'TrWait', 'DistanceFlag', 'TimeFlag', 'TransferPoint', 'TWTPoint','ConnectionScore']]

print "importing connection score table"

#create Connection Score table in postgres
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:sergt@localhost:5432/RTPS')
SubRegionDF.to_sql('ConnectionScore2', engine, chunksize = 10000)

###DEMAND SCORE###
print "gathering demand data"

Vols = {}

for versionFilePath in paths[0:4]:
    Visum.LoadVersion(versionFilePath)
    TOD = Visum.Net.AttValue("TOD")
    
    #get values from OD Pairs listing
    TotalVol = h.GetMulti(Visum.Net.ODPairs,"MatValue(100004 TotalVol)")
    #add to dictionary
    Vols[TOD] = TotalVol
    
FromZone = h.GetMulti(Visum.Net.ODPairs,"FromZoneNo")
ToZone = h.GetMulti(Visum.Net.ODPairs,"ToZoneNo")
DirectDist = h.GetMulti(Visum.Net.ODPairs,"DirectDist")
FromAType = h.GetMulti(Visum.Net.ODPairs,"FromZone\Area_Type")
ToAType = h.GetMulti(Visum.Net.ODPairs,"ToZone\Area_Type")

DailyVols = []

#iterate through all the items in each list of TOD volumes
for i in xrange(0, len(Vols["AM"])):
    _sum = 0.0
    #iterate through the TODs
    for key in Vols:
        _sum += Vols[key][i]
    DailyVols.append(_sum)
    
#create OD demand (total volume) data frame
Demand = pd.DataFrame(
    {'FromZone': FromZone,
     'ToZone':   ToZone,
     'DailyVols': DailyVols,
     'Dist': DirectDist})

FromRegion = Demand['FromZone'] < 50000
ToRegion = Demand['ToZone'] < 50000

RegionDemand = pd.DataFrame(Demand[FromRegion & ToRegion])

print "importing demand score table"

from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:sergt@localhost:5432/RTPS')
RegionDemand.to_sql('DemandScore2', engine, chunksize = 10000)

import psycopg2 as psql # PostgreSQL connector
#connect to SQL DB in python
con = psql.connect(dbname = "RTPS", host = "localhost", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()

print "calculating demand score"

#2 bins for pairs with demand (above and below 5 - half of mean)
Q_AddCol = """
    ALTER TABLE public."DemandScore2"
    ADD COLUMN "DemScore" integer;"""
cur.execute(Q_AddCol)
con.commit()

#no demand
Q_SetDemandScore_0 = """
    UPDATE public."DemandScore2"
    SET "DemScore" = 0
    WHERE public."DemandScore2"."DailyVols" < 1 ; 
    """
cur.execute(Q_SetDemandScore_0)

#below mean demand
Q_SetDemandScore_1 = """
    UPDATE public."DemandScore2"
    SET "DemScore" = 1
    WHERE public."DemandScore2"."DailyVols" <= 5
    AND public."DemandScore2"."DailyVols" >= 1; 
    """
cur.execute(Q_SetDemandScore_1)

#above mean demand
Q_SetDemandScore_rest = """
    UPDATE public."DemandScore2"
    SET "DemScore" = 2
    WHERE public."DemandScore2"."DailyVols" <= 7240
    AND public."DemandScore2"."DailyVols" > 5; 
    """
cur.execute(Q_SetDemandScore_rest)
con.commit()

