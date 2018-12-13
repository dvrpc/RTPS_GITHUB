
# coding: utf-8

# In[1]:


import VisumPy.helpers as h
import numpy
import os
import csv

Visum = h.CreateVisum(15)
#drag in base version file

#create vehicle journey items table to export to CSV and read into Postgres

LineName = h.GetMulti(Visum.Net.VehicleJourneyItems, "VehJourney\LineName", True)
LineRoute = h.GetMulti(Visum.Net.VehicleJourneyItems, "VehJourney\LineRouteName", True)
Direction = h.GetMulti(Visum.Net.VehicleJourneyItems, "VehJourney\DirectionCode", True)
Index = h.GetMulti(Visum.Net.VehicleJourneyItems, "TimeProfileItem\LineRouteItem\Index", True)

VehJourNo = h.GetMulti(Visum.Net.VehicleJourneyItems, "VehJourneyNo", True)
PrevDep = h.GetMulti(Visum.Net.VehicleJourneyItems, "PreviousVehJourneyItem\Dep", True)
Arrival = h.GetMulti(Visum.Net.VehicleJourneyItems, "Arr", True)
PreLength = h.GetMulti(Visum.Net.VehicleJourneyItems, "PreLength", True)

PrevLRIndex = h.GetMulti(Visum.Net.VehicleJourneyItems, "PreviousVehJourneyItem\TimeProfileItem\LineRouteItem\Index", True)
CurLRIndex = h.GetMulti(Visum.Net.VehicleJourneyItems, "TimeProfileItem\LineRouteItem\Index", True)

PrevStop    = h.GetMulti(Visum.Net.VehicleJourneyItems, "PreviousVehJourneyItem\TimeProfileItem\LineRouteItem\StopPointNo", True)
CurrentStop = h.GetMulti(Visum.Net.VehicleJourneyItems, "TimeProfileItem\LineRouteItem\StopPointNo", True)

for i in xrange(0, len(PrevStop)):
    if PrevStop[i] != None:
        PrevStop[i] = int(PrevStop[i])
for i in xrange(0, len(CurrentStop)):
    if CurrentStop[i] != None:
        CurrentStop[i] = int(CurrentStop[i])

for i in xrange(0, len(VehJourNo)):
    if VehJourNo[i] != None:
        VehJourNo[i] = int(VehJourNo[i])

for i in xrange(0, len(Index)):
    if Index[i] != None:
        Index[i] = int(Index[i])
for i in xrange(0, len(PrevLRIndex)):
    if PrevLRIndex[i] != None:
        PrevLRIndex[i] = int(PrevLRIndex[i])
for i in xrange(0, len(CurLRIndex)):
    if CurLRIndex[i] != None:
        CurLRIndex[i] = int(CurLRIndex[i])

Arrival[1]

tblA = numpy.column_stack((LineName, LineRoute, Direction, VehJourNo, Index, PrevDep, Arrival, PreLength, PrevLRIndex, CurLRIndex, PrevStop, CurrentStop))

tblA[0:5]

with open(r'D:\BikePedTransit\RTPS\ScheduledSpeed\VehJourneyItems.csv', 'wb') as IO:
    w = csv.writer(IO)
    w.writerow(['LineName', 'LineRoute', 'Direction', 'VehJourNo', 'Index', 'PrevDep', 'Arrival', 'PreLength', 'PrevLRIndex', 'CurLRIndex', 'PrevStop', 'CurrentStop'])
    for row in tblA:
        w.writerow(row)

del LineName, LineRoute, Direction, VehJourNo, Index, PrevDep, Arrival, PreLength, PrevLRIndex, CurLRIndex, PrevStop, CurrentStop

#create stop point table to get geometries to read into PostGIS

StopPointNum = h.GetMulti(Visum.Net.StopPoints, "No", True)

for i in xrange(0, len(StopPointNum)):
    if StopPointNum[i] != None:
        StopPointNum[i] = int(StopPointNum[i])

StopName = h.GetMulti(Visum.Net.StopPoints, "Name", True)

StopType = h.GetMulti(Visum.Net.StopPoints, "TypeNo", True)

StopTSys = h.GetMulti(Visum.Net.StopPoints, "TSysSet", True)

StopGeom = h.GetMulti(Visum.Net.StopPoints, "WKTLoc", True)

StopName[0]

tblB = numpy.column_stack((StopPointNum, StopName, StopType, StopTSys, StopGeom))

with open(r'D:\BikePedTransit\RTPS\ScheduledSpeed\StopPointLocations.csv', 'wb') as IO:
    w = csv.writer(IO)
    w.writerow(['StopPointNum', 'StopName', 'StopType', 'StopTSys', 'StopGeom'])
    for row in tblB:
        w.writerow(row)

del StopPointNum, StopName, StopType, StopTSys, StopGeom

#create line route item table to get links between stop points (index) to read into PostGIS

LRIndex = h.GetMulti(Visum.Net.LineRouteItems, "Index", True)
for i in xrange(0, len(LRIndex)):
    if LRIndex[i] != None:
        LRIndex[i] = int(LRIndex[i])
LRIname = h.GetMulti(Visum.Net.LineRouteItems, "LineName", True)
LRIroute = h.GetMulti(Visum.Net.LineRouteItems, "LineRouteName", True)
LRIdirection = h.GetMulti(Visum.Net.LineRouteItems, "DirectionCode", True)
LRIstop = h.GetMulti(Visum.Net.LineRouteItems, "StopPointNo", True)
for i in xrange(0, len(LRIstop)):
    if LRIstop[i] != None:
        LRIstop[i] = int(LRIstop[i])
LRInode = h.GetMulti(Visum.Net.LineRouteItems, "NodeNo", True)
for i in xrange(0, len(LRInode)):
    if LRInode[i] != None:
        LRInode[i] = int(LRInode[i])


tblC = numpy.column_stack((LRIndex, LRIname, LRIroute, LRIdirection, LRIstop, LRInode))


tblC[0:5]



with open(r'D:\BikePedTransit\RTPS\ScheduledSpeed\LineRouteItems.csv', 'wb') as IO:
    w = csv.writer(IO)
    w.writerow(['LRIndex', 'LRIname', 'LRIdirection', 'LRIstop', 'LRInode'])
    for row in tblC:
        w.writerow(row)

del LRIndex, LRIname, LRIroute, LRIdirection, LRIstop, LRInode


#export links to shapefile and import into DB

#followed by CalcAvgSpeedBetweenStopPointsByLine.sql

