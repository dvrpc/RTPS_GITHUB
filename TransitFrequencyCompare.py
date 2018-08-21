import numpy
import VisumPy.helpers as h
import os
import csv
import pandas as pd

Visum = h.CreateVisum(15)

#point to time period result versionfiles in run directory
runDir = r"D:\BikePedTransit\RTPS\ServiceFrequencyBase_TIM231"
TODs = ["AM", "MD", "PM", "NT"]
paths = []
for root, dirs, files in os.walk(runDir):
    for f in files: 
        if True in [TOD in f for TOD in TODs] and f.endswith(".ver"):
            paths.append(os.path.join(root, f))

#create dictionaries to hold values
OZonalPuT = {}
DZonalPuT = {}
OZonalCar = {}
DZonalCar = {}

PassBoard  = {}
PassAlight = {}
PassOrigin = {}
PassDest   = {}
PassTransTot = {}

LinePassBoard = {}
LinePassAlight = {}

#gather attributes from version files
for versionFilePath in paths:
    #open time peiod version file and collect TOD attribute for labeling purposes
    Visum.LoadVersion(versionFilePath)
    TOD = Visum.Net.AttValue("TOD")
    print TOD
    
    #collect attributes from time period version files
    ZoneNo = h.GetMulti(Visum.Net.Zones, "No")
    #convert to integer (as necessary)
    for i in xrange(0, len(ZoneNo)):
        if ZoneNo[i] != None:
            ZoneNo[i] = int(ZoneNo[i])
    OZonalPuT[TOD] = h.GetMulti(Visum.Net.Zones, "MatRowSum(2200TrTotal)")
    DZonalPuT[TOD] = h.GetMulti(Visum.Net.Zones, "MatColSum(2200TrTotal)")
    OZonalCar[TOD] = h.GetMulti(Visum.Net.Zones, "MatRowSum(2000Highway)")
    DZonalCar[TOD] = h.GetMulti(Visum.Net.Zones, "MatColSum(2000Highway)")
    
    #gather stop point attributes
    StopNo = h.GetMulti(Visum.Net.StopPoints, "No")
    for i in xrange(0, len(StopNo)):
        if StopNo[i] != None:
            StopNo[i] = int(StopNo[i])
    StopCode = h.GetMulti(Visum.Net.StopPoints, "Code")
    StopName = h.GetMulti(Visum.Net.StopPoints, "Name")
    NodeNo = h.GetMulti(Visum.Net.StopPoints, "NodeNo")
    for i in xrange(0, len(NodeNo)):
        if NodeNo[i] != None:
            NodeNo[i] = int(NodeNo[i])
    PassBoard[TOD] = h.GetMulti(Visum.Net.StopPoints, "PassBoard(AP)")
    PassAlight[TOD] = h.GetMulti(Visum.Net.StopPoints, "PassAlight(AP)")
    PassOrigin[TOD] = h.GetMulti(Visum.Net.StopPoints, "PassOrigin(AP)")
    PassDest[TOD] = h.GetMulti(Visum.Net.StopPoints, "PassDestination(AP)")
    PassTransTot[TOD] = h.GetMulti(Visum.Net.StopPoints, "PassTransTotal(AP)")
    #PassThruStop = h.GetMulti(Visum.Net.StopPoints, "PassThroughStop(AP)")
    #PassThruNoStop = h.GetMulti(Visum.Net.StopPoints, "PassThroughNoStop(AP)")
    
    #gather line attributes
    LineName = h.GetMulti(Visum.Net.Lines, "Name")
    TSysCode = h.GetMulti(Visum.Net.Lines, "TSysCode")
    LinePassBoard[TOD] =  h.GetMulti(Visum.Net.Lines, "Sum:LineRoutes\Sum:LineRouteItems\PassBoard(AP)")
    LinePassAlight[TOD] = h.GetMulti(Visum.Net.Lines, "Sum:LineRoutes\Sum:LineRouteItems\PassAlight(AP)")


#create lists of zeros to add to
sum_OZonalPuT = [0] * len(ZoneNo)
sum_DZonalPuT = [0] * len(ZoneNo)
sum_OZonalCar = [0] * len(ZoneNo)
sum_DZonalCar = [0] * len(ZoneNo)

sum_PassBoard    = [0] * len(StopNo)
sum_PassAlight   = [0] * len(StopNo)
sum_PassOrigin   = [0] * len(StopNo)
sum_PassDest     = [0] * len(StopNo)
sum_PassTransTot = [0] * len(StopNo)

sum_LinePassBoard  = [0] * len(LineName)
sum_LinePassAlight = [0] * len(LineName)

#sum to get daily total
for key in OZonalPuT:
    for i in xrange(0,len(sum_OZonalPuT)):
        sum_OZonalPuT[i] += OZonalPuT[key][i]
for key in DZonalPuT:
    for i in xrange(0,len(sum_DZonalPuT)):
        sum_DZonalPuT[i] += DZonalPuT[key][i]
for key in OZonalCar:
    for i in xrange(0,len(sum_OZonalCar)):
        sum_OZonalCar[i] += OZonalCar[key][i]
for key in DZonalCar:
    for i in xrange(0,len(sum_DZonalCar)):
        sum_DZonalCar[i] += DZonalCar[key][i]
    
for key in PassBoard:
    for i in xrange(0,len(sum_PassBoard)):
        sum_PassBoard[i] += PassBoard[key][i]
for key in PassAlight:
    for i in xrange(0,len(sum_PassAlight)):
        sum_PassAlight[i] += PassAlight[key][i]
for key in PassOrigin:
    for i in xrange(0,len(sum_PassOrigin)):
        sum_PassOrigin[i] += PassOrigin[key][i]
for key in PassDest:
    for i in xrange(0,len(sum_PassDest)):
        sum_PassDest[i] += PassDest[key][i]
for key in PassTransTot:
    for i in xrange(0,len(sum_PassTransTot)):
        sum_PassTransTot[i] += PassTransTot[key][i]
    
for key in LinePassBoard:
    for i in xrange(0,len(sum_LinePassBoard)):
        sum_LinePassBoard[i] += LinePassBoard[key][i]
for key in LinePassAlight:
    for i in xrange(0,len(sum_LinePassAlight)):
        sum_LinePassAlight[i] += LinePassAlight[key][i]
    
    
#combine into dataframes
zones_df = pd.DataFrame(
    {'ZoneNo': ZoneNo,
     'Base_OZonalPuT': sum_OZonalPuT,
     'Base_DZonalPuT': sum_DZonalPuT,
     'Base_OZonalCar': sum_OZonalCar,
     'Base_DZonalCar': sum_DZonalCar
    })
    
stops_df = pd.DataFrame(
    {'StopNo'   : StopNo ,
     'StopCode' : StopCode ,
     'StopName' : StopName ,
     'NodeNo'   : NodeNo ,
     'Base_PassBoard'    : sum_PassBoard,
     'Base_PassAlight'   : sum_PassAlight,
     'Base_PassOrigin'   : sum_PassOrigin,
     'Base_PassDest'     : sum_PassDest,
     'Base_PassTransTot' : sum_PassTransTot
    })
    
lines_df = pd.DataFrame(
    {'LineName'   : LineName ,
     'TSysCode' : TSysCode,
     'Base_LinePassBoard' : sum_LinePassBoard,
     'Base_LinePassAlight' : sum_LinePassAlight
    })
    
    
###REPEAT FOR 2X FREQUENCY
##WILL OVERWRITE EXISTING LISTS/DICTS/ATTRIBUTES
#point to time period result versionfiles in run directory
runDir = r"D:\BikePedTransit\RTPS\2xSEPTA_TIM231"
TODs = ["AM", "MD", "PM", "NT"]
paths = []
for root, dirs, files in os.walk(runDir):
    for f in files: 
        if True in [TOD in f for TOD in TODs] and f.endswith(".ver"):
            paths.append(os.path.join(root, f))

#create dictionaries to hold values
OZonalPuT = {}
DZonalPuT = {}
OZonalCar = {}
DZonalCar = {}

PassBoard  = {}
PassAlight = {}
PassOrigin = {}
PassDest   = {}
PassTransTot = {}

LinePassBoard = {}
LinePassAlight = {}

#gather attributes from version files
for versionFilePath in paths:
    #open time peiod version file and collect TOD attribute for labeling purposes
    Visum.LoadVersion(versionFilePath)
    TOD = Visum.Net.AttValue("TOD")
    print TOD
    
    #collect attributes from time period version files
    ZoneNo = h.GetMulti(Visum.Net.Zones, "No")
    #convert to integer (as necessary)
    for i in xrange(0, len(ZoneNo)):
        if ZoneNo[i] != None:
            ZoneNo[i] = int(ZoneNo[i])
    OZonalPuT[TOD] = h.GetMulti(Visum.Net.Zones, "MatRowSum(2200TrTotal)")
    DZonalPuT[TOD] = h.GetMulti(Visum.Net.Zones, "MatColSum(2200TrTotal)")
    OZonalCar[TOD] = h.GetMulti(Visum.Net.Zones, "MatRowSum(2000Highway)")
    DZonalCar[TOD] = h.GetMulti(Visum.Net.Zones, "MatColSum(2000Highway)")
    
    #gather stop point attributes
    StopNo = h.GetMulti(Visum.Net.StopPoints, "No")
    for i in xrange(0, len(StopNo)):
        if StopNo[i] != None:
            StopNo[i] = int(StopNo[i])
    StopCode = h.GetMulti(Visum.Net.StopPoints, "Code")
    StopName = h.GetMulti(Visum.Net.StopPoints, "Name")
    NodeNo = h.GetMulti(Visum.Net.StopPoints, "NodeNo")
    for i in xrange(0, len(NodeNo)):
        if NodeNo[i] != None:
            NodeNo[i] = int(NodeNo[i])
    PassBoard[TOD] = h.GetMulti(Visum.Net.StopPoints, "PassBoard(AP)")
    PassAlight[TOD] = h.GetMulti(Visum.Net.StopPoints, "PassAlight(AP)")
    PassOrigin[TOD] = h.GetMulti(Visum.Net.StopPoints, "PassOrigin(AP)")
    PassDest[TOD] = h.GetMulti(Visum.Net.StopPoints, "PassDestination(AP)")
    PassTransTot[TOD] = h.GetMulti(Visum.Net.StopPoints, "PassTransTotal(AP)")
    #PassThruStop = h.GetMulti(Visum.Net.StopPoints, "PassThroughStop(AP)")
    #PassThruNoStop = h.GetMulti(Visum.Net.StopPoints, "PassThroughNoStop(AP)")
    
    #gather line attributes
    LineName = h.GetMulti(Visum.Net.Lines, "Name")
    TSysCode = h.GetMulti(Visum.Net.Lines, "TSysCode")
    LinePassBoard[TOD] =  h.GetMulti(Visum.Net.Lines, "Sum:LineRoutes\Sum:LineRouteItems\PassBoard(AP)")
    LinePassAlight[TOD] = h.GetMulti(Visum.Net.Lines, "Sum:LineRoutes\Sum:LineRouteItems\PassAlight(AP)")


#create lists of zeros to add to
sum_OZonalPuT = [0] * len(ZoneNo)
sum_DZonalPuT = [0] * len(ZoneNo)
sum_OZonalCar = [0] * len(ZoneNo)
sum_DZonalCar = [0] * len(ZoneNo)

sum_PassBoard    = [0] * len(StopNo)
sum_PassAlight   = [0] * len(StopNo)
sum_PassOrigin   = [0] * len(StopNo)
sum_PassDest     = [0] * len(StopNo)
sum_PassTransTot = [0] * len(StopNo)

sum_LinePassBoard  = [0] * len(LineName)
sum_LinePassAlight = [0] * len(LineName)

#sum to get daily total
for key in OZonalPuT:
    for i in xrange(0,len(sum_OZonalPuT)):
        sum_OZonalPuT[i] += OZonalPuT[key][i]
for key in DZonalPuT:
    for i in xrange(0,len(sum_DZonalPuT)):
        sum_DZonalPuT[i] += DZonalPuT[key][i]
for key in OZonalCar:
    for i in xrange(0,len(sum_OZonalCar)):
        sum_OZonalCar[i] += OZonalCar[key][i]
for key in DZonalCar:
    for i in xrange(0,len(sum_DZonalCar)):
        sum_DZonalCar[i] += DZonalCar[key][i]
    
for key in PassBoard:
    for i in xrange(0,len(sum_PassBoard)):
        sum_PassBoard[i] += PassBoard[key][i]
for key in PassAlight:
    for i in xrange(0,len(sum_PassAlight)):
        sum_PassAlight[i] += PassAlight[key][i]
for key in PassOrigin:
    for i in xrange(0,len(sum_PassOrigin)):
        sum_PassOrigin[i] += PassOrigin[key][i]
for key in PassDest:
    for i in xrange(0,len(sum_PassDest)):
        sum_PassDest[i] += PassDest[key][i]
for key in PassTransTot:
    for i in xrange(0,len(sum_PassTransTot)):
        sum_PassTransTot[i] += PassTransTot[key][i]
    
for key in LinePassBoard:
    for i in xrange(0,len(sum_LinePassBoard)):
        sum_LinePassBoard[i] += LinePassBoard[key][i]
for key in LinePassAlight:
    for i in xrange(0,len(sum_LinePassAlight)):
        sum_LinePassAlight[i] += LinePassAlight[key][i]

#add to dataframes
zones_df['2x_OZonalPuT'] = sum_OZonalPuT
zones_df['2x_DZonalPuT'] = sum_DZonalPuT
zones_df['2x_OZonalCar'] = sum_OZonalCar
zones_df['2x_DZonalCar'] = sum_DZonalCar

stops_df['2x_PassBoard'   ] = sum_PassBoard
stops_df['2x_PassAlight'  ] = sum_PassAlight
stops_df['2x_PassOrigin'  ] = sum_PassOrigin
stops_df['2x_PassDest'    ] = sum_PassDest
stops_df['2x_PassTransTot'] = sum_PassTransTot

lines_df['2x_LinePassBoard' ] = sum_LinePassBoard
lines_df['2x_LinePassAlight'] = sum_LinePassAlight
     
#calculate difference and percent difference columns





    #write output for zonal transit ridership attributes
    #change csv names for subsequent runs on different run folders
    if not os.path.exists(r'D:\BikePedTransit\RTPS\Frequency\HalfZonalPuT.csv'):
        with open(r'D:\BikePedTransit\RTPS\Frequency\HalfZonalPuT.csv','wb') as IO:
            w = csv.writer(IO)
            w.writerow(['ZoneNo', 'OriginZonalPuT', 'DestinationZonalPuT','TOD'])

    with open(r'D:\BikePedTransit\RTPS\Frequency\HalfZonalPuT.csv','ab') as IO:
        w = csv.writer(IO)
        for i in xrange(0, len(ZoneNo)):
            w.writerow([
                ZoneNo[i],
                OZonalPuT[i],
                DZonalPuT[i],
                TOD
            ])
            
    #write output for stop point level transit ridership attributes
    #change csv names for subsequent runs on different run folders
    if not os.path.exists(r'D:\BikePedTransit\RTPS\Frequency\HalfStopPointPuT.csv'):
        with open(r'D:\BikePedTransit\RTPS\Frequency\HalfStopPointPuT.csv','wb') as IO:
            w = csv.writer(IO)
            w.writerow(['StopNo', 'StopCode', 'StopName','NodeNo', 'PassBoard', 'PassAlight', 'PassOrigin', 'PassDest', 'PassTransTot', 'PassThruStop', 'PassThruNoStop', 'TPLineCount', 'TPLaneNames', 'TPTSys', 'TPTSysCount', 'TOD'])

    with open(r'D:\BikePedTransit\RTPS\Frequency\HalfStopPointPuT.csv','ab') as IO:
        w = csv.writer(IO)
        for i in xrange(0, len(ZoneNo)):
            w.writerow([
                StopNo[i],
                StopCode[i],
                StopName[i],
                NodeNo[i],
                PassBoard[i],
                PassAlight[i],
                PassOrigin[i],
                PassDest[i],
                PassTransTot[i],
                PassThruStop[i],
                PassThruNoStop[i],
                TPLineCount[i],
                TPLaneNames[i],
                TPTSys[i],
                TPTSysCount[i],
                TOD
            ])

