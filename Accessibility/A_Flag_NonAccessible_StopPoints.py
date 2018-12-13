import VisumPy.helpers as h
import csv
import time
import numpy
import os

#flag connectors to remove based on stations that are not wheelchair accessible

#look for version files in the run folder
runDir = r"D:\BikePedTransit\RTPS\ServiceAccessibility\Accessible_Connectors\Current_NoBus"
TODs = ["AM", "MD", "PM", "NT"]

#append the TOD keywords to the file path
paths = []
for root, dirs, files in os.walk(runDir):
    for f in files: 
        if True in [TOD in f for TOD in TODs] and f.endswith(".ver"):
            paths.append(os.path.join(root, f))
            
#open version files
Visum = h.CreateVisum(15)

for versionFilePath in paths:
    Visum.LoadVersion(versionFilePath)
    TOD = Visum.Net.AttValue("TOD")
    
    #create UDAs
    Visum.Net.StopPoints.AddUserDefinedAttribute("BlackList","BlackList","BlackList",9,defval=False)
    
    #open and read list of nodes (from stop areas) the TWalk connectors need to be removed from
    with open(r'D:\BikePedTransit\RTPS\ServiceAccessibility\Accessible_Connectors\Current_NoBus\NodeNos_NotAccessible.csv','rb') as IO:
        r = csv.reader(IO)
        header = r.next()
        NodeList = []
        for row in r:
            NodeList.append(float(row[0]))


    Nodes = h.GetMulti(Visum.Net.StopPoints, "NodeNo", True)
    BlackList = h.GetMulti(Visum.Net.StopPoints, "BlackList", True)

    #set to false if NodeNo is in NodeList
    for i in xrange(0, len(BlackList)):
        if Nodes[i] in NodeList:
                BlackList[i] = True
     
    h.SetMulti(Visum.Net.StopPoints, "BlackList", BlackList, True)
    
    Visum.SaveVersion(Visum.UserPreferences.DocumentName)

##delete stop areas associated with these stop points (using filters)
##reskim RIT in each version file
