# find the number of zones you can get to via transit from each zone in a certain amount of time

import numpy
import VisumPy.helpers as h
import csv
import os

#look for version files in the run folder
runDir = r"D:\BikePedTransit\RTPS\ServiceFrequencyBase"
TODs = ["AM", "MD", "PM", "NT"]

#append the TOD keywords to the file path
paths = []
for root, dirs, files in os.walk(runDir):
    for f in files: 
        if True in [TOD in f for TOD in TODs] and f.endswith(".ver"):
            paths.append(os.path.join(root, f))

            
###Volume = service frequency; matrix 100000            
#open version files
Visum = h.CreateVisum(15)
#create blank dictionaries for TOD_VolSums and RIT to hold those values
#TOD_VolSums = {}
RIT = {}
#create a blank variable for future use
dims = None

#for each version file in the paths found above
#load the version file
for versionFilePath in paths:
    Visum.LoadVersion(versionFilePath)
    TOD = Visum.Net.AttValue("TOD")

    #Pull zone numbers from Version file
    Zone_Number = h.GetMulti(Visum.Net.Zones,"No")
    #convert the zone numbers to integers (they come out of visum as float)
    for i in xrange(0,len(Zone_Number)):
        Zone_Number[i] = int(Zone_Number[i])
    #pull Ride times from matrix
    TOD_TransitRideTime = numpy.array(h.GetSkimMatrix(Visum, 100005)) #RIT
    #create a blank matrix in the shape of TOD_Volume (array)
    dims = TOD_TransitRideTime.shape
    #take the sum of the travel times across all 4 time periods
    RIT[TOD] = TOD_TransitRideTime

  
#create new variables to be added to; set SUM_RIT to zero in the shape of dims set above
SUM_RIT = numpy.zeros(dims)
#for a zone in RIT, multiply the volume and RIT to get the product (SUM_RIT) 
for key in RIT:
    SUM_RIT += RIT[key]
    #SumWeights += TOD_VolSums[key]


#divide the sum-product by the sum of the weights to calculate final average values (AVG_RIT_NoWeight); the output is an array
AVG_RIT_NoWeight = SUM_RIT/4

#return number zones accessibile within 45 minutes via transit for each zone
#write out to .csv
#"""with open(r'D:\BikePedTransit\RTPS\ServiceAccessibility\RIT.csv','wb') as IO:
#    w = csv.writer(IO)
#    w.writerow(['Zone_Number','RowCount', 'ColumnCount'])
#    #ZoneRow is a counter that matches the row number for the Zone_Number list which already exists
#    ZoneRow = 0
#    for row in AVG_RIT_NoWeight:
#        Column = AVG_RIT_NoWeight[:,ZoneRow]
#        w.writerow([Zone_Number[ZoneRow], row[row < 45].size, Column[Column <45].size] )
#        ZoneRow += 1
#"""
#read csv with essiential service counts; read in s strings, so convert to integer to do things with them
with open(r'D:\BikePedTransit\RTPS\ServiceAccessibility\ES_ByTAZ.csv','rb') as IO:
    r = csv.reader(IO)
    header = r.next() #read header row and move on to next before starting for loop
    #create array to hold integers from csv
    ES_byTAZ = []
    for row in r:
        Integers = []
        for x in row:
            Integers.append(int(x))
        ES_byTAZ.append(Integers)

#create nested dictionary
Zone = {}
#create dictionaries within the zone dictionary
for i in xrange(0, len(ES_byTAZ)):
#Each column within the csv is its own dictionary; the keys are the field titles, the values are the values in the columns from the csv called out by the index number (which starts at 0)
    Zone[ES_byTAZ[i][0]] = {
        "Jobs":             ES_byTAZ[i][2],
        "ParkTrail":        ES_byTAZ[i][3],
        "ActivityCenter":   ES_byTAZ[i][4],
        "Grocery":          ES_byTAZ[i][5],
        "HealthFac":        ES_byTAZ[i][6],
        "SchoolU":          ES_byTAZ[i][7]
    }
#there are some zones in the model that are not included in the csv  - all outside of the region
#create a dictionary to store these zones to make sure we aren't missing anything important
NoDataZones = {}
    
#create file and open for writing as the for loop iterates
with open(r'D:\BikePedTransit\RTPS\ServiceAccessibility\Base_NoBus_60minRIT.csv','wb') as IO:
    w = csv.writer(IO)
    #write the header row
    w.writerow(['Zone_Number','Jobs','ParkTrail','ActivityCenter','Grocery','HealthFac','SchoolU','Count'])    
    #for each origin zone
    for i in xrange(0,len(AVG_RIT_NoWeight)):
        #i is origin zones, j is destination zones
        #OZone is the zone number in position i
        OZone = Zone_Number[i]
        #create blank dictionary
        OZoneDict = {}
        #create counter starting at 0
        CountZones = 0
        #for each destination zone from that origin zone (i)
        for j in xrange(0, len(AVG_RIT_NoWeight[i])):
            #if weighted Ride time at inteserction of Origin Zone [i] and Destination zone [j] in the AVG_RIT_NoWeight array is less than 60 minues
            if AVG_RIT_NoWeight[i][j]<60:
                # and if the zone is in the csv with essential service attributes
                if Zone.has_key(Zone_Number[j]):
                    #add that destination zone number as a key in OZone Dict
                    #the values for that key are the values in the zone dictionary counting the essential services for that destination zone
                    OZoneDict[Zone_Number[j]] = Zone[Zone_Number[j]]
                #OZoneDict has the zone numbers of the zones that can be accessed in 45 mins (Dzones) as the key and the attributes from the Zone Dictionary as the value
                else: 
                    #if the key (destination zones) is not in the csv, add it and its corresponding origin zone to the NoDataZones csv for tracking purposes
                    #.has_key is a dictionary function to see if a certain key is contained in the csv
                    #NoDataZones in a dictionary and the values are arrays
                    #the keys are the OZone numbers and the values are arrays of DZone numbers within 45 minutes of the OZone that are not in the csv
                    if not NoDataZones.has_key(Zone_Number[i]):
                        NoDataZones[OZone] = []
                    NoDataZones[OZone].append(Zone_Number[j])
                #every time the if statement evaluates to true, count the zone (even if there is no data in the CSV for that zone because it's external); the resulting countZones variable will be the count of zones accessible within 45 minues via transit
                CountZones += 1
                
            
        #create placeholder dictionary that starts at 0 and can be added to
        Service = {
            "Jobs":              0,
            "ParkTrail":         0,
            "ActivityCenter":    0,
            "Grocery":           0,
            "HealthFac":         0,
            "SchoolU":           0
        }
        #for each DZone and its corresponding attributes from the Zone dictionary, add the values (additivly) for those attributes to the place holder dictionary (Service)
        #this happens for every DZone accessible from a certain OZone in OZoneDict
        for key,value in OZoneDict.items():
            Service["Jobs"]             += value["Jobs"]
            Service["ParkTrail"]        += value["ParkTrail"]
            Service["ActivityCenter"]   += value["ActivityCenter"]
            Service["Grocery"]          += value["Grocery"]   
            Service["HealthFac"]        += value["HealthFac"]   
            Service["SchoolU"]          += value["SchoolU"]
        #wrtie the resulting totals before processing the next OZone
        w.writerow([OZone,
            Service["Jobs"]            ,
            Service["ParkTrail"]       ,
            Service["ActivityCenter"]  ,
            Service["Grocery"]         ,
            Service["HealthFac"]       ,
            Service["SchoolU"],
            CountZones])
            #this file saves and closes automatically when the for loop is finished (reaches the end of xrange - len(AVG_RIT_NoWeight))
            
#write out NoDataZones to csv for reference
#the resulting csv has 2 columns, 1 is the origin zone, 2 is the destination zone within 45 minutes with no data. origin zones are repeated for however many destination zones have no data.
with open(r'D:\BikePedTransit\RTPS\ServiceAccessibility\Base_NoBus_60minRIT_NoDataZones.csv','wb') as IO:
    w = csv.writer(IO)    
    w.writerow(['OZone','DZone'])
    for OZone in NoDataZones:
        for DZone in NoDataZones[OZone]:
            w.writerow([OZone, DZone])

            
        

    
