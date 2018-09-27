import numpy
import VisumPy.helpers as h
import os
import csv
import pandas as pd

Visum = h.CreateVisum(15)

versionfile = r"D:\BikePedTransit\RTPS\ServiceFrequencyBase_wShuttles\TIM23_2015_Base_FixPuT_180830b.ver"

Visum.LoadVersion(versionfile)

#pull attributes from version file including which lines use each link and how many times each lines uses it oper day
LinkNo = h.GetMulti(Visum.Net.Links, "No")
FromNode = h.GetMulti(Visum.Net.Links, "FromNodeNo")
ToNode = h.GetMulti(Visum.Net.Links, "ToNodeNo")
TypeNo = h.GetMulti(Visum.Net.Links, "TypeNo")
LinesOnLink = h.GetMulti(Visum.Net.Links, "Distinct:LineRoutes\LineName")
DailyJourneys = h.GetMulti(Visum.Net.Links, "Histogram:LineRoutes\Histogram:VehJourneys\LineName")

#convert daily journeys string into a dictionary
vehjour_d = []
empty = 0
for i in xrange(0, len(DailyJourneys)):
    if DailyJourneys[i]:
        a = DailyJourneys[i].split(',')
        keys = []
        vals = []
        for item in a:
            #print item
            x = string.replace(item,'[', '')
            y = string.replace(x,']', '')
            z = y.split(':')
            strings = []
            for val in z:
                strings.append(str(val))
            keys.append(strings[0])    
            vals.append(strings[1])
        #intvals = []
        #for t in val:
        #    intvals.append(int(float(t)))
        d = dict(zip(keys, vals))
        vehjour_d.append(d)
    else: 
        empty += 1
        vehjour_d.append(0)

print empty

#test to make sure it worked
vehjour_d[45]

#convert linesoklink string to a list
LinesList = []
for i in xrange(0, len(LinesOnLink)):
    lines = []
    b = LinesOnLink[i].split(',')
    for item in b:
        lines.append(str(item))
    LinesList.append(lines)
    
#connect to sql db to pull other line/link level data that has already been prepared
import psycopg2 as psql # PostgreSQL connector
#connect to SQL DB in python
con = psql.connect(dbname = "RTPS", host = "localhost", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()


#grab link speed by line data
GetLinkSpeed = """
    SELECT
        no, 
        fromnodeno,
        tonodeno,
        cnt,
        avgspeed
    FROM linkspeed_byline
    ;"""
cur.execute(GetLinkSpeed)
linkspeed = cur.fetchall()

#put it into lists
linknum = []
for i in xrange(0, len(linkspeed)):
    linknum.append(int(linkspeed[i][0]))
fromnodeno = []
for i in xrange(0, len(linkspeed)):
    fromnodeno.append(int(linkspeed[i][1]))
tonodeno = []
for i in xrange(0, len(linkspeed)):
    tonodeno.append(int(linkspeed[i][2]))
scheduledspeed = []
for i in xrange(0, len(linkspeed)):
    scheduledspeed.append(linkspeed[i][4])
    
#grab stats by line data
GetStatsbyLine = """
    SELECT
        linename, 
        name,
        ampeak_f,
        base_freq,
        dailyrider,
        otp, 
        division
    FROM statsbyline_allgeom
    ;"""
cur.execute(GetStatsbyLine)
StatsbyLine = cur.fetchall()

#put it into lists
s_linename = []
for i in xrange(0, len(StatsbyLine)):
    s_linename.append(StatsbyLine[i][0])
ampeak_f = []
for i in xrange(0, len(StatsbyLine)):
    ampeak_f.append(StatsbyLine[i][2])
base_freq = []
for i in xrange(0, len(StatsbyLine)):
    base_freq.append(StatsbyLine[i][3])
dailyrider = []
for i in xrange(0, len(StatsbyLine)):
    dailyrider.append(StatsbyLine[i][4])
otp = []
for i in xrange(0, len(StatsbyLine)):
    otp.append(StatsbyLine[i][5])
division = []
for i in xrange(0, len(StatsbyLine)):
    division.append(StatsbyLine[i][6])

#convert stats by line data to a nested dictionary
lineStats = {}
for line in s_linename:
    lineStats[line] = {}
    
for line in s_linename:
    a = s_linename.index(line)
    lineStats[line]['ampeak'] = ampeak_f[a]
    lineStats[line]['base_freq'] = base_freq[a]
    lineStats[line]['dailyrider'] = dailyrider[a]
    lineStats[line]['otp'] = otp[a]
    lineStats[line]['division'] = division[a]
        
        
#calculate OTP for weighted by number of times each line uses that link
linkOTP =[]
for i in xrange(0,len(vehjour_d)):
    if vehjour_d[i] <> 0:
        weightedotp = 0
        weights = 0
        for key, value in vehjour_d[i].items():
            if key in lineStats:
                lineotp = lineStats[key]['otp']
                weights += int(value)
                weightedotp += (int(lineotp) * int(value))
                linkOTP.append(weightedotp/weights)
    else:
        linkOTP.append(0)
        
        
#this can be repeaded for any attribute at the line level
#incorporating stop level ridership will be a different story

