import datetime

from init import *
from util import (kilDist, getTimeDeltas, revGeoCode)
from processVehicles import findStopsAll
from classes import *


EMORNING = 'emorning'
LMORNING = 'lmorning'
AFTERNOON = 'afternoon'
EVENING = 'evening'
NIGHT = 'night'
MNTOMORN = 'mntomorn'
# truckId, dateNum, lat, lon, time, vel


# ################ Begin Database Helpers #######################

def getLineForItems(items):
    length = len(items)
    line = ""
    for i in range(length):
        item = items[i]
        line += str(item)
        if i != length - 1:
            line += ","
        else:
            line += "\n"
    return line


def getDateByDatenum(datenum):
    return (((getTruckPointsByDateNum(WATTS_DATA_DB_KEY, datenum))[0].timestamp).split('T')[0])


def getDuration(t1, t2):
    # print t2-t1
    ta = datetime.timedelta(hours=t1.hour, minutes=t1.minute)
    tb = datetime.timedelta(hours=t2.hour, minutes=t2.minute)
    a = tb - ta
    return (float(a.total_seconds() / 60))

    # 7/4/2015 - returning duration as total minutes instead of time value
    # ret = datetime.time(hour=(a.seconds/3600), minute=(a.seconds/60)%60)
    # return ret


def getTime(h, m):
    # return (MIN_NUM*h+m)
    return datetime.time(h, m)


def getCentroid(cluster):
    length = float(len(cluster))
    lat = sum([x.lat for x in cluster]) / length
    lon = sum([x.lon for x in cluster]) / length

    return Point(lat, lon)


def getTrucks(db):
    tbl = TruckPoint.getTbl(db)
    return tbl.distinct(TRUCK_ID_KEY)


def getTruckPointsByDateNum(db, dateNum):
    return TruckPoint.find({DATE_NUM_KEY: dateNum}, db)


def getTruckPoints(truckId, dateNum=None, db=WATTS_DATA_DB_KEY):
    if dateNum == None:
        return TruckPoint.findItemList(TRUCK_ID_KEY, truckId, db)
    return TruckPoint.find({TRUCK_ID_KEY: truckId, DATE_NUM_KEY: dateNum}, db)


def getStops(db):
    tbl = Stop.getTbl(db)
    return tbl.distinct(ID_KEY)


def getStopProperties(db):
    tbl = StopProperties.getTbl(db)
    return tbl.distinct(STOP_PROP_ID_KEY)


def saveStopsData(items, db, delete=False):
    Stop.saveItems(items, db, delete)


def saveStopsPropsData(items, db, delete=False):
    StopProperties.saveItems(items, db, delete)


# def deleteStopsDb(db):
# getStopsDb(client).remove()
#
# def deleteStopsPropsDb(db):
#     getStopsPropsDb(client).remove()


def getStopPropsFromTruckDate(truckId, dateNum=None, db=WATTS_DATA_DB_KEY):
    if dateNum == None:
        return StopProperties.findItemList(TRUCK_ID_KEY, truckId, db)
    return StopProperties.find({TRUCK_ID_KEY: truckId, DATE_NUM_KEY: dateNum}, db)


def getStopsFromTruckDate(truckId, dateNum=None, db=WATTS_DATA_DB_KEY):
    stops = {}
    if dateNum == None:
        props = StopProperties.findItemList(TRUCK_ID_KEY, truckId, db)
        for s in props:
            x = Stop.findItem(ID_KEY, s.stopPropId, db)
            if stops.has_key(x.id):
                continue
            else:
                stops[x.id] = s
    else:
        props = StopProperties.find({TRUCK_ID_KEY: truckId, DATE_NUM_KEY: dateNum}, db)
        stops = {}
        for s in props:
            x = Stop.findItem(ID_KEY, s.stopPropId, db)
            #print x.id,
            if stops.has_key(x.id):
                #print ":x",
                continue
            else:
                #print ":a"
                stops[x.id] = s

    return stops
    #return StopProperties.find({TRUCK_ID_KEY: truckId, DATE_NUM_KEY: dateNum}, db)


def getStopFromStopPropId(stopPropId, db=WATTS_DATA_DB_KEY):
    props = StopProperties.findItem(ID_KEY, stopPropId, db)
    return Stop.findItem(ID_KEY, props.stopPropId, db)


def getStopPropsFromStopId(stopId, db=WATTS_DATA_DB_KEY):
    return StopProperties.findItems(STOP_PROP_ID_KEY, stopId, db)


def getStopTruckDateCombos(db=WATTS_DATA_DB_KEY, truckId=None, dateNum=None, stopPropId=None):
    return StopProperties.find({TRUCK_ID_KEY: truckId, DATE_NUM_KEY: dateNum, STOP_PROP_ID_KEY: stopPropId}, db)


# get all the properties from the db
# then using the prop id, locate all the associated stops for that prop id
# return a dictionary of propid,centroid with the value of the stops
# intensive process, shot not be used
def getStopPropsCombos(db=WATTS_DATA_DB_KEY, stopPropId=None, thresh=None):
    tbl = StopProperties.getTbl(db)
    stopPropCombos = {}

    if stopPropId:
        propList = getStopFromStopPropId(stopPropId)
    else:
        propList = list(tbl.find())

    for pl in propList:
        tup = (pl[STOP_PROP_ID_KEY], pl[LAT_KEY], pl[LON_KEY])
        propList = getStopTruckDateCombos(client, stopPropId=pl[ID_KEY])
        if len(propList) >= thresh:
            stopPropCombos[tup] = propList

    return stopPropCombos


################# End Database Helpers #######################


# stop ID, lat, lon, time of day, duration
def computeStopData():
    print 'processing stops for each truck and date - this will take time, please be patient'
    stops = {}
    stats = {}
    stopList = []
    first = None
    stop_id = 1

    masterList = findStopsAll()

    for ml in masterList:
        addRow = True
        if first is None:
            point = (stop_id, float(ml[2]), float(ml[3]))
            stats[TRUCK_ID_KEY] = ml[1]
            stats[DATE_NUM_KEY] = ml[0]
            t1 = getTime(int(ml[5].split(":")[0]), int(ml[5].split(":")[1]))
            t2 = getTime(int(ml[6].split(":")[0]), int(ml[6].split(":")[1]))
            stats[TIME_KEY] = t1
            stats[DURATION_KEY] = getDuration(t1, t2)
            stats[LAT_KEY] = point[1]
            stats[LON_KEY] = point[2]
            stats[RADIUS_KEY] = ml[4]
            stopList.append(stats)
            stops[point] = stopList
            first = 1
            stats = {}
            stopList = []
        else:
            # get the new line from the document and get the lat,long for the stop
            # compare to the existing stops using kildist and if its less than constraint
            # if yes, then it means they are the same stop.
            #   add to the list under the stop entry without incrementing stop ID
            #   add the truck id, time, duration, datenum
            # if no, then it means its a differnet stop
            #   add a new entry with an id.
            #   add the truck id, datenum, stop time, and duration
            oldPoint = {}

            newPoint = {}
            newPoint[LAT_KEY] = float(ml[2])
            newPoint[LON_KEY] = float(ml[3])
            keys = stops.keys()
            for i in keys:

                oldPoint[LAT_KEY] = i[1]
                oldPoint[LON_KEY] = i[2]

                if (kilDist(oldPoint, newPoint)) <= CONSTRAINT:
                    #if yes, then it means they are the same stop.
                    #   add to the list under the stop entry without incrementing stop prop ID
                    #   add the truck id, time, duration, datenum, stop id, radius
                    # print "same stop so adding to the list of stops"

                    stats[TRUCK_ID_KEY] = ml[1]
                    stats[DATE_NUM_KEY] = ml[0]

                    t1 = getTime(int(ml[5].split(":")[0]), int(ml[5].split(":")[1]))
                    t2 = getTime(int(ml[6].split(":")[0]), int(ml[6].split(":")[1]))

                    stats[TIME_KEY] = t1
                    stats[DURATION_KEY] = getDuration(t1, t2)
                    stats[LAT_KEY] = newPoint[LAT_KEY]
                    stats[LON_KEY] = newPoint[LON_KEY]
                    stats[RADIUS_KEY] = ml[4]
                    stops[i].append(stats)
                    stats = {}

                    addRow = False

            # if all keys have been checked and no matches found then
            # if no, then it means its a differnet stop
            #   add a new entry with an id.
            #   add the truck id, datenum, stop time, and duration
            if addRow:
                stop_id += 1
                pt = (stop_id, float(ml[2]), float(ml[3]))

                stats[TRUCK_ID_KEY] = ml[1]
                stats[DATE_NUM_KEY] = ml[0]
                t1 = getTime(int(ml[5].split(":")[0]), int(ml[5].split(":")[1]))
                t2 = getTime(int(ml[6].split(":")[0]), int(ml[6].split(":")[1]))
                stats[TIME_KEY] = t1
                stats[DURATION_KEY] = getDuration(t1, t2)
                stats[LAT_KEY] = pt[1]
                stats[LON_KEY] = pt[2]
                stats[RADIUS_KEY] = ml[4]
                stopList.append(stats)
                stops[pt] = stopList
                stats = {}
                stopList = []

    return stops


def saveComputedStops(db=WATTS_DATA_DB_KEY):
    computedStopData = computeStopData()

    stopList = []
    stopPropList = []
    propID = 1

    keys = computedStopData.keys()
    for i in keys:
        cluster = []
        stop = {}
        ls = computedStopData[i]
        for j in ls:
            stopProp = {}
            stopProp[ID_KEY] = propID
            stopProp[LAT_KEY] = j[LAT_KEY]
            stopProp[LON_KEY] = j[LON_KEY]
            stopProp[TRUCK_ID_KEY] = j[TRUCK_ID_KEY]
            stopProp[DATE_NUM_KEY] = j[DATE_NUM_KEY]
            stopProp[DURATION_KEY] = str(j[DURATION_KEY])
            stopProp[TIME_KEY] = str(j[TIME_KEY])
            stopProp[STOP_PROP_ID_KEY] = i[0]
            stopProp[RADIUS_KEY] = j[RADIUS_KEY]
            stopProp[ADDRESS_KEY] = revGeoCode(j[LAT_KEY], j[LON_KEY])

            stopPropList.append(stopProp)
            cluster.append(Point(stopProp[LAT_KEY], stopProp[LON_KEY]))
            propID += 1

        centroid = getCentroid(cluster)
        stop[ID_KEY] = i[0]
        stop[LAT_KEY] = centroid.lat
        stop[LON_KEY] = centroid.lon

        stopList.append(stop)
    saveStopsData(stopList, db, delete=True)
    saveStopsPropsData(stopPropList, db, delete=True)
    return stopPropList


# returns stops with stop duration greater than the specified time in minutes
# input - drtn - minutes of duration
def getStopByDuration(drtn, db=WATTS_DATA_DB_KEY):
    stops = Stop.getItemList(db)
    retd = {}
    for st in stops:
        props = getStopPropsFromStopId(st.id)
        for prp in props:
            p = props[prp]
            if retd.has_key(st.id):
                if (float(p.duration) / 60) > drtn:
                    ls = [st.id, p.lat, p.lon, p.truckId, p.duration, p.time, p.dateNum]
                    retd[st.id].append([p.id, p.lat, p.lon, p.duration, p.time, p.truckId, p.dateNum])
            else:
                if (float(p.duration) / 60) > drtn:
                    if inSantiago(p):
                        ls = [st.id, p.lat, p.lon, p.truckId, p.duration, p.time, p.dateNum]
                        retd[st.id] = [[p.id, p.lat, p.lon, p.duration, p.time, p.truckId, p.dateNum]]
    return retd


def findPotentialDCs(db=WATTS_DATA_DB_KEY):
    return getStopByDuration(DC_HOURS)


def inSantiago(point):
    santi = Point(SANTI_LAT, SANTI_LON)
    if kilDist(point, santi) < SANTIAGO_RADIUS:
        #print kilDist(point, santi)
        return True
    else:
        return False


def getStopStatistics(truckId=None, dateNum=None):
    stprops = getStopPropsFromTruckDate(truckId, dateNum)
    stops = getStopsFromTruckDate(truckId, dateNum)

    print "TOTAL STOP PROPS:", len(stprops)
    print "TOTAL STOPs:", len(stops)
    for s in stprops:
        singStop = getStopFromStopPropId(s.id)
        print '--- TRUCK DATA ---'
        print 'STOP PROP ID: ' + str(s.id)
        print 'STOP ID: ' + str(singStop.id)
        print 'CENTROID LAT: ' + str(singStop.lat)
        print 'CENTROID LON: ' + str(singStop.lon)
        print 'LAT: ' + str(s.lat)
        print 'LON: ' + str(s.lon)
        print 'ADDRESS: ' + s.address
        print 'TRUCK ID: ' + s.truckId
        print 'DATENUM: ' + str(s.dateNum)
        print 'TIME: ' + s.time
        print 'DURATION: ' + s.duration
    return stprops, stops


def windowhelper(timewindow, time, i):
    if timewindow.has_key(time):
        timewindow[time].append((i.stopPropId, i.lat, i.lon, i.truckId, i.dateNum, i.time, i.duration))
    else:
        timewindow[time] = []
        timewindow[time].append((i.stopPropId, i.lat, i.lon, i.truckId, i.dateNum, i.time, i.duration))


# go through the stops
# create a dictionary for each hour of day {1.2. 24}
# the values for the dictionaries will be the truck id, datenum, and duration
# # go through each stop and its assoviated properties
# tally the properties and put them in hourly window buckets.
# # after
# u can query for each stop and get the windows.
# status - in progress
def getTimeWindows(db=WATTS_DATA_DB_KEY):
    #counted as 0-1, 1-2, 1-3...23-0
    timewindow = {}
    # stops = Stop.getItems(db)
    # cnt = 0
    # looking for window by truck and date
    #qry = ({TRUCK_ID_KEY:"VG-3837",DATE_NUM_KEY:314, STOP_PROP_ID_KEY:stopId})

    ls = StopProperties.getItems(db)
    for prop in ls:
        i = ls[prop]
        hour = float(i.duration) / 60
        if hour < 4:
            windowhelper(timewindow, hour, i)

    return timewindow


def getTruckList(db=WATTS_DATA_DB_KEY):
    tdict = {}
    tlist = TruckPoint.getItemList(db)
    for t in tlist:
        if tdict.has_key(t.truckId):
            tdict[t.truckId] += 1
        else:
            tdict[t.truckId] = 1

    return tdict


def getTruckScheduleForDay(truckId, dateNum):
    dict = getStopsFromTruckDate(truckId, dateNum)
    schd = {}

    for d in dict:
        vl = dict[d]
        schd[vl.time] = list([vl.lat, vl.lon, vl.duration])

    ret = []
    for i in sorted(schd):
        ret.append((i, schd[i]))

    return ret


def getTotalDistanceTraveled(truckId, datenum, db=WATTS_DATA_DB_KEY, ):
    ts = getTruckPoints(truckId, datenum, db)
    dist, tme = 0, 0
    totalDistance = 0
    first = True
    for t in ts:
        if first:
            dist = t.point
            first = False
            continue
        totalDistance += kilDist(dist, t.point)

        dist = t.point
    return totalDistance


def getTotalTimeOnRoad(truckId, datenum, db=WATTS_DATA_DB_KEY, ):
    ts = getTruckPoints(truckId, datenum, db)
    dist, tme = 0, 0
    totalDistance = 0
    totalTime = datetime.timedelta(hours=0, minutes=0, seconds=0)
    first = True
    for t in ts:
        if first:
            dist = t.point
            tme = t.time
            first = False
            continue
        curr = kilDist(dist, t.point)
        totalDistance += curr

        if curr > 0:
            x = getTimeDeltas(t.time) - getTimeDeltas(tme)
            totalTime = totalTime + x
        dist = t.point
        tme = t.time

    return totalTime.total_seconds() / 3600

def getAverageSpeedByDatenum(truckId, datenum):
    return getTotalDistanceTraveled(truckId, datenum) / getTotalTimeOnRoad(truckId, datenum)

def getAddressForStop(stop):
    return revGeoCode(stop.lat, stop.lon)


# #
# returns metrics (average speed, cost, distance) between
# stops on the file
# accepts StopProperties as input
def getMetricCostBetweenStops(stopA, stopB):
    distance = kilDist(Point(stopA.lat, stopA.lon), Point(stopB.lat, stopB.lon))
    return distance

#
# #print getTrucks(WATTS_DATA_DB_KEY)
# a = getTruckScheduleForDay("11FB5201",259)
# b = getStopsFromTruckDate("11FB5201",259)
# print a
# print getMetricCostBetweenStops(b[1],b[2])
