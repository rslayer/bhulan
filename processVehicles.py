from init import *
import glob
from computed import *
import xlrd
import datetime
from util import (notify, throwException, kilDist, mileDist, getDate, getDateNum, getClockTime, getSeconds,
                  getMinutes, getHours, getDateTime, getDateNumForChile, xldate_to_datetime, getExcelDate)
COMPUTED = None
SAMPLE_RATE = 100
MAX_DISTANCE = 5
MAX_SPEED = 40

################# Begin Core Functions #######################

def formatCoord(coord):
    return coord / 10000000000.0
################# Begin Helper Funcs #######################


################# Begin Helper Funcs #######################

def timeKeyFunc(x):
    return x.time

def getNumZeros(cluster):
    return sum([1 if point.velocity == 0 else 0 for point in cluster])


def getCentroid(cluster):
    length = float(len(cluster))
    lat = sum([x.lat for x in cluster]) / length
    lon = sum([x.lon for x in cluster]) / length

    return Point(lat, lon)


def getDistances(centroids, distFunc=kilDist):
    num = len(centroids)
    dists = []
    for i in range(num):
        dist = []
        centroid1 = centroids[i]
        for j in range(num):
            if i is not j:
                centroid2 = centroids[j]
                dist.append(distFunc(centroid1, centroid2))

        dists.append(dist)

    return dists


def getDiameter(cluster, distFunc=kilDist):
    distances = getDistances(cluster, distFunc)
    return max([max(x) for x in distances])


def getStopTime(cluster):
    minTime = min(cluster, key=timeKeyFunc).time
    maxTime = max(cluster, key=timeKeyFunc).time
    return getMinutes(maxTime) - getMinutes(minTime)


def getStartStop(cluster, timeFunc=getClockTime):
    minTime = min(cluster, key=timeKeyFunc).time
    maxTime = max(cluster, key=timeKeyFunc).time

    return (timeFunc(minTime), timeFunc(maxTime))


def getSpeed(point1, point2):
    hours1 = getHours(point1.time)
    hours2 = getHours(point2.time)

    dist = mileDist(point1, point2)

    return abs(dist / (hours1 - hours2))

def getTrucks(db):
    tbl = TruckPoint.getTbl(db)
    return tbl.distinct(TRUCK_ID_KEY)


def getDateNums(db):
    tbl = TruckPoint.getTbl(db)
    return tbl.distinct(DATE_NUM_KEY)

def getTruckPoints(truckId, db, dateNum=None):
    if dateNum == None:
        return TruckPoint.findItemList(TRUCK_ID_KEY, truckId, db)
    return TruckPoint.find({TRUCK_ID_KEY: truckId, DATE_NUM_KEY: dateNum}, db)

def getLatLonPoints(truckId, db, dateNum=None):
    points = getTruckPoints(truckId, db, dateNum)
    latLons = [x.getLatLon() for x in points]
    final = [latLons[i] for i in range(0, len(latLons), SAMPLE_RATE)]

    return latLons

def getPointsAll(dateNum, db, trucks=None):
    if trucks is None:
        trucks = getTrucks(db)
    pointsAll = {}

    for truckId in trucks:
        points = getTruckPoints(truckId, db, dateNum)
        pointsAll[truckId] = points

    return pointsAll


def getGPSFrequency(truckId, dateNum, db=WATTS_DATA_DB_KEY):
    points = getTruckPoints(truckId, db, dateNum)
    numPoints = len(points)
    if numPoints < 2:
        return None
    diffs = []
    for i in range(0, numPoints - 1):
        point1 = points[i]
        point2 = points[i + 1]
        time1 = getSeconds(point1.time)
        time2 = getSeconds(point2.time)

        diffs.append(time2 - time1)
    print diffs
    return sum(diffs) / float(numPoints - 1)

def sortList(stDict):
    for key in sorted(stDict.iterkeys()):
        print "%s : %s" % (key, stDict[key])

################# Begin Computed Funcs #######################

def saveTruckDateCombo(combo, dateNum, db):
    item = {}
    item[AVAILABILITY_KEY] = combo
    item[DATE_NUM_KEY] = dateNum
    item[ROUTE_CENTERS_KEY] = None
    TruckDates.saveItem(item, db)


def computeTruckDateCombos(db=WATTS_DATA_DB_KEY):
    TruckDates.deleteItems(db)
    dateNums = getDateNums(db)
    truckIds = getTrucks(db)
    for dateNum in dateNums:
        truckDateCombo = {}
        for truckId in truckIds:
            item = TruckPoint.findOne({TRUCK_ID_KEY: truckId, DATE_NUM_KEY: dateNum}, db)
            if item is not None:
                truckDateCombo[truckId] = True
            else:
                truckDateCombo[truckId] = False

        saveTruckDateCombo(truckDateCombo, dateNum, db)


def computeRouteCenters(db=WATTS_DATA_DB_KEY):
    dateNums = getDateNums(db)
    for dateNum in dateNums:
        truckDates = TruckDates.findItem(DATE_NUM_KEY, dateNum, db)
        availability = truckDates.availability
        centers = {}
        for truckId in availability:
            if availability[truckId]:
                points = getTruckPoints(truckId, db, dateNum)
                center = getCentroid(points)
                centers[truckId] = center.getItem()
            else:
                centers[truckId] = None

        truckDates.routeCenters = centers
        truckDates.save()

def findStops(truckId, dateNum, db=WATTS_DATA_DB_KEY, constraint=None):
    if constraint is None:
        constraint = CONSTRAINT
    points = getTruckPoints(truckId, db, dateNum)
    numPoints = len(points)

    first = None
    last = None
    cluster = []
    clusters = []
    for i in range(0, numPoints):
        point = points[i]
        if first is None:
            first = point
            cluster.append(first)
        else:
            if kilDist(first, point) < constraint:
                last = point
                cluster.append(last)
                first = getCentroid(cluster)
            else:
                if len(cluster) > 1:
                    clusters.append(cluster)
                last = None
                first = point
                cluster = [first]
    if len(cluster) > 1:
        clusters.append(cluster)

    lengths = [len(i) for i in clusters]
    zeros = [getNumZeros(i) for i in clusters]
    times = [getStopTime(i) for i in clusters]
    centroids = [getCentroid(i) for i in clusters]
    diameters = [getDiameter(i) for i in clusters]
    distances = getDistances(centroids, kilDist)
    startStops = [getStartStop(i, timeFunc=getClockTime) for i in clusters]

    filtered = []
    for i in range(len(clusters)):
        if times[i] >= MIN_STOP_TIME:
            stop = {}
            stop[POINT_KEY] = centroids[i]
            stop[RADIUS_KEY] = diameters[i] / 2
            stop[START_STOP_KEY] = startStops[i]

            filtered.append(stop)

    return filtered


def findStopsAll(db=WATTS_DATA_DB_KEY, constraint=None, trucks=None,datenums=None):
    if datenums is None:
        datenums = getDateNums(db)
    if trucks is None:
        trucks = getTrucks(db)

    stopsAll = []

    for dns in datenums:
        for truckId in trucks:
            print 'processing: '+str(truckId)+' for date: '+str(dns)
            stops = findStops(truckId, dns, db, constraint)
            for s in stops:
                dat = [dns,truckId, s['point'].lat, s['point'].lon, s['radius'],s['startStop'][0],s['startStop'][1]]
                stopsAll.append(dat)

    return stopsAll

################# End Helper Funcs #######################

################# Begin Core Funcs #######################

def initCompute(db=WATTS_DATA_DB_KEY):
    computeTruckDateCombos(db)
    notify("computed truck dates")
    computeRouteCenters(db)
    COMPUTED = Computed()


def createMongoItem(code, patent, timestamp, lat, lon, direction, commune, velocity, temperature):
    truck = {}
    truck[TRUCK_ID_KEY] = code
    truck[TIME_KEY] = str(getDateTime(timestamp).time())
    truck[VELOCITY_KEY] = velocity
    truck[LAT_KEY] = lat
    truck[LON_KEY] = lon
    truck[DATE_NUM_KEY] = getDateNum(timestamp)
    truck[PATENT_KEY] = patent
    truck[DIRECTION_KEY] = direction #has to be formatted
    truck[TEMPERATURE_KEY] = temperature
    truck[COMMUNE_KEY] = commune

    return truck
##
# current version of the API is hardcoded to read excel files in a given format
def readData(filename, db):
    print 'importing file:',filename
    workbook = xlrd.open_workbook(filename)
    worksheet = workbook.sheet_by_name(WORKSHEET_NAME)
    num_rows = worksheet.nrows - 1
    num_cells = worksheet.ncols - 1
    curr_row = 1
    items = []
    count = 0
    while curr_row < num_rows:
        row = worksheet.row(curr_row)

        code, patent, day, hour, lat, lon, direction, commune, velocity, temperature, temp2 = [x.value for x in row]
        try:
            timestamp = xlrd.xldate_as_tuple(day+hour,0)
        except TypeError:
            timestamp = xlrd.xldate_as_tuple(float(getExcelDate(day))+hour,0)

        #throwException('testing this out')
        item = createMongoItem(code, patent, timestamp, lat, lon, direction, commune, velocity, temperature)
        items.append(item)
        curr_row += 1
        item = []

    TruckPoint.saveItems(items, db)

################# End Helper Functions #################

################# Begin Core Functions #################

def pointsStraight(truckId, db):
    return TruckPoint.findItem(TRUCK_ID_KEY, truckId, db)

def importTrucks(db=WATTS_DATA_DB_KEY, delete=True):
    if delete:
        TruckPoint.deleteItems(db)

    filenames = glob.glob(GPS_FILE_DIRECTORY+GPS_FILE_EXTENSION)
    for filename in filenames:
        if '$' in filename or '~' in filename:
            continue
        else:
            readData(filename, db)

################# End Core Functions #################