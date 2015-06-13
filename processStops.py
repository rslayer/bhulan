import datetime
import glob
from init import *
from util import (kilDist)
from computed import *
from inputOutput import getLineForItems
from processVehicles import findStopsAll

EMORNING = 'emorning'
LMORNING = 'lmorning'
AFTERNOON = 'afternoon'
EVENING = 'evening'
NIGHT = 'night'
MNTOMORN = 'mntomorn'
# truckId, dateNum, lat, lon, time, vel


# ################ Begin Database Helpers #######################

def toIso(sttime):
    date_object = datetime.strptime(sttime,'%H:%M:%S')
    return date_object.isoformat()

def getDuration(t1, t2):
    #print t2-t1
    ta = datetime.timedelta(hours=t1.hour,minutes=t1.minute)
    tb = datetime.timedelta(hours=t2.hour,minutes=t2.minute)
    a = tb - ta
    ret = datetime.time(hour=(a.seconds/3600), minute=(a.seconds/60)%60)
    return ret

def getTime(h,m):
    #return (MIN_NUM*h+m)
    return datetime.time(h,m)


def getCentroid(cluster):
    length = float(len(cluster))
    lat = sum([x.lat for x in cluster]) / length
    lon = sum([x.lon for x in cluster]) / length

    return Point(lat, lon)

def getTrucks(db):
    tbl = TruckPoint.getTbl(db)
    return tbl.distinct(TRUCK_ID_KEY)

def getTruckPoints(truckId, db, dateNum=None):
    if dateNum == None:
        return TruckPoint.findItemList(TRUCK_ID_KEY, truckId, db)
    return TruckPoint.find({TRUCK_ID_KEY: truckId, DATE_NUM_KEY: dateNum}, db)

# def getStops(db):
#     tbl = Stop.getTbl(db)
#     return tbl.distinct(ID_KEY)
#
# def getStopProperties(db):
#     tbl = StopProperties.getTbl(db)
#     return tbl.distinct(STOP_PROP_ID_KEY)

def saveStopsData(items, db, delete=False):
    Stop.saveItems(items,db,delete)

def saveStopsPropsData(items, db, delete=False):
    StopProperties.saveItems(items,db, delete)

# def deleteStopsDb(db):
# #    getStopsDb(client).remove()
#
# def deleteStopsPropsDb(db):
    #   getStopsPropsDb(client).remove()


def getStopPropsFromTruckDate(truckId, dateNum=None, db=WATTS_DATA_DB_KEY):
    if dateNum == None:
        return StopProperties.findItemList(TRUCK_ID_KEY, truckId, db)
    return StopProperties.find({TRUCK_ID_KEY: truckId, DATE_NUM_KEY: dateNum}, db)

def getStopsFromTruckDate(truckId, dateNum=None, db=WATTS_DATA_DB_KEY):
    stops = {}
    if dateNum == None:
        props = StopProperties.findItemList(TRUCK_ID_KEY, truckId, db)
        for s in props:
            x = Stop.findItem(ID_KEY,s.stopPropId, db)
            if stops.has_key(x.id):
                continue
            else:
                stops[x.id] = s
    else:
        props = StopProperties.find({TRUCK_ID_KEY: truckId, DATE_NUM_KEY: dateNum}, db)
        stops = {}
        #print len(props)
        #print 'hello world'
        for s in props:
            x = Stop.findItem(ID_KEY,s.stopPropId, db)
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
    props = StopProperties.findItem(ID_KEY,stopPropId,db)
    return Stop.findItem(ID_KEY,props.stopPropId,db)
    #{ID_KEY: stopPropId}, db)
def getStopPropsFromStopId(stopId, db=WATTS_DATA_DB_KEY):
    return StopProperties.findItems(STOP_PROP_ID_KEY,stopId,db)

def getStopTruckDateCombos(db=WATTS_DATA_DB_KEY, truckId=None, dateNum=None, stopPropId=None):

    return StopProperties.find({TRUCK_ID_KEY:truckId, DATE_NUM_KEY:dateNum, STOP_PROP_ID_KEY:stopPropId},db)

    # if dateNum and truckId and stopPropId:
    #     print 1
    #     return StopProperties.find({TRUCK_ID_KEY:truckId, DATE_NUM_KEY:dateNum, ID_KEY:stopPropId},db)
    # elif dateNum and truckId:
    #     print 2
    #     return StopProperties.find({TRUCK_ID_KEY:truckId, DATE_NUM_KEY:dateNum, STOP_PROP_ID_KEY:stopId},db)
    # elif dateNum and stopPropId:
    #     print 3
    #     return StopProperties.find({DATE_NUM_KEY:dateNum, ID_KEY:stopPropId, STOP_PROP_ID_KEY:stopId},db)
    # elif stopPropId and truckId:
    #     print 4
    #     return StopProperties.find({TRUCK_ID_KEY:truckId, ID_KEY:stopPropId, STOP_PROP_ID_KEY:stopId},db)

# get all the properties from the db
# then using the prop id, locate all the associated stops for that prop id
# return a dictionary of propid,centroid with the value of the stops
# intensive process, shot not be used
def getStopPropsCombos(db=WATTS_DATA_DB_KEY,stopPropId=None,thresh=None):
    tbl = StopProperties.getTbl(db)
    stopPropCombos = {}

    if stopPropId:
        propList = findStopProps(db,stopPropId)
    else:
        propList = list(tbl.find())

    for pl in propList:
        tup = (pl[STOP_PROP_ID_KEY],pl[LAT_KEY],pl[LON_KEY])
        propList = getStopTruckDateCombos(client,stopPropId=pl[ID_KEY])
        if len(propList) >= thresh:
            stopPropCombos[tup] = propList

    return stopPropCombos

# helper to save stops data to file temporarily
def saveStopPropsDataToFile(db=WATTS_DATA_DB_KEY):
    stops = StopProperties.getItems(db)

    with open('/Users/alikamil/Desktop/stoppropssall.csv','w') as wf:
        for i in stops.keys():
            prop = stops[i]
            items = [prop.id, prop.time]
            line = getLineForItems(items)
            wf.write(line)

def saveStopsDataToFile(db=WATTS_DATA_DB_KEY):
    stops = Stop.getItems(db)

    with open('/Users/alikamil/Desktop/stopsall.csv','w') as wf:
        for i in stops.keys():
            prop = stops[i]
            items = [prop.id, prop.time]
            line = getLineForItems(items)
            wf.write(line)

def saveStopsToFile(datenum):
    filename = "/Users/alikamil/Desktop/santiago_truckstops/santi_truckstops_"+str(datenum)+".csv"
    wf = open(filename,'w')
    trucklist = getTruckList().keys()
    wf.write("id,datenum,lat,lng,duration,time, truckid\n")

    for t in trucklist:
        stops = getStopPropsFromTruckDate(t, datenum)

        for s in stops:
            if inSantiago(s):
                tm = s.time.split(":")
                drt = s.duration.split(":")
                dt = datetime(year=2014, month=9, day=1,hour=int(tm[0]), minute=int(tm[1]), second=int(tm[2]))
                date = dt.strftime("%Y-%m-%d %H:%M:%S")
                ls = [s.id, date, s.lat, s.lon,s.duration, dt.isoformat(), s.truckId]
                line = getLineForItems(ls)
                wf.write(line)

    wf.close()

def saveTruckDateCombosToFile(truckId,datenum, db=WATTS_DATA_DB_KEY):
    cnt = 0
    dict = getStopsFromTruckDate(truckId,datenum)
    filename = "/Users/alikamil/Desktop/truckdata/"+truckId+"_"+str(datenum)+".csv"
    wf = open(filename,'w')
    wf.write("truckid,datenum,lat,lng,time,duration\n")
    print "total:",len(dict)
    for d in dict:
        x = dict[d]
        if inSantiago(x):
            #print getStopPropsFromStopId(x.id)
            ls = [truckId,datenum,x.lat, x.lon,toIso(x.time),x.duration]
            line = getLineForItems(ls)
            cnt +=1
            wf.write(line)

    wf.close()

################# End Database Helpers #######################


# stop ID, lat, lon, time of day, duration
def computeStopData():
    print 'processig stops - this process will take time, please be patient'
    stops = {}
    stats = {}
    stopList = []
    first = None
    stop_id = 1

    masterList = findStopsAll()

    for ml in masterList:
        addRow = True
        #print ml
        if first is None:
            point = (stop_id, float(ml[2]),float(ml[3]))
            stats[TRUCK_ID_KEY] = ml[1]
            stats[DATE_NUM_KEY] = ml[0]
            t1 = getTime(int(ml[5].split(":")[0]),int(ml[5].split(":")[1]))
            t2 = getTime(int(ml[6].split(":")[0]),int(ml[6].split(":")[1]))
            stats[TIME_KEY] = t1
            stats[DURATION_KEY] = getDuration(t1,t2)
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

                if (kilDist(oldPoint,newPoint)) <= CONSTRAINT:
                    #if yes, then it means they are the same stop.
                    #   add to the list under the stop entry without incrementing stop prop ID
                    #   add the truck id, time, duration, datenum, stop id, radius
                    # print "same stop so adding to the list of stops"

                    stats[TRUCK_ID_KEY] = ml[1]
                    stats[DATE_NUM_KEY] = ml[0]

                    t1 = getTime(int(ml[5].split(":")[0]),int(ml[5].split(":")[1]))
                    t2 = getTime(int(ml[6].split(":")[0]),int(ml[6].split(":")[1]))

                    stats[TIME_KEY] = t1
                    stats[DURATION_KEY] = getDuration(t1,t2)
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
                pt = (stop_id, float(ml[2]),float(ml[3]))

                stats[TRUCK_ID_KEY] = ml[1]
                stats[DATE_NUM_KEY] = ml[0]
                t1 = getTime(int(ml[5].split(":")[0]),int(ml[5].split(":")[1]))
                t2 = getTime(int(ml[6].split(":")[0]),int(ml[6].split(":")[1]))
                stats[TIME_KEY] = t1
                stats[DURATION_KEY] = getDuration(t1,t2)
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
        cluster=[]
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
            stopPropList.append(stopProp)
            cluster.append(Point(stopProp[LAT_KEY],stopProp[LON_KEY]))
            propID+=1

        centroid = getCentroid(cluster)
        stop[ID_KEY] = i[0]
        stop[LAT_KEY] = centroid.lat
        stop[LON_KEY] = centroid.lon

        stopList.append(stop)
        #print "printing PROPERTIES LIST\n"
        #print stopPropList
        # raise Exception('spam and eggs')
    saveStopsData(stopList,db, delete=True)
    saveStopsPropsData(stopPropList,db, delete=True)
    #notify("computed stops and properties")


def getStopByDuration(drtn,truckId=None):
    # if truckId == None:
    #     return StopProperties.findItemList(TRUCK_ID_KEY, truckId, db)
    # return StopProperties.find({TRUCK_ID_KEY: truckId, DATE_NUM_KEY: dateNum}, db)
    #
    print('x')

def findDCs(db=WATTS_DATA_DB_KEY):
    stops = Stop.getItemList(db)

    retd = {}
    print "stopid, lat, lon, truckid, duration, time, datenum"
    for st in stops:
        props = getStopPropsFromStopId(st.id)
        for pix in props:
            p = props[pix]
            dur = p.duration.split(":")
            if retd.has_key(st.id):
                if int(dur[0][1]) > DC_HOURS:
                    if inSantiago(p):
                        ls = [st.id, p.lat, p.lon, p.truckId,p.duration, p.time, p.dateNum]
                        #print getLineForItems(ls).rstrip("\n")
                        retd[st.id].append([p.id, p.lat, p.lon, p.duration, p.time, p.truckId, p.dateNum])
            else:
                if int(dur[0][1]) > DC_HOURS:
                    if inSantiago(p):
                        ls = [st.id, p.lat, p.lon, p.truckId,p.duration, p.time, p.dateNum]
                        print getLineForItems(ls).rstrip('\n')
                        retd[st.id] = [[p.id, p.lat, p.lon, p.duration, p.time, p.truckId, p.dateNum]]


    # go through the d, print the following
    # stopid, lat, lon, truckid, duration, time, datenum
    #
    # for rtd in retd:
    #     ls = retd[rtd]
    #     for l in ls:
    #         print l

    print len(retd)

def inSantiago(point):
    santi = Point(SANTI_LAT,SANTI_LON)
    if kilDist(point, santi) < SANTIAGO_RADIUS:
        #print kilDist(point, santi)
        return True
    else:
        return False

def getStopStatistics(truckId=None, dateNum=None):
    stprops = getStopPropsFromTruckDate(truckId,dateNum)
    stops = getStopsFromTruckDate(truckId,dateNum)

    print "TOTAL STOP PROPS:",len(stprops)
    print "TOTAL STOPs:",len(stops)
    for s in stprops:
        singStop = getStopFromStopPropId(s.id)
        print '--- TRUCK DATA ---'
        print 'STOP PROP ID: ' + str(s.id)
        print 'STOP ID: ' + str(singStop.id)
        print 'CENTROID LAT: ' + str(singStop.lat)
        print 'CENTROID LON: ' + str(singStop.lon)
        print 'LAT: ' + str(s.lat)
        print 'LON: ' + str(s.lon)
        print 'TRUCK ID: ' + s.truckId
        print 'DATENUM: ' + str(s.dateNum)
        print 'TIME: ' + s.time
        print 'DURATION: ' + s.duration
    return stprops,stops

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
    wfile = "/Users/alikamil/Desktop/windowFile.csv"
    f = open(wfile, "w")
    stops = Stop.getItems(db)
    cnt = 0
    # looking for window by truck and date
    #qry = ({TRUCK_ID_KEY:"VG-3837",DATE_NUM_KEY:314, STOP_PROP_ID_KEY:stopId})
    #
    # for s in stops:
    #     sts = stops[s]
#sts.id
    qry = ({STOP_PROP_ID_KEY:1})
    #qry = ({STOP_PROP_ID_KEY:sts.id})
    #ls = StopProperties.find(qry,CHILE_DATA_DB_KEY)
    ls = StopProperties.getItems(db)
    for prop in ls:
        i = ls[prop]
        #print "Time:",i.time, "Duration:",i.duration
        #print i.duration
        hour = getTime(int((i.duration).split(":")[0]),int((i.duration).split(":")[1])).hour
        #print time
        if hour < 4:
            windowhelper(timewindow,hour, i)


    for tw in timewindow:
        items = timewindow[tw]
        for i in items:
            tups = [i[0],i[1],i[2],i[3],i[4],i[5],i[6]]
            line = getLineForItems(tups)
            f.write(line)
        #print tw,":", timewindow[tw]


def getTruckList(db=WATTS_DATA_DB_KEY):
    tdict = {}
    tlist = TruckPoint.getItemList(db)
    for t in tlist:
        if tdict.has_key(t.truckId):
            tdict[t.truckId] += 1
        else:
            tdict[t.truckId] = 1

    return tdict

def getStopsFromDate(dateNum, db=WATTS_DATA_DB_KEY):
    print 'x'

def getTruckScheduleForDay(truckId, dateNum):
    dict = getStopsFromTruckDate(truckId,dateNum)
    schd = {}

    for d in dict:
        vl = dict[d]
        schd[vl.time] = list([vl.lat, vl.lon, vl.duration])

    return schd


#### Test area ####
if __name__ == '__main__':
    db = WATTS_DATA_DB_KEY
    saveComputedStops(db)

    #saveStopDataToDb()
    #saveStopsToFile(275)

    #findDCs(db)
    # path = "/Users/alikamil/Desktop/truckdata/11FC6904_"
    # datlist = [273,274,275,276,277,279,280]
    # for d in datlist:
    #     print "processing:",d
    #     saveStopsToFile(d)
        #io.sendtoCartodb(path+str(d)+".csv")

    #print getStopsFromTruckDate("11FC6904")
    #print io.sendtoCartodb(path)



    # trucklist = getTruckList().keys()
    # for t in trucklist:
    #     for d in datlist:
    #         saveTruckDateCombosToFile(t, d)

    # sched = getTruckScheduleForDay("11FC6904",275)
    #
    # for s in sorted(sched):
    #     print s+":"+str(sched[s])


    # print "id,datenum,lat,lng,duration,time, truckid"
    # for t in trucklist:
    #     stops = getStopPropsFromTruckDate(t, 273)
    #     for s in stops:
    #         tm = s.time.split(":")
    #         drt = s.duration.split(":")
    #         dt = datetime.datetime(year=2014, month=8, day=25,hour=int(tm[0]), minute=int(tm[1]), second=int(tm[2]))
    #         date = dt.strftime("%Y-%m-%d %H:%M:%S")
    #         #throwException("testing")
    #         ls = [s.id, date, s.lat, s.lon,s.duration, dt.time(), s.truckId]
    #         print getLineForItems(ls),


    #generating stop file for import
    # print 'stopid,lat,lon'
    # for s in stops:
    #     print str(s.id)+","+str(s.lat)+","+str(s.lon)

    # getStopStatistics(truckId='11FC6990', dateNum=279)
    # sto = getStopsFromTruckDate(truckId='11FC6990', dateNum=273)
    # print sto
    # for s in sto:
    #     x = sto[s]
    #     print x.id
    #     print x.lat, x.lon
    #
    # print getTimeWindows()

    # stProperties, stops = getStopStatistics(truckId='VG-3837',dateNum=314)
    # print "*******************"
    # print "TIME     : DURATION"
    # drlst = {}
    # for stp in stProperties:
    #     drlst[stp.time] = (stp.duration)
    #sorted(drlst,key=itemgetter(0))
    #lst = sortList(drlst)
    #print lst
    #getStopStatistics(dateNum=314, truckId='VG-3837')
    #saveStopsDataToFile(db)

    #print getTimeWindows()

    #     return sorted(paths,key=itemgetter(1))
    #getTimeWindows(client,1)
    #a = getStopTruckDateCombos(client)
    #print len(a)
    # stops = getStopTruckDateCombos(client,dateNum=314, truckId='VG-3837')
    # print stops
    # stList = []
    # for i in stops:
    #     t = getTime(int(i[TIME_KEY].split(":")[0]),int(i[TIME_KEY].split(":")[1]))
    #     d = getTime(int(i[DURATION_KEY].split(":")[0]),int(i[DURATION_KEY].split(":")[1]))
    #     stList.append(((t.hour+60*t.minute),i[LAT_KEY], i[LON_KEY],d.hour, d.minute))
    #
    # stList = sorted(stList, key=lambda tup:tup[0])
    # print stList
    # for i in stList:
    #     print i[1],
    #     print ',',
    #     print i[2]#, i[3], i[4]


    # print len(findStop(client))
    # print len(findStopProps(client))
    #
    # deleteStopsDb(client)
    # deleteStopsPropshttp://isites.harvard.edu/course/colgsas-23513Db(client)
    # print findStop(client)
    # saveStopDataToDb(client)
    #'duration': datetime.timedelta(0, 1500), 'dateNum': 240, 'truckId': 'SG-5117', 'time': datetime.time(17, 34)}]
    # 0:3,7:21,

    print '*** done ***'



