from constants import *
import xlrd
import math
from datetime import *
import time
HEAP_ID_KEY = "heapId"
PRIORITY_KEY = "priority"

def throwException(text):
    raise Exception(text)

################ Begin Graph Functions ################

def isEnd(edge, nodeId):
    return edge[END_NODE_KEY] == nodeId

def isStart(edge, nodeId):
    return edge[START_NODE_KEY] == nodeId

################ End Big Data Functions ################

################ Begin Date Time Functions ################

#excel function
# http://stackoverflow.com/questions/3727916/xldate-as-tuple
def getDate(timestamp):
    print "timestamp: " + str(timestamp)
    return xlrd.xldate_as_tuple(timestamp, 0)

def getDateTime(ts):
    dte = datetime(year=ts[0], month=ts[1], day=ts[2], hour=ts[3], minute=ts[4], second=ts[5])
    return dte

def getDateNumForChile(timestamp):
    month = timestamp.month
    day = timestamp.day
    return month * MONTH_NUM + day
    #print "datenumforchile:"+ str(x)
    #return x

#excel function
def getDateNum(timestamp):
    year, month, day, hour, minute, second = [x for x in timestamp]#xlrd.xldate_as_tuple(timestamp, 0)
    return month * MONTH_NUM + day

#excel function
def getClockTime(timestamp):
    #year, month, day, hour, minute, sec = getDate(timestamp)
    tm = timestamp.split(":")
    dte = datetime(year=1900, month=1, day=1,hour=int(tm[0]), minute=int(tm[1]), second=int(tm[2]))
    return str(dte.hour) + ":" + str(dte.minute)

#excel function
def getSeconds(timestamp):
    #year, month, day, hour, minute, sec = getDate(timestamp)
    tm = timestamp.split(":")
    dte = datetime(year=1900, month=1, day=1,hour=int(tm[0]), minute=int(tm[1]), second=int(tm[2]))
    return dte.hour * 3600 + dte.minute * 60 + dte.second

#excel function
def getMinutes(timestamp):
    #year, month, day, hour, minute, sec = getDate(timestamp)
    tm = timestamp.split(":")
    dte = datetime(year=1900, month=1, day=1,hour=int(tm[0]), minute=int(tm[1]), second=int(tm[2]))
    return dte.hour * 60 + dte.minute

#excel function
def getHours(timestamp):
    #year, month, day, hour, minute, sec = getDate(timestamp)
    tm = timestamp.split(":")
    dte = datetime(year=1900, month=1, day=1,hour=int(tm[0]), minute=int(tm[1]), second=int(tm[2]))
    return dte.hour + dte.minute / float(60) + dte.second / float(3600)

# a ueseful helper function for timing other functions
# example def saveNodes(root, client, delete):
#       getTime(saveNodes, "Nodes saved!", root, client, delete)
def getTime(func, statement, *args):
    start = time.time()
    returnValue = func(*args)
    print statement + ": " + str((time.time() - start))

    return returnValue

def toIso(sttime):
    date_object = datetime.strptime(sttime,'%H:%M:%S')
    return date_object.isoformat()

def xldate_to_datetime(xldate):
    temp = datetime(day=1, month=1, year=1900)
    delta = timedelta(days=xldate)
    x = temp+delta
    return x.isoformat()

def getExcelDate(date):
    dt = date.split('-')
    dt = datetime(day=int(dt[0]),month=int(dt[1]),year=int(dt[2]))
    temp = datetime(1899, 12, 30)
    delta = dt - temp
    return float(delta.days) + (float(delta.seconds) / 86400)

################ End Date Time Functions ################

################ Begin Distance Functions ################

def euclidean(node1, node2, dist=False):
    lat1 = node1['lat']
    lat2 = node2['lat']

    lon1 = node1['lon']
    lon2 = node2['lon']

    value = (lat1 - lat2) ** 2 + (lon1 - lon2) ** 2
    if dist:
        value = value ** .5

    return value

def getLat(point):
    if type(point) is not dict:
        return point.lat

    return point[LAT_KEY]

def getLon(point):
    if type(point) is not dict:
        return point.lon

    return point[LON_KEY]

# http://www.johndcook.com/python_longitude_latitude.html
# http://www.thetruckersreport.com/infographics/cost-of-trucking/
def findArc(point1, point2):
    point1Lat = getLat(point1)
    point2Lat = getLat(point2)
    point1Lon = getLon(point1)
    point2Lon = getLon(point2)

    if point1Lat == point2Lat and point1Lon == point2Lon:
        return 0

    degreesRadians = math.pi / 180.0

    phi1 = (90.0 - float(point1Lat)) * degreesRadians
    phi2 = (90.0 - float(point2Lat)) * degreesRadians

    theta1 = float(point1Lon) * degreesRadians
    theta2 = float(point2Lon) * degreesRadians

    cos = (math.sin(phi1) * math.sin(phi2) * math.cos(theta1 - theta2) + math.cos(phi1) * math.cos(phi2))
    #THIS IS A HACK. FORCING COS ANGLE TO BE B/W -1 AND 1
    #http://stackoverflow.com/questions/13637400/why-do-i-get-valueerror-math-domain-error
    cos = min(1,max(cos,-1))
    arc = math.acos(cos)

    return arc

def mileDist(point1, point2):
    arc = 0
    try:
        arc = findArc(point1, point2)
    except:
        print "Point 1: "  + str(point1)
        print "Point 2: " + str(point2)
    return arc * 3960

def kilDist(point1, point2):
    # print point1.lat, point2.lat
    # print "TESSTING"
    arc = 0
    try:
        arc = findArc(point1, point2)
    except:
        print "Point 1: "  + str(point1)
        print "Point 2: " + str(point2)
    return arc * 6373

# http://www.movable-type.co.uk/scripts/latlong.html
def meterDist(point1, point2):
    return kilDist(point1, point2) * 1000

################ End Distance Functions ################

################ Begin Grid Functions ################

def getMeters(coord):
    return coord * 10 ** 5

def getCoord(meters):
    return meters / float(10**5)

################ End Grid Functions ################

################ Begin Assorted Functions ###############

def notify(message=None):
    if message is not None:
        print message
        return

def addIfKey(struct, key, item):
    if struct.has_key(key):
        struct[key].append(item)
    else:
        struct[key] = [item]

def getIfKey(struct, key, default=None):
    if struct.has_key(key):
        return struct[key]
    return default

class positive_infinity:
    "bigger than any object other than itself"

    def __cmp__(self, other):
        if isinstance(other, self.__class__):
            return 0
        return 1

class negative_infinity:
    "smaller than any object other than itself"

    def __cmp__(self, other):
        if isinstance(other, self.__class__):
            return 0
        return -1

################ Begin Assorted Functions ###############
