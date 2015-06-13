import glob
import xlrd
from pymongo import MongoClient
from util import getTime
from chile import getPointsDb
from util import getDate, getDateNum


# http://www.youlikeprogramming.com/2012/03/examples-reading-excel-xls-documents-using-pythons-xlrd/
# https://secure.simplistix.co.uk/svn/xlrd/trunk/xlrd/doc/xlrd.html?p=4966
# http://stackoverflow.com/questions/3964681/find-all-files-in-directory-with-extension-txt-with-python

WORKSHEET_NAME = "Detalle"
LAT_KEY = "lat"
LON_KEY = "lon"
VELOCITY_KEY = "vel"
truckId_KEY = "truckId"
TIME_KEY = "time"
TEMP_KEY = "temp"
DRIVER_KEY = "driver"

TRUCKS_DB_KEY = "trucks"
POINTS_KEY = "points"
ID_MAPS_KEY = "idMaps"

DATE_NUM_KEY = "dateNum"
MONTH_NUM = 31


# year, month, day, hour, minute, second = xldate_as_tuple(timestamp, 0)

def getIDMapsDb(client):
    return client[TRUCKS_DB_KEY][ID_MAPS_KEY]


def saveData(items, truckId, client):
    db = getPointsDb(client)
    db.insert(items)


# _ids = [item['_id'] for item in items]
# idMap = {}
# idMap[truckId_KEY] = truckId
# idMap['values'] = _ids
# db = getIDMapsDb(client)
# if delete:
# 	db.remove()
# db.insert(idMap)
def formatCoord(coord):
    return coord / 10000000000.0


def createMongoItem(truckId, timestamp, velocity, lat, lon):
    truck = {}
    truck[truckId_KEY] = truckId
    truck[TIME_KEY] = timestamp
    truck[VELOCITY_KEY] = velocity
    truck[LAT_KEY] = formatCoord(lat)
    truck[LON_KEY] = formatCoord(lon)
    truck[DATE_NUM_KEY] = getDateNum(timestamp)

    return truck

def readData(filename, client):
    workbook = xlrd.open_workbook(filename)
    worksheet = workbook.sheet_by_name(WORKSHEET_NAME)
    num_rows = worksheet.nrows - 1
    num_cells = worksheet.ncols - 1
    curr_row = 1
    items = []
    while curr_row < num_rows:
        row = worksheet.row(curr_row)
        truckId, timestamp, velocity, lat, lon = [x.value for x in row]
        item = createMongoItem(truckId, timestamp, velocity, lat, lon)
        items.append(item)
        curr_row += 1

    saveData(items, truckId, client)

    return truckId

def pointsForMap(truckId, client):
    db = getIDMapsDb(client)
    item = db.find_one({"truckId": truckId})
    _ids = item['values']

    db = getPointsDb(client)
    db.find({"_id": {"$in": _ids}})

def pointsStraight(truckId, client):
    db = getPointsDb(client)
    db.find({"truckId": truckId})

def timeTest(truckId):
    client = MongoClient()
    # getTime(pointsForMap, "Got Points using Map!", truckId, client)
    getTime(pointsStraight, "Got Points straight!", truckId, client)

def importAll(delete=True):
    client = MongoClient()
    db = getPointsDb(client)
    if delete:
        db.remove()

    filenames = glob.glob("/data/chile/*/*.xlsx")
    for filename in filenames:
        truckId = readData(filename, client)
    
getTime(importAll, "import All!")




