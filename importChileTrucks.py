from mongo import *
from classes import TruckPoint, Truck
from init import *
import glob
import xlrd
from util import getDate, getDateNum, getDateForChile, getDateNumForChile, throwException

################# Begin Core Functions #######################

def formatCoord(coord):
    return coord / 10000000000.0

def createMongoItem(code, patent, timestamp, lat, lon, direction, commune, velocity, temperature, driver, capacity):
    truck = {}
    truck[TRUCK_ID_KEY] = code
    truck[TIME_KEY] = str(getDateForChile(timestamp).time())
    truck[VELOCITY_KEY] = velocity
    truck[LAT_KEY] = lat
    truck[LON_KEY] = lon
    truck[DATE_NUM_KEY] = getDateNumForChile(getDateForChile(timestamp))
    truck[PATENT_KEY] = patent
    truck[DIRECTION_KEY] = direction #has to be formatted
    truck[TEMPERATURE_KEY] = temperature
    truck[DRIVER_KEY] = driver #has to be formatted
    truck[COMMUNE_KEY] = commune
    truck[CAPACITY_KEY] = capacity

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
        code, patent, timestamp, lat, lon, direction, commune, velocity, temperature, driver, capacity = [x.value for x in row]
        item = createMongoItem(code, patent, timestamp, lat, lon, direction, commune, velocity, temperature, driver, capacity)
        items.append(item)
        curr_row += 1
        item = []

    TruckPoint.saveItems(items, db)

################# End Helper Functions #################

################# Begin Core Functions #################

def pointsStraight(truckId, db):
    return TruckPoint.findItem(TRUCK_ID_KEY, truckId, db)

def importAll(db=WATTS_DATA_DB_KEY, delete=True):
    TruckPoint.deleteItems(db)
    if delete:
        TruckPoint.deleteItems(db)

    filenames = glob.glob(GPS_FILE_DIRECTORY+GPS_FILE_EXTENSION)
    for filename in filenames:
        readData(filename, db)

################# End Core Functions #################

if __name__ == '__main__':
    print "### DONE ###"
