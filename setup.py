import sys
import traceback
from processVehicles import importTrucks, initCompute
from processStops import saveComputedStops
from util import notify, getTime
from pymongo import MongoClient
from constants import WATTS_DATA_DB_KEY
from inputOutput import saveStopsToFile

db = WATTS_DATA_DB_KEY

def trucks():
    importTrucks()

def compute():
    initCompute()

def stops():
    saveComputedStops()
    return 0

def run(func, args):
    messages = {
        trucks: "import trucks ",
        compute: "compute truck dates and centers ",
        stops: "compute stops and properties"
    }
    message = messages[func]

    try:
        getTime(func, message, *args)
        # func(*args)
        #notify(message)
    except:
        print traceback.format_exc()
        notify(message + "failed")


def setupAll():
    try:
        run(trucks, [])
        run(compute, [])
        run(stops,[])
        notify("complete setup succeeded!")
    except:
        print traceback.format_exc()
        notify("complete setup failed...")


##
# deletes the database and cleans up the collections
def dataPurge(db):
    client = MongoClient()
    client.drop_database(db)

if __name__ == '__main__':
    dataPurge(db)
    setupAll()

    if len(sys.argv) == 2:
        if sys.argv[1] == "all":
            getTime(setupAll, "Ran complete setup")
        if sys.argv[1] == "trucks":
            run(trucks, [])
        if sys.argv[1] == "stops":
            run(stops, [])
        if sys.argv[1] == "compute":
            run(compute, [])
