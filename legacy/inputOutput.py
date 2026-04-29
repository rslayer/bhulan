from processStops import *
from init import *
import datetime
import requests
from util import toIso
import sys

def sendtoCartodb(fileloc):
    files = {'file': open(fileloc)}
    r = requests.post(CARTO_URL+CARTO_DB_API_KEY,files=files)
    print(r.text)
    return '0'

# Save stops to file for future processing
def saveStopsToFile(db=WATTS_DATA_DB_KEY):

    #need to change to utf-8 for encoding
    #reference: https://pythonadventures.wordpress.com/tag/ascii/
    reload(sys)
    sys.setdefaultencoding("utf-8")

    stops = Stop.getItems(db)

    with open(GPS_FILE_DIRECTORY+"stopsall.csv",'w') as wf:
        wf.write("id,lat,lng,address\n")
        for i in stops.keys():
            prop = stops[i]
            address = revGeoCode(prop.lat, prop.lon)
            items = [prop.id, prop.lat, prop.lon, address]
            line = getLineForItems(items)
            wf.write(line)

# Save stop properties to file for future processing
def saveStopPropsToFile(datenum):

    #https://pythonadventures.wordpress.com/tag/ascii/
    #need to change to utf-8 for encoding
    reload(sys)
    sys.setdefaultencoding("utf-8")

    filename = GPS_FILE_DIRECTORY+"santi_truckstops_"+str(datenum)+".csv"
    wf = open(filename,'w')
    trucklist = getTruckList().keys()
    wf.write("id,lat,lng,duration,time,truckid,address\n")
    ts = getDateByDatenum(datenum).split('-')

    for t in trucklist:
        stops = getStopPropsFromTruckDate(t, datenum)

        for s in stops:
            if inSantiago(s):
                tm = s.time.split(":")
                dt = datetime.datetime(year=int(ts[0]), month=int(ts[1]), day=int(ts[2]),
                                       hour=int(tm[0]), minute=int(tm[1]), second=int(tm[2]))
                ls = [s.id, s.lat, s.lon,s.duration, dt.isoformat(), s.truckId,s.address]
                line = getLineForItems(ls)
                wf.write(line)

    wf.close()

def saveTruckDateCombosToFile(truckId,datenum, db=WATTS_DATA_DB_KEY):
    cnt = 0
    dict = getStopsFromTruckDate(truckId,datenum)
    filename = GPS_FILE_DIRECTORY+truckId+"_"+str(datenum)+".csv"
    wf = open(filename,'w')
    wf.write("truckid,datenum,lat,lng,address,time,duration\n")
    print("total:", len(dict))
    for d in dict:
        x = dict[d]
        if inSantiago(x):
            #print getStopPropsFromStopId(x.id)
            ls = [truckId,datenum,x.lat, x.lon,x.address,toIso(x.time),x.duration]
            line = getLineForItems(ls)
            cnt +=1
            wf.write(line)

    wf.close()
