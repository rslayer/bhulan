from processStops import *
from init import *
import datetime
import requests

def sendtoCartodb(fileloc):
    files = {'file': open(fileloc)}
    r = requests.post(CARTO_URL+CARTO_DB_API_KEY,files=files)
    print r.text
    return '0'

# helper to save stops data to file temporarily
def saveStopPropsDataToFile(db=WATTS_DATA_DB_KEY):
    stops = StopProperties.getItems(db)

    with open('/Users/alikamil/Desktop/stoppropssall.csv','w') as wf:
        for i in stops.keys():
            prop = stops[i]
            items = [prop.id, prop.time]
            line = getLineForItems(items)
            wf.write(line)

def saveStopsToFile(datenum):
    filename = GPS_FILE_DIRECTORY+"santi_truckstops_"+str(datenum)+".csv"
    wf = open(filename,'w')
    trucklist = getTruckList().keys()
    wf.write("id,datenum,lat,lng,duration,time, truckid\n")
    ts = getDateByDatenum(datenum).split('-')

    for t in trucklist:
        stops = getStopPropsFromTruckDate(t, datenum)

        for s in stops:
            if inSantiago(s):
                tm = s.time.split(":")
                dt = datetime.datetime(year=int(ts[0]), month=int(ts[1]), day=int(ts[2]),
                                       hour=int(tm[0]), minute=int(tm[1]), second=int(tm[2]))
                date = dt.isoformat()#.strftime("%Y-%m-%d %H:%M:%S")
                ls = [s.id, date, s.lat, s.lon,s.duration, dt.isoformat(), s.truckId]
                line = getLineForItems(ls)
                wf.write(line)

    wf.close()

def saveTruckDateCombosToFile(truckId,datenum, db=WATTS_DATA_DB_KEY):
    cnt = 0
    dict = getStopsFromTruckDate(truckId,datenum)
    filename = GPS_FILE_DIRECTORY+truckId+"_"+str(datenum)+".csv"
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