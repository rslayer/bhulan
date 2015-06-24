from mongo import *
from util import getMeters, addIfKey, getTime

def isNotEmpty(item):
    return len(item)

class Point:
    def __init__(self, lat, lon):
        self.lat = lat
        self.lon = lon
        self.mLat = getMeters(lat)
        self.mLon = getMeters(lon)

    def getLatLon(self):
        return (self.lat, self.lon)

    def getItem(self):
        return { LAT_KEY : self.lat, LON_KEY : self.lon }

    def __str__(self):
        return str(self.getItem())

    def __repr__(self):
        return str(self.getItem())

class TruckPoint(DBItem):
    tblKey = TRUCK_POINTS_KEY

    __slots__ = [TRUCK_ID_KEY, TIME_KEY, VELOCITY_KEY, LAT_KEY, LON_KEY, DATE_NUM_KEY, POINT_KEY, DRIVER_KEY,
                 TEMPERATURE_KEY, DIRECTION_KEY, PATENT_KEY, CAPACITY_KEY, COMMUNE_KEY, TIMESTAMP_KEY]


    def __init__(self, item, db):
        DBItem.__init__(self, item, db)
        self.truckId = item[TRUCK_ID_KEY]
        self.time = item[TIME_KEY]
        self.velocity = item[VELOCITY_KEY]
        self.lat = item[LAT_KEY]
        self.lon = item[LON_KEY]
        self.dateNum = item[DATE_NUM_KEY]
        self.temperature = item[TEMPERATURE_KEY]
        self.direction = item[DIRECTION_KEY]
        self.patent = item[PATENT_KEY]
        self.commune = item[COMMUNE_KEY]
        self.timestamp = item[TIMESTAMP_KEY]
        self.point = Point(self.lat, self.lon)

    def save(self):
        self.item[TRUCK_ID_KEY] = self.truckId
        self.item[TIME_KEY] = self.time
        self.item[VELOCITY_KEY] = self.velocity
        self.item[LAT_KEY] = self.lat
        self.item[LON_KEY] = self.lon
        self.item[DATE_NUM_KEY] = self.dateNum
        self.item[TEMPERATURE_KEY] = self.temperature
        self.item[DIRECTION_KEY] = self.direction
        self.item[COMMUNE_KEY] = self.commune
        self.item[PATENT_KEY] = self.patent
        self.item[POINT_KEY] = self.point
        self.item[TIMESTAMP_KEY] = self.timestamp
        super(TruckPoint, self).save(TruckPoint.tblKey)


def getLatLon(self):
    return self.point.getLatLon()

def save(self):
    self.item[TRUCK_ID_KEY] = self.truckId
    self.item[TIME_KEY] = self.time
    self.item[VELOCITY_KEY] = self.velocity
    self.item[LAT_KEY] = self.lat
    self.item[LON_KEY] = self.lon
    self.item[DATE_NUM_KEY] = self.dateNum
    self.item[TEMPERATURE_KEY] = self.temperature
    self.item[DIRECTION_KEY] = self.direction
    self.item[PATENT_KEY] = self.patent
    self.item[COMMUNE_KEY] = self.commune
    self.item[TIMESTAMP_KEY] = self.timestamp
    super(Point, self).save(Point.tblKey)

class Truck(DBItem):
    tblKey = TRUCKS_KEY

    __slots__ = [ID_KEY, TIME_KEY, VELOCITY_KEY, DATE_NUM_KEY,
                 PATENT_KEY, TIMESTAMP_KEY]

    def __init__(self, item, db):
        DBItem.__init__(self, item, db)
        self.id = item[ID_KEY]
        self.time = item[TIME_KEY]
        self.velocity = item[VELOCITY_KEY]
        self.dateNum = item[DATE_NUM_KEY]
        self.patent = item[PATENT_KEY]
        self.timestamp = item[TIMESTAMP_KEY]

    def save(self):
        self.item[ID_KEY] = self.id
        self.item[TIME_KEY] = self.time
        self.item[VELOCITY_KEY] = self.velocity
        self.item[DATE_NUM_KEY] = self.dateNum
        self.item[PATENT_KEY] = self.patent
        self.item[TIMESTAMP_KEY] = self.timestamp
        super(Truck, self).save(Truck.tblKey)

class TruckDates(DBItem):
    tblKey = TRUCK_DATES_KEY

    __slots__ = [AVAILABILITY_KEY, DATE_NUM_KEY, ROUTE_CENTERS_KEY]

    def __init__(self, item, db):
        DBItem.__init__(self, item, db)
        self.availability = item[AVAILABILITY_KEY]
        self.dateNum = item[DATE_NUM_KEY]
        self.routeCenters = item[ROUTE_CENTERS_KEY]

    def save(self):
        self.item[AVAILABILITY_KEY] = self.availability
        self.item[DATE_NUM_KEY] = self.dateNum
        self.item[ROUTE_CENTERS_KEY] = self.routeCenters
        super(TruckDates, self).save(TruckDates.tblKey)

class Stop(DBItem):
    tblKey = STOPS_KEY

    __slots__ = [ID_KEY, LAT_KEY, LON_KEY]

    def __init__(self, item, db):
        DBItem.__init__(self, item, db)
        self.id = item[ID_KEY]
        self.lat = item[LAT_KEY]
        self.lon = item[LON_KEY]

    def save(self):
        self.item[ID_KEY] = self.id
        self.item[LAT_KEY] = self.lat
        self.item[LON_KEY] = self.lon
        super(Stop, self).save(Stop.tblKey)

class StopProperties(DBItem):
    tblKey = STOP_PROPS_KEY

    __slots__ = [ID_KEY, STOP_PROP_ID_KEY, DATE_NUM_KEY, LAT_KEY, LON_KEY, DURATION_KEY,
                 TIME_KEY, RADIUS_KEY, TRUCK_ID_KEY]

    def __init__(self, item, db):
        DBItem.__init__(self, item, db)
        self.id = item[ID_KEY]
        self.dateNum = item[DATE_NUM_KEY]
        self.lat = item[LAT_KEY]
        self.lon = item[LON_KEY]
        self.duration = item[DURATION_KEY]
        self.time = item[TIME_KEY]
        self.radius = item[RADIUS_KEY]
        self.truckId = item[TRUCK_ID_KEY]
        self.stopPropId = item[STOP_PROP_ID_KEY]

    def save(self):
        self.item[ID_KEY] = self.id
        self.item[DATE_NUM_KEY] = self.dateNum
        self.item[LAT_KEY] = self.lat
        self.item[LON_KEY] = self.lon
        self.item[DURATION_KEY] = self.duration
        self.item[TIME_KEY] = self.time
        self.item[RADIUS_KEY] = self.radius
        self.item[TRUCK_ID_KEY] = self.truckId
        self.item[STOP_PROP_ID_KEY] = self.stopPropId
        super(StopProperties, self).save(StopProperties.tblKey)

class Input(DBItem):
    tblKey = INPUT_KEY

    __slots__ = [TIME_KEY, LAT_KEY, LON_KEY, FILE_NUM_KEY, M_LAT_KEY, M_LON_KEY]

    def __repr__(self):
        item = {}
        item[TIME_KEY] = self.time
        item[LAT_KEY] = self.lat
        item[LON_KEY] = self.lon

        return str(item)

    def __str__(self):
        item = {}
        item[TIME_KEY] = self.time
        item[LAT_KEY] = self.lat
        item[LON_KEY] = self.lon

        return str(item)

    def __init__(self, item, db):
        DBItem.__init__(self, item, db)
        self.time = item[TIME_KEY]
        self.lat = item[LAT_KEY]
        self.lon = item[LON_KEY]
        self.mLat = getMeters(self.lat)
        self.mLon = getMeters(self.lon)
        self.fileNum = item[FILE_NUM_KEY]

    def save(self):
        self.item[TIME_KEY] = self.time
        self.item[LAT_KEY] = self.lat
        self.item[LON_KEY] = self.lon
        self.item[FILE_NUM_KEY] = self.fileNum
        super(Input, self).save(Input.tblKey)


class Candidate(DBItem):
    tblKey = CANDIDATE_KEY

    __slots__ = [EDGES_KEY, INPUT_ID_KEY]

    def __init__(self, item, db):
        DBItem.__init__(self, item, db)
        edges = item[EDGES_KEY]
        self.edges = [Edge(edge, db) for edge in edges]
        self.inputId = item[INPUT_ID_KEY]

class Output(DBItem):
    tblKey = OUTPUT_KEY

    __slots__ = [TIME_KEY, EDGE_ID_KEY, CONF_KEY, FILE_NUM_KEY]

    def __init__(self, item, db):
        DBItem.__init__(self, item, db)
        self.time = item[TIME_KEY]
        self.edgeId = item[EDGE_ID_KEY]
        self.conf = item[CONF_KEY]
        self.fileNum = item[FILE_NUM_KEY]

    def save(self):
        self.item[TIME_KEY] = self.time
        self.item[EDGE_ID_KEY] = self.edgeId
        self.self.item[CONF_KEY] = self.conf
        self.item[FILE_NUM_KEY] = self.fileNum
        super(Output, self).save(Output.tblKey)
