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


class Path:
    def __init__(self, edge, input, prob):
        self.edges = [edge]
        self.results = {}
        self.results[input.time] = edge
        self.prob = prob

    def __str__(self):
        return str({ 'prob' : self.prob, 'path' : self.results })

    def __repr__(self):
        return str({ 'prob' : self.prob, 'path' : self.results })

    def add(self, edge, input, prob):
        self.edges.append(edge)
        self.results[input.time] = edge
        self.prob = prob

    def getEdge(self, time):
        return self.results[time]

class Edge(DBItem):
    tblKey = EDGES_KEY

    __slots__ = [ID_KEY, COST_KEY, START_NODE_KEY, START_LAT_KEY, START_LON_KEY,
                 START_POINT_KEY, END_NODE_KEY, END_LAT_KEY, END_LON_KEY, END_POINT_KEY]

    # def __repr__(self):
    #     return "Edge~" + str(self.id)

    def __init__(self, item, db):
        DBItem.__init__(self, item, db)

        if isNotEmpty(item):
            self.id = item[ID_KEY]
            self.cost = getIfKey(item, COST_KEY)
            self.startNodeId = item[START_NODE_KEY]
            self.endNodeId = item[END_NODE_KEY]
            self.startPoint = Point(item[START_LAT_KEY], item[START_LON_KEY])
            self.endPoint = Point(item[END_LAT_KEY], item[END_LON_KEY])

    def save(self):
        self.item[ID_KEY] = self.id
        self.item[COST_KEY] = self.cost
        self.item[START_NODE_KEY] = self.startNodeId
        self.item[END_NODE_KEY] = self.endNodeId
        super(Edge, self).save(Edge.tblKey)

    @classmethod
    def getGraph(cls, db):
        tbl = getTbl(db, cls.tblKey)
        # items = tbl.find(sort=[( '$natural', 1 )] )

        items = getTime(tbl.find, "got items")
        graph = {}

        for item in items:
            edge = Edge(item, db)
            addIfKey(graph, edge.startNodeId, edge)

        return graph

        # return graph

class MiniEdge(DBItem):
    tblKey = MINI_EDGES_KEY

    __slots__ = [ID_KEY, EDGE_ID_KEY, START_NODE_KEY, LENGTH_KEY, END_LON_KEY,
                 END_NODE_KEY, START_LAT_KEY, START_LON_KEY, END_LAT_KEY,
                 START_POINT_KEY, END_POINT_KEY, REMAINING_KEY]

    def __init__(self, item, db):
        DBItem.__init__(self, item, db)
        if isNotEmpty(item):
            self.id = item[ID_KEY]
            self.startNodeId = item[START_NODE_KEY]
            self.endNodeId = item[END_NODE_KEY]
            self.startLat = item[START_LAT_KEY]
            self.startLon = item[START_LON_KEY]
            self.endLat = item[END_LAT_KEY]
            self.endLon = item[END_LON_KEY]
            self.length = item[LENGTH_KEY]
            self.edgeId = item[EDGE_ID_KEY]
            self.remaining = getIfKey(item, REMAINING_KEY)
            self.startPoint = Point(item[START_LAT_KEY], item[START_LON_KEY])
            self.endPoint = Point(item[END_LAT_KEY], item[END_LON_KEY])

    def save(self):
        self.item[ID_KEY] = self.id
        self.item[START_NODE_KEY] = self.startNodeId
        self.item[END_NODE_KEY] = self.endNodeId
        self.item[START_LAT_KEY] = self.startLat
        self.item[START_LON_KEY] = self.startLon
        self.item[END_LAT_KEY] = self.endLat
        self.item[END_LON_KEY] = self.endLon
        self.item[LENGTH_KEY] = self.length
        self.item[EDGE_ID_KEY] = self.edgeId
        super(MiniEdge, self).save(MiniEdge.tblKey)

    @classmethod
    def getEdgeNetwork(cls, db):
        tbl = getTbl(db, cls.tblKey)
        items = tbl.find()
        network = {}

        for item in items:
            miniEdge = MiniEdge(item, db)
            addIfKey(network, miniEdge.edgeId, miniEdge)

        return network


class Node(DBItem):
    tblKey = NODES_KEY

    __slots__ = [ID_KEY, LAT_KEY, LON_KEY, FOR_NEIGHBORS_KEY, BACK_NEIGHBORS_KEY,
                 PRIORITY_KEY, MARKED_KEY, VISITED_KEY, HEAP_ID_KEY, DISTANCE_KEY, POINT_KEY]

    def __init__(self, item, db):
        DBItem.__init__(self, item, db)

        if isNotEmpty(item):
            self.id = item[ID_KEY]
            self.lat = item[LAT_KEY]
            self.lon = item[LON_KEY]
            self.forNeighbors = getIfKey(item, FOR_NEIGHBORS_KEY, [])
            self.backNeighbors = getIfKey(item, BACK_NEIGHBORS_KEY, [])
            self.point = Point(self.lat, self.lon)

        self.marked = False
        self.visited = False

    def save(self):
        self.item[ID_KEY] = self.id
        self.item[LAT_KEY] = self.lat
        self.item[LON_KEY] = self.lon
        self.item[FOR_NEIGHBORS_KEY] = self.forNeighbors
        self.item[BACK_NEIGHBORS_KEY] = self.backNeighbors
        super(Node, self).save(Node.tblKey)

    def getLatLon(self):
        return (self.lat, self.lon)


class TestEdge(Edge):
    tblKey = TEST_EDGES_KEY

    __slots__ = [DESCRIPTION_KEY]

class TestNode(Node):
    tblKey = TEST_NODES_KEY

    __slots__ = [STATE_KEY, DESCRIPTION_KEY]

    def __repr__(self):
        """Convert to string for printing."""
        return "Node(%s, %s, '%s', '%s')" % (self.lon, self.lat,
                                             self.state, self.description)

class TruckPoint(DBItem):
    tblKey = TRUCK_POINTS_KEY

    __slots__ = [TRUCK_ID_KEY, TIME_KEY, VELOCITY_KEY, LAT_KEY, LON_KEY, DATE_NUM_KEY, POINT_KEY, DRIVER_KEY,
                 TEMPERATURE_KEY, DIRECTION_KEY, PATENT_KEY, CAPACITY_KEY, COMMUNE_KEY]


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
        self.point = Point(self.lat, self.lon)


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
    super(Point, self).save(Point.tblKey)

class Truck(DBItem):
    tblKey = TRUCKS_KEY

    __slots__ = [ID_KEY, TIME_KEY, VELOCITY_KEY, DATE_NUM_KEY, DRIVER_KEY,
                 PATENT_KEY, CAPACITY_KEY]

    def __init__(self, item, db):
        DBItem.__init__(self, item, db)
        print 'in constructor'
        print item
        self.id = item[ID_KEY]
        self.time = item[TIME_KEY]
        self.velocity = item[VELOCITY_KEY]
        self.dateNum = item[DATE_NUM_KEY]
        self.driver  = item[DRIVER_KEY]
        self.capacity = item[CAPACITY_KEY]
        self.patent = item[PATENT_KEY]

    def save(self):
        self.item[ID_KEY] = self.id
        self.item[TIME_KEY] = self.time
        self.item[VELOCITY_KEY] = self.velocity
        self.item[DATE_NUM_KEY] = self.dateNum
        self.item[DRIVER_KEY] = self.driver
        self.item[PATENT_KEY] = self.patent
        self.item[CAPACITY_KEY] = self.capacity
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

    __slots__ = [ID_KEY, STOP_PROP_ID_KEY, DATE_NUM_KEY, LAT_KEY, LON_KEY, DURATION_KEY, TIME_KEY, RADIUS_KEY, TRUCK_ID_KEY]

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

class MiniNode(DBItem):
    tblKey = MINI_NODES_KEY

    __slots__ = [ID_KEY, LAT_KEY, LON_KEY, EDGE_ID_KEY, CELL_ID_KEY]

    def __init__(self, item, db):
        DBItem.__init__(self, item, db)
        self.id = item[ID_KEY]
        self.lat = item[LAT_KEY]
        self.lon = item[LON_KEY]
        self.edgeId = item[EDGE_ID_KEY]
        self.cellId = getIfKey(item, CELL_ID_KEY)

    def save(self):
        self.item[ID_KEY] = self.id
        self.item[LAT_KEY] = self.lat
        self.item[LON_KEY] = self.lon
        self.item[EDGE_ID_KEY] = self.edgeId
        self.item[CELL_ID_KEY] = self.cellId
        super(MiniNode, self).save(MiniNode.tblKey)

class Cell(DBItem):
    tblKey = CELLS_KEY

    __slots__ = [ID_KEY, MINI_EDGES_KEY]

    def __init__(self, item, db):
        DBItem.__init__(self, item, db)
        self.id = item[ID_KEY]
        self.miniEdges = item[MINI_EDGES_KEY]

    def save(self):
        self.item[ID_KEY] = self.id
        self.item[MINI_EDGES_KEY] = self.miniEdges
        super(Cell, self).save(Cell.tblKey)

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
