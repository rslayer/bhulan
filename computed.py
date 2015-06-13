from classes import *
from util import addIfKey, getIfKey

class Computed:
    tblKey = COMPUTED_KEY

    def __init__(self):
        self.funcs = {
            MAX_LON_KEY: computeMaxLon,
            MAX_LAT_KEY: computeMaxLat,
            MIN_LON_KEY: computeMinLon,
            MIN_LAT_KEY: computeMinLat,
            MAX_MINS_KEY: computeMaxMins,
            GRID_INDEXES_KEY: computeGridIndexes,
            NODE_EDGES_KEY: computeNodeEdges,
            MAX_DEGREE_KEY: computeMaxOutDegree,
        }

    def getFunc(self, key):
        return self.funcs[key]

    def add(self, key, func):
        self.funcs[key] = func

    def delete(self, key, db):
        tbl = getTbl(db, Computed.tblKey)
        tbl.remove({KEY: key})
        flushBigData(key, db)

    def save(self, key, value, db, delete=True):
        if delete:
            self.delete(key, db)

        saveBigItem(key, value, db, Computed.tblKey)

    def get(self, key, db, delete=False):
        if delete:
            self.delete(key, db)

        tbl = getTbl(db, Computed.tblKey)
        item = tbl.find_one({KEY: key}) or getBigData(key, db)
        if item is None:
            value = self.getFunc(key)(db)            
            saveBigItem(key, value, db, Computed.tblKey)
            item = { VALUE_KEY : value }
                


        return getIfKey(item, VALUE_KEY, item)


################# Begin Computed Funcs #######################

def computeMaxLat(db):
    tbl = Node.getTbl(db)
    maxLat = findMax(tbl, LAT_KEY)

    return maxLat

def computeMinLat(db):
    tbl = Node.getTbl(db)
    minLat = findMin(tbl, LAT_KEY)

    return minLat

def computeMaxLon(db):
    tbl = Node.getTbl(db)
    maxLon = findMax(tbl, LON_KEY)

    return maxLon

def computeMinLon(db):
    tbl = Node.getTbl(db)
    minLon = findMin(tbl, LON_KEY)

    return minLon

def computeNodeEdges(db):
    edges = Edge.getMongoItems(db)
    nodeEdges = {}

    for edgeId in edges:
        edge = edges[edgeId]
        startNodeId = edge[START_NODE_KEY]
        endNodeId = edge[END_NODE_KEY]
        addIfKey(nodeEdges, startNodeId, edgeId)
        addIfKey(nodeEdges, endNodeId, edgeId)

    return nodeEdges

def computeMaxMins(db):
    computed = Computed()
    minLat = computed.get(MIN_LAT_KEY, db)
    minLon = computed.get(MIN_LON_KEY, db)
    maxLat = computed.get(MAX_LAT_KEY, db)
    maxLon = computed.get(MAX_LON_KEY, db)

    return minLat, minLon, maxLat, maxLon

def computeGridIndexes(db):
    computed = Computed()
    minLat, minLon, maxLat, maxLon = computed.get(MAX_MINS_KEY, db)
    grid = Grid(minLat, minLon, maxLat, maxLon)
    nodes = Node.getItems(db)
    gridIndex = {}

    for key in nodes:
        node = nodes[key]
        lat = node.lat
        lon = node.lon

        cellID = str(grid.getCellID(lat, lon))
        addIfKey(gridIndex, cellID, key)

    return gridIndex

def computeMaxOutDegree(db):
    nodes = Node.getItemList(db)
    return max([len(node.forNeighbors) for node in nodes])  

# def computeGraphWidth(db):
#     computed = Computed()
#     minLon = getMeters(computed.get(MIN_LON_KEY, db))
#     maxLon = getMeters(computed.get(MAX_LON_KEY, db))

#     return int(math.ceil(maxLon - minLon)) / CELL_SIZE


################# End Computed Funcs #######################







