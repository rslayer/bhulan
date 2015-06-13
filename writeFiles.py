from classes import *
from util import getTime, addIfKey
import ast


def getLineForItems(items):
    length = len(items)
    line = ""
    for i in range(length):
        item = items[i]
        line += str(item)
        if i != length - 1:
            line += ","
        else:
            line += "\n"
    return line


def writeNodes(db):
    # fileName = "/home/ubuntu/mapData/%s/nodes" % db
    fileName = "/Users/alikamil/Desktop/crowdSOS-v2.0/mapData/%s/nodes" % db
    f = open(fileName, "w")

    nodes = Node.getItemList(db)
    for node in nodes:
        items = [node.id, node.lat, node.lon]
        line = getLineForItems(items)
        f.write(line)

    f.close()


def writeEdges(db):
    #fileName = "/home/ubuntu/mapData/%s/edges" % db
    fileName = "/Users/alikamil/Desktop/crowdSOS-v2.0/mapData/%s/edges" % db
    f = open(fileName, "w")
    #id, cost, startNodeId, endNodeId, startLat, startLon, endLat, endLon
    edges = Edge.getItemList(db)
    for edge in edges:
        items = [edge.id,edge.cost, edge.startNodeId,edge.endNodeId,edge.startPoint.lat,
                 edge.startPoint.lon,edge.endPoint.lat, edge.endPoint.lon]
        line = getLineForItems(items)
        f.write(line)

    f.close()


def readNodes(db):
    #fileName = "/home/ubuntu/mapData/%s/nodes" % db
    fileName = "/Users/alikamil/Desktop/crowdSOS-v2.0/mapData/%s/nodes" % db
    f = open(fileName, "r")

    nodes = {}
    for line in f.readlines():
        # id, lat, lon = ast.literal_eval(line)
        id, lat, lon = eval(line)
        node = Node({}, db)
        node.id = id
        node.lat = lat
        node.lon = lon
        nodes[id] = node

    return nodes


def readGraph(db):
    #fileName = "/home/ubuntu/mapData/%s/edges" % db
    fileName = "/Users/alikamil/Desktop/crowdSOS-v2.0/mapData/%s/edges" % db
    f = open(fileName, 'r')
    graph = {}
    edges = {}

    for line in f.readlines():
        edge = Edge({}, db)
        id, cost, startNodeId, endNodeId, startLat, startLon, endLat, endLon = eval(line)

        edge.id = id
        edge.cost = cost
        edge.startNodeId = startNodeId
        edge.endNodeId = endNodeId
        edge.startPoint = Point(startLat, startLon)
        edge.endPoint = Point(endLat, endLon)

        addIfKey(graph, startNodeId, edge)
        edges[id] = edge

    return graph, edges


if __name__ == '__main__':
    db = 'chileMap'
    # getTime(writeNodes, "wrote nodes to file", db)
    #getTime(writeEdges, "wrote edges to file", db)
    #nodes = getTime(readNodes, "read nodes from file", db)
    graph, edges = getTime(readGraph, "read graph from file", db)
    #print nodes
    #print graph
    #print edges



