from classes import *
from util import getTime, addIfKey
import ast

import requests

API_KEY = "0938198967c61b688987b732da2cb47854c825c4"
URL = "https://alikamil.cartodb.com/api/v1/imports/?api_key="


def sendtoCartodb(fileloc):
    files = {'file': open(fileloc)}
    r = requests.post(URL+API_KEY,files=files)
    print r.text
    return '0'

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


if __name__ == '__main__':
    print 'text'



