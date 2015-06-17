from classes import *
from util import getTime, addIfKey
import ast
from init import *

import requests

def sendtoCartodb(fileloc):
    files = {'file': open(fileloc)}
    r = requests.post(CARTO_URL+CARTO_DB_API_KEY,files=files)
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




