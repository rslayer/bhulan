from pymongo import MongoClient
from constants import *
from util import getIfKey
import gridfs
import ast

client = MongoClient()

# we would make this method available in util or something like that
def getTbl(db, tblKey):
    return client[db][tblKey]

def getDb(db):
    return client[db]

class DBItem(object):
    @classmethod
    def getTbl(cls, db):
        return getTbl(db, cls.tblKey)

    @classmethod
    def deleteItems(cls, db):
        tbl = getTbl(db, cls.tblKey)
        tbl.remove()

        return True

    @classmethod
    def saveItem(cls, item, db):
        tbl = getTbl(db, cls.tblKey)
        tbl.save(item)

    @classmethod
    def saveItems(cls, items, db, delete=False):
        if delete:
            cls.deleteItems(db)

        tbl = getTbl(db, cls.tblKey)
        tbl.insert(items)

        return items

    @classmethod
    def findItem(cls, key, value, db):
        tbl = getTbl(db, cls.tblKey)
        item = tbl.find_one({key: value})
        if item is None:
            return None

        return cls(item, db)

    @classmethod
    def findItems(cls, key, value, db):
        tbl = getTbl(db, cls.tblKey)
        items = tbl.find({key: value})

        return {item[ID_KEY]: cls(item, db) for item in items}

    @classmethod
    def findItemList(cls, key, value, db):
        tbl = getTbl(db, cls.tblKey)
        items = tbl.find({key: value})

        return [cls(item, db) for item in items]

    @classmethod
    def find(cls, query, db):
        tbl = getTbl(db, cls.tblKey)
        items = tbl.find(query)

        return [cls(item, db) for item in items]

    @classmethod
    def findOne(cls, query, db):
        tbl = getTbl(db, cls.tblKey)
        item = tbl.find_one(query)
        if item is None:
            return None

        return cls(item, db)

    @classmethod
    def getItems(cls, db):
        tbl = getTbl(db, cls.tblKey)
        items = tbl.find()

        return {item[ID_KEY] : cls(item, db) for item in items}

    @classmethod
    def getItemList(cls, db):
        tbl = getTbl(db, cls.tblKey)
        items = tbl.find()

        return [cls(item, db) for item in items]

    @classmethod
    def getMongoItems(cls, db):
        tbl = getTbl(db, cls.tblKey)
        items = tbl.find()

        return {item[ID_KEY] : item for item in items}

    @classmethod
    def getMongoItemList(cls, db):
        tbl = getTbl(db, cls.tblKey)
        items = tbl.find({},{'_id': 0})

        return [item for item in items]



    __slots__ = [DB_KEY, DB_ID_KEY, ITEM_KEY]

    def __init__(self, item, db):
        self.db = db
        self.item = item
        self.dbId = getIfKey(item, MONGO_ID_KEY)

    def save(self, tblKey):
        tbl = getTbl(self.db, tblKey)
        tbl.save(self.item)

    def getItem(self):
        return self.item


def flushBigData(fileName, db=BIG_DATA_DB_KEY):
    db = getDb(db)
    fs = gridfs.GridFS(db)
    try:
        string = fs.get_last_version(fileName=fileName)
        fs.delete(string._id)
    except:
        return

def saveBigData(fileName, data, db=BIG_DATA_DB_KEY):
    db = getDb(db)
    fs = gridfs.GridFS(db)
    fs.put(str(data), fileName=fileName)

def getBigData(fileName, db=BIG_DATA_DB_KEY):
    db = getDb(db)
    fs = gridfs.GridFS(db)
    data = None
    print fileName
    if fs.exists(fileName=fileName):
        string = fs.get_last_version(fileName=fileName)
        # ooo = string.read()
        # print ooo
        # data = eval(ooo)
        data = ast.literal_eval(string.read())
    # except:
        # print traceback.format_exc()
        # data = None 

    return data

def saveBigItem(key, value, db, tblKey):
    item = {}
    item[KEY] = key
    item[VALUE_KEY] = value  

    try:
        tbl = getTbl(db, tblKey)
        tbl.insert(item)

    except:
        saveBigData(key, item, db)
        
def findMax(tbl, key):
    print key
    item = tbl.find().sort(key,-1).limit(1)
    print item
    # print item[0]
    # print item[0][key]
    return list(item)[0][key]
    # try:
    #     res = db.find().sort({key: -1}).limit(1)[0][key]
    # except TypeError as te:
    #     print te.message
    #     res = 0

    # return res

def findMin(tbl, key):
    print key
    item = tbl.find().sort(key,1).limit(1)
    print item
    # print item[0]
    # print item[0][key]
    # return item[0][key]
    return list(item)[0][key]
    # try:
    #     res = db.find().sort({key: 1}).limit(1)[0][key]
    # except TypeError as te:
    #     print te.message
    #     res = 0

    # return res

