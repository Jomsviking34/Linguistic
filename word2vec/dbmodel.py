# -*- encoding: utf-8 -*-
from pymongo import MongoClient
from bson.objectid import ObjectId

# from motor.motor_asyncio import AsyncIOMotorClient
# # TODO: make async model | Нет.
# #uri = "mongodb://dev:dev@localhost:27017/mydatabase?authSource=admin"
# client = AsyncIOMotorClient(uri)


class Connection:
    uri = None
    conn = None

    def __init__(self):
        #вместо host вписать адрес монго БД
        #self.uri = f"mongodb://{host}:{port}"
        self.conn = MongoClient('localhost', 27017)

    def getConnection(self):
        return self.conn


class Mongo:
    mydb = None

    def __init__(self, connection, databaseName):
        self.mydb = connection[databaseName]

    def table(self, tableName: str):
        return self.mydb[tableName]

    def insert(self, tableName: str, data):
        mycol = self.mydb[tableName]
        print("Пишем в бд")
        insertedId = mycol.insert_many(data)
        return insertedId

    def selectAll(self, tableName: str):
        mycol = self.mydb[tableName]
        return list(mycol.find({}))

    def selectBy(self, tableName: str, fieldName: str, condition):
        mycol = self.mydb[tableName]
        return mycol.find_one({fieldName: ObjectId(condition)})
