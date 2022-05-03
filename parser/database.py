import requests
import lxml
import pymongo
from bs4 import BeautifulSoup
from pymongo import MongoClient

client = MongoClient('localhost', 27017)

db = client['News']
series_collection = db.collection

# Тестовые данные
new_data = {
    "_id": 1,
    "title": "ff",
    "date": "22313",
    "link": "fdsd",
    "text": "fdsd",
    "cmts": 3,
    "people": '',
    "places": ''
}


def find_document(collection, elements, multiple=False):
    if multiple:
        results = collection.find(elements)
        return [r for r in results]
    else:
        return collection.find_one(elements)


def insert_document(collection, data):
    return collection.insert_one(data).inserted_id


def update_document(collection, query_elements, new_values):
    collection.update_one(query_elements, {'$set': new_values})


def delete_document(collection, query):
    collection.delete_one(query)
