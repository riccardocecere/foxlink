import json


def get_db():
    from pymongo import MongoClient
    client = MongoClient('172.17.0.2:27017')
    db = client.ProductFinderCrawlerData
    return db


def put(domain,content):
    db = get_db()
    db[domain].insert(json.loads(content))
    return content