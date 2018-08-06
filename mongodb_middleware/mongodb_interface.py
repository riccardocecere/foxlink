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


def get_collection(collection):
    db = get_db()
    return db[collection]


def update_document(collection, id_name, id, attribute_name, content):
    db = get_db()
    db[collection].update_one({id_name:id}, {'$set': {attribute_name: content}})
    return content

def get_all_collections():
    db = get_db()
    return db.list_collection_names()