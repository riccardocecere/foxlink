import json,random
import pymongo


def get_db():
    from pymongo import MongoClient
    client = MongoClient('172.17.0.3:27017')
    db = client.ProductFinderCrawlerData
    return db


def put(domain,content):
    try:
        db = get_db()
        db[domain].insert(json.loads(content))
        return content
    except:
        return None


def get_collection(collection):
    try:
        db = get_db()
        return db[collection]
    except:
        return None


def update_document(collection, id_name, id, attribute_name, content):
    try:
        db = get_db()
        db[collection].update_one({id_name:id}, {'$set': {attribute_name: content}})
        return content
    except:
        return None

def get_all_collections():
    try:
        db = get_db()
        return db.list_collection_names()
    except:
        return None


# It returns the html_raw_text of a given page
def get_html_page(collection, url):
    try:
        db = get_db()
        page = db[collection].find_one({"url_page":str(url)},{"html_raw_text":1})
        return page['html_raw_text']
    except:
        return None

def get_referring_url(collection, url):
    try:
        db = get_db()
        page = db[collection].find_one({"url_page":str(url)},{"referring_url":1})
        return page['referring_url']
    except:
        return None

def get_depth_level(collection, url):
    try:
        db = get_db()
        page = db[collection].find_one({"url_page":str(url)},{"depth_level":1})
        return page['depth_level']
    except:
        return None

def get_random_html(collection):
    try:
        db = get_db()
        collection_size = db[collection].count()
        return db[collection].find()[random.randrange(collection_size)]['html_raw_text']
    except:
        return None

def drop_collection(collection):
    if collection != None and collection!='':
        try:
            db = get_db()
            db.drop_collection(collection)
        except:
            return None
    return None




'''

collections = get_all_collections()
for collection in collections:
    drop_collection(collection)
'''


