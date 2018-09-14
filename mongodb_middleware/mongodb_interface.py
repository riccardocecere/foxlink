import json,random
import pymongo


def get_db():
    from pymongo import MongoClient
    client = MongoClient('172.17.0.3:27017')
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
    try:
        db = get_db()
        db[collection].update_one({id_name:id}, {'$set': {attribute_name: content}})
        return content
    except:
        return None

def get_all_collections():
    db = get_db()
    return db.list_collection_names()


# It returns the html_raw_text of a given page
def get_html_page(collection, url):
    try:
        db = get_db()
        page = db[collection].find_one({"url_page":str(url)},{"html_raw_text":1})
        return page['html_raw_text']
    except:
        return None


def get_random_html(collection):
    try:
        db = get_db()
        collection_size = db[collection].count()
        return db[collection].find()[random.randrange(collection_size)]['html_raw_text']
    except:
        return None
