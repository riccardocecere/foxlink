# -*- coding: utf-8 -*-
import shingler
from mongodb_middleware import mongodb_interface
from bs4 import BeautifulSoup
from general_utils import text_parser


# Function that calculate shingle vectors for all pages of a given domain
def calculate_shingles_on_domain(domain,window):

    collection_name = text_parser.add_www_domain(str(domain))
    collection = mongodb_interface.get_collection(str(collection_name))

    print '-----------------------------------------------------------'
    print '-------------------'+str(collection)+'---------------------'
    print '-----------------------------------------------------------'

    for post in collection.find():
        if 'shingle_vector' in post:
            break
        body = BeautifulSoup(post['html_raw_text'], 'html.parser')
        if body == None or body =='':
            print str(domain)+' NONE OR EMPTY'
            continue
        result = shingler.compute_shingle_vector(body, window)
        mongodb_interface.update_document(str(collection_name), 'url_page', post['url_page'], 'shingle_vector', str(result))
    return None

# Function that generate the shingle vectors of all domains
def generate_shingles(shingle_window):

    product_sites_crawled = mongodb_interface.get_all_collections()

    if product_sites_crawled != None:
        for site in product_sites_crawled:
            calculate_shingles_on_domain(str(site),shingle_window)
    return None