# -*- coding: utf-8 -*-
import shingle_helper
from mongodb_middleware import mongodb_interface
from bs4 import BeautifulSoup
from web_discovery_aux import parsing_aux

def calculate_shingles_on_domain(domain,window):
    collection_name = parsing_aux.add_www_domain(str(domain))
    collection = mongodb_interface.get_collection(str(collection_name))
    print '-------------------'+str(collection) + ' --------'

    for post in collection.find():
        body = BeautifulSoup(post['html_raw_text'], 'html.parser')
        if body == None or body =='':
            print str(domain)+' NONE OR EMPTY'
            continue
        result = shingle_helper.compute_shingle_vector(body, window)
        mongodb_interface.update_document(str(collection_name), 'url_page', post['url_page'], 'shingle_vector', str(result))
    return 'shingled'


def generate_shingles(sc,shingle_window,save_rdd, output_path):
    product_sites_crawled = mongodb_interface.get_all_collections()
    if product_sites_crawled != None:
        for site in product_sites_crawled:
            calculate_shingles_on_domain(str(site),shingle_window)
    return 'done'

'''
    output = product_sites_crawled.filter(lambda (domain,values): values['crawling_status'] == 'crawled')\
        .map(lambda (domain,values): (domain, {'pages': values['pages'], 'shingle_status':calculate_shingles_on_domain(str(domain),shingle_window)}))

    if (save_rdd):
        if output_path != None and output_path != '':
            output.saveAsTextFile(output_path)

    return output
'''




