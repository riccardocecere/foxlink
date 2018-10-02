# -*- coding: utf-8 -*-
import product_finder_spider
from web_discovery_aux import parsing_aux
from mongodb_middleware import mongodb_interface
import requests
from bs4 import BeautifulSoup
import crawler_html_parser
import json

def insert_home_pages(collections):
    if collections == None or collections == '' or collections ==  []:
        return None
    for collection in collections:
        if mongodb_interface.get_html_page(collection,collection) == None:
            try:
                response = requests.get(collection,timeout=15).text
                content = {
                    'url_page': collection,
                    'html_raw_text': str(BeautifulSoup(response, 'html.parser').body),
                    'page_relevant_links': str(list(set(crawler_html_parser.extract_relevant_links(response,parsing_aux.remove_www_domain(collection),parsing_aux.add_www_domain(collection))))),
                    'depth_level': '1',
                    'referring_url': collection
                }
                content = json.dumps(content)
                mongodb_interface.put(collection,content)

            except:
                print 'errore'
                continue
    return None


def intrasite_crawling(sc,product_sites,depth_limit,download_delay,closesspider_pagecount,autothrottle_enable,autothrottle_target_concurrency,save_rdd,output_path):

    output = product_sites.map(lambda (domain,values): (domain, {'pages': values['pages'], 'crawling_status': product_finder_spider.start_crawling([str(domain)], [str(parsing_aux.remove_www_domain(domain))], depth_limit, download_delay, closesspider_pagecount, autothrottle_enable, autothrottle_target_concurrency)}))

    if (save_rdd):
        if output_path != None and output_path != '':
            output.saveAsTextFile(output_path)

    return output



def intrasite_crawling_iterative(product_sites,depth_limit,download_delay,closesspider_pagecount,autothrottle_enable,autothrottle_target_concurrency,save_rdd,output_path):

    #output = product_sites.map(lambda (domain,values): (domain, {'pages': values['pages'], 'crawling_status': product_finder_spider.start_crawling([str(domain)], [str(parsing_aux.remove_www_domain(domain))], depth_limit, download_delay, closesspider_pagecount, autothrottle_enable, autothrottle_target_concurrency)}))
    allowed_domains = []
    for site in product_sites:
        allowed_domain = str(parsing_aux.remove_www_domain(site))
        allowed_domains.append(allowed_domain)
    product_finder_spider.start_crawling(product_sites, allowed_domains, depth_limit,download_delay, closesspider_pagecount, autothrottle_enable,autothrottle_target_concurrency)

    insert_home_pages(mongodb_interface.get_all_collections())
    #Verificare
    return product_sites
