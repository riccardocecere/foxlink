# -*- coding: utf-8 -*-
from general_utils import text_parser
from mongodb_middleware import mongodb_interface
from bs4 import BeautifulSoup
import crawler_utils,json,requests,foxlink_spider


#Fucntion to start and perform intrasite crawling outside a spark context
def intrasite_crawling_iterative(product_sites,depth_limit,download_delay,closesspider_pagecount,autothrottle_enable,autothrottle_target_concurrency):

    allowed_domains = []
    for site in product_sites:
        allowed_domain = str(text_parser.remove_www_domain(site))
        allowed_domains.append(allowed_domain)

    foxlink_spider.start_crawling(product_sites, allowed_domains, depth_limit, download_delay, closesspider_pagecount, autothrottle_enable, autothrottle_target_concurrency)
    insert_home_pages(mongodb_interface.get_all_collections())

    return product_sites


# Function to manage home pages in mongodb
def insert_home_pages(collections):
    if collections == None or collections == '' or collections == []:
        return None
    for collection in collections:
        if mongodb_interface.get_html_page(collection, collection) == None:
            try:
                response = requests.get(collection, timeout=15).text
                content = {
                    'url_page': collection,
                    'html_raw_text': str(BeautifulSoup(response, 'html.parser').body),
                    'page_relevant_links': str(list(set(crawler_utils.extract_relevant_links(response, text_parser.remove_www_domain(collection),
                                                                                             text_parser.add_www_domain(collection))))),
                    'depth_level': '1',
                    'referring_url': collection
                }
                content = json.dumps(content)
                mongodb_interface.put(collection, content)

            except:
                print 'error inserting home pages after crawling'
                continue

    return None