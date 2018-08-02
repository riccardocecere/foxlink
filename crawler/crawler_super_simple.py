# -*- coding: utf-8 -*-
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
import crawler_html_parser, json
from mongodb_middleware import mongodb_interface
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from web_discovery_aux import parsing_aux


class ProductFinderSpider(CrawlSpider):

    name = 'spider'

    rules = (
        Rule(LinkExtractor(), callback='parse_item', follow=True),
    )

    def parse_item(self, response):
        #item_name = response.url.split("/")[-2] + '.html'
        domain = parsing_aux.find_domain_url(response.url)
        relevant_links = crawler_html_parser.extract_relevant_links(response.body, parsing_aux.remove_www_domain(domain))
        content = {'url_page': str(response.url),
                   'html_raw_text': str(response.body),
                   'page_relevant_links': str(relevant_links)}
        content = json.dumps(content)
        domain = parsing_aux.add_www_domain(domain)
        mongodb_interface.put(domain,content)

#execute('scrapy runspider crawler_super_simple.py'.split())

process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0'
})

# Function for starting the crawler, it takes several parameters for the settings
def start_crawling(start_urls, allowed_domains,depth_limit,download_delay, closespider_pagecount, autothrottle_enable, autothrottle_target_concurrency):

    #Check if the donwload delay is at a minimum of 0.4 sec
    if download_delay == None or download_delay<0.4:
        download_delay = 0.4

    custom_settings = get_project_settings()
    custom_settings.update({
        'DEPTH_LIMIT': str(depth_limit),
        'DOWNLOAD_DELAY': str(download_delay),
        'CLOSESPIDER_PAGECOUNT': str(closespider_pagecount),
        'AUTOTHROTTLE_ENABLED': str(autothrottle_enable),
        'AUTOTHROTTLE_TARGET_CONCURRENCY': str(autothrottle_target_concurrency)
    })
    process = CrawlerProcess(custom_settings)
    process.crawl(ProductFinderSpider, start_urls=start_urls,allowed_domains=allowed_domains)
    process.start()


start_urls = ['https://www.unieuro.it/online/','http://egyp.it']
allowed_domains = ['unieuro.it','egyp.it']
depth_limit = 7
download_delay = 0.4
closesspider_pagecount = 200
autothrottle_enable = True
autothrottle_target_concurrency = 3

start_crawling(start_urls,allowed_domains,depth_limit,download_delay,closesspider_pagecount,autothrottle_enable,autothrottle_target_concurrency)

