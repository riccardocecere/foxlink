# -*- coding: utf-8 -*-
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.cmdline import execute
import crawler_html_parser, json
from mongodb_middleware import mongodb_interface
from scrapy.crawler import CrawlerProcess
from web_discovery_aux import parsing_aux


class ProductFinderSpider(CrawlSpider):

    name = 'spider'

    custom_settings = {
        'DEPTH_LIMIT': '7',
        'DOWNLOAD_DELAY': '0.4',
        'CLOSESPIDER_PAGECOUNT': '100',
        'AUTOTHROTTLE_ENABLED': 'True',
        'AUTOTHROTTLE_TARGET_CONCURRENCY': '3'
    }

    #allowed_domains = ['unieuro.it']
    #start_urls = ['https://www.unieuro.it/online/']

    rules = (
        Rule(LinkExtractor(), callback='parse_item', follow=True),
    )

    def parse_item(self, response):
        #item_name = response.url.split("/")[-2] + '.html'
        relevant_links = crawler_html_parser.extract_relevant_links(response.body, self.name)
        content = {'url_page': str(response.url),
                   'html_raw_text': str(response.body),
                   'page_relevant_links': str(relevant_links)}
        content = json.dumps(content)
        domain = parsing_aux.find_domain_url(response.url)
        domain = parsing_aux.add_www_domain(domain)
        mongodb_interface.put(domain,content)

#execute('scrapy runspider crawler_super_simple.py'.split())

process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0'
})

def start_crawling(start_urls, allowed_domains):
    process.crawl(ProductFinderSpider, start_urls=start_urls,allowed_domains=allowed_domains)
    process.start()

start_crawling(['https://www.unieuro.it/online/','http://egyp.it'],['unieuro.it','egyp.it'])

