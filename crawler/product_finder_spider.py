# -*- coding: utf-8 -*-
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
import crawler_html_parser, json
from mongodb_middleware import mongodb_interface
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from web_discovery_aux import parsing_aux
from bs4 import BeautifulSoup


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
                   'html_raw_text': str(BeautifulSoup(response.body,'html.parser').body),
                   'page_relevant_links': str(list(set(relevant_links)))}
        content = json.dumps(content)
        domain = parsing_aux.add_www_domain(domain)
        mongodb_interface.put(domain,content)



# Function for starting the crawler, it takes several parameters for the settings
def start_crawling(start_urls, allowed_domains,depth_limit,download_delay, closespider_pagecount, autothrottle_enable, autothrottle_target_concurrency):

    print '------------------------------------'
    print '--------------URLS---------------'
    print start_urls
    print allowed_domains
    #Check if the donwload delay is at a minimum of 0.4 sec
    if download_delay == None or download_delay<0.4:
        download_delay = 0.4

    custom_settings = get_project_settings()
    custom_settings.update({
        'DEPTH_LIMIT': str(depth_limit),
        'DOWNLOAD_DELAY': str(download_delay),
        'CLOSESPIDER_PAGECOUNT': str(closespider_pagecount),
        'AUTOTHROTTLE_ENABLED': str(autothrottle_enable),
        'AUTOTHROTTLE_TARGET_CONCURRENCY': str(autothrottle_target_concurrency),
        'CONCURRENT_REQUESTS': str(150),
        'REACTOR_THREADPOOL_MAXSIZE': str(20),
        'LOG_LEVEL' : 'INFO',
        'COOKIES_ENABLED':'False',
        'RETRY_ENABLE':'False',
        'DOWNLOAD_TIMEOUT':str(15),
        'REDIRECT_ENABLED':'False',
        'AJAXCRAWL_ENABLED':'True',
        'ROBOTSTXT_OBEY':'True',
        'USER_AGENT': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0'
    })
    process = CrawlerProcess(custom_settings)
    process.crawl(ProductFinderSpider, start_urls=start_urls,allowed_domains=allowed_domains)
    process.start()
    return 'crawled'


