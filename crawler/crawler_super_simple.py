from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.cmdline import execute
import crawler_html_parser, json
from mongodb_middleware import mongodb_interface

class MySpider(CrawlSpider):

    name = 'unieuro.it'

    custom_settings = {
        'DEPTH_LIMIT': '7',
        'DOWNLOAD_DELAY': '0.8',
        'CLOSESPIDER_PAGECOUNT': '5'
    }

    allowed_domains = ['unieuro.it']
    start_urls = ['https://www.unieuro.it/online/']

    rules = (
        Rule(LinkExtractor(), callback='parse_item', follow=True),
    )

    def parse_item(self, response):
        item_name = response.url.split("/")[-2] + '.html'
        relevant_links = crawler_html_parser.extract_relevant_links(response.body, self.name)
        content = {'item_name': response.url,
                   'html_text': response.body,
                   'links': relevant_links}
        content = json.loads(content)
        mongodb_interface.put(self.name,content)

execute('scrapy runspider crawler_super_simple.py'.split())