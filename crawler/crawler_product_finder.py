# -*- coding: utf-8 -*-
import product_finder_spider
from web_discovery_aux import parsing_aux


def intrasite_crawling(sc,product_sites,depth_limit,download_delay,closesspider_pagecount,autothrottle_enable,autothrottle_target_concurrency,save_rdd,output_path):

    output = product_sites.map(lambda (domain,values): (domain, {'pages': values['pages'], 'crawling_status': product_finder_spider.start_crawling([str(domain)], [str(parsing_aux.remove_www_domain(domain))], depth_limit, download_delay, closesspider_pagecount, autothrottle_enable, autothrottle_target_concurrency)}))

    if (save_rdd):
        if output_path != None and output_path != '':
            output.saveAsTextFile(output_path)

    return output