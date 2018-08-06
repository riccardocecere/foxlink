from pyspark import SparkContext, SparkConf
from web_discovery_aux import web_discovery_searx, bayes_model
from crawler import crawler_product_finder
from shingler import product_finder_shingler
import math


conf = SparkConf().setAppName('product_finder')
sc = SparkContext(conf=conf)


#----------------
#WEB DISCOVERY CONFIG
#----------------
id_seed_path = 'hdfs:///user/maria_dev/data/test.txt'
training_path = 'hdfs:///user/maria_dev/data/test_training_set_products.txt'
path_to_save_classifier_output = 'hdfs:///user/maria_dev/data/evaluation'
path_to_save_web_discovery_output = 'hdfs:///user/maria_dev/data/searx_discovery'
num_of_searx_result_pages = 1
save_web_discovery_output = False
save_classifier_output = False
number_of_features = int(math.pow(2,18))
#-------------------------------------------------------------------------------------

#----------------
#CRAWLER CONFIG
#----------------
depth_limit = 7
download_delay = 0.4
closesspider_pagecount = 50
autothrottle_enable = True
autothrottle_target_concurrency = 3
save_product_site_crawled_output = True
path_to_save_crawler_output = 'hdfs:///user/maria_dev/data/product_sites_crawled'
#------------------------------------------------------------------------------------


#----------------
#SHINGLE CONFIG
#----------------
shingle_window = 3
save_product_site_shingled_output = True
path_to_save_shingled_output = 'hdfs:///user/maria_dev/data/product_sites_shingled'
#------------------------------------------------------------------------------------

sites = web_discovery_searx.web_discovery_with_searx(id_seed_path, sc, num_of_searx_result_pages, save_web_discovery_output, path_to_save_web_discovery_output)
(model,idf) = bayes_model.create_naive_bayes_model(sc,training_path,number_of_features)
product_sites = bayes_model.classify_with_naive_bayes(sc, model, idf, number_of_features, sites, save_classifier_output, path_to_save_classifier_output)
product_sites = list(product_sites.keys().take(3))
sc.stop()

product_sites_crawled = crawler_product_finder.intrasite_crawling_iterative(product_sites,depth_limit,download_delay,closesspider_pagecount,autothrottle_enable,autothrottle_target_concurrency,save_product_site_crawled_output,path_to_save_crawler_output)
product_sites_shingled = product_finder_shingler.generate_shingles(sc,shingle_window,save_product_site_shingled_output,path_to_save_shingled_output)








