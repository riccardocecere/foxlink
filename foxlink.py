from pyspark import SparkContext, SparkConf
from web_discovery import web_discovery_searx
from clustering import foxlink_structural_clustering,foxlink_shingler
from mongodb_middleware import mongodb_interface
from crawler import foxlink_crawler
from cluster_linkage_analysis import referring_url_analysis
from classifier import naive_bayes_classifier
from xpath_analysis import xpath_patterns
import math,json


f = open('/root/config.json','r')
config = json.loads(f.read())
f.close()


conf = SparkConf().setAppName('foxlink')
sc = SparkContext(conf=conf)

#web discovery with searx
#sites = web_discovery_searx.web_discovery_with_searx(config['web_discovery']['id_seed_path'], sc, config['web_discovery']['searx']['num_of_searx_result_pages'], config['web_discovery']['searx']['save_web_discovery_output'], config['web_discovery']['searx']['path_to_save_web_discovery_output'])

#training and evaluation product sites
#product_sites = naive_bayes_classifier.keywords_naive_bayes_classifier(sc, config['web_discovery']['product_sites_classifier']['training_path'], int(math.pow(2,int(config['web_discovery']['product_sites_classifier']['number_of_features_exponent']))), sites, config['web_discovery']['product_sites_classifier']['prepare_training_input'], config['web_discovery']['product_sites_classifier']['output_train_path_parquet'], config['web_discovery']['product_sites_classifier']['output_eval_path_parquet'], config['web_discovery']['product_sites_classifier']['save_classifier_output'], config['web_discovery']['product_sites_classifier']['path_to_save_classifier_output'],'home_pages')
#product_sites = product_sites.map(lambda row: row.domain).take(20)

sc.stop()


#intrasite crawler
#product_sites_crawled = foxlink_crawler.intrasite_crawling_iterative(product_sites, config['crawler']['depth_limit'], config['crawler']['download_delay'], config['crawler']['closespider_pagecount'], config['crawler']['autothrottle_enable'], config['crawler']['autothrottle_target_concurrency'], config['crawler']['save_product_site_crawled_output'], config['crawler']['path_to_save_crawler_output'])


conf = SparkConf().setAppName('foxlink')
sc = SparkContext(conf=conf)

#calculate shingle vectors on web pages
product_sites_shingled = foxlink_shingler.generate_shingles(config['shingle']['shingle_window'])

#compute structural clustering of web pages
collections = mongodb_interface.get_all_collections()
clusters = foxlink_structural_clustering.all_sites_structural_clustering(sc.parallelize(collections), config['structural_clustering']['save_structural_clustering_output'], config['structural_clustering']['path_to_save_structural_clustering_output'], config['structural_clustering']['threshold_number_element_in_cluster'])

#culsters elements linkage analysis
referring_url_metrics = referring_url_analysis.calculate_all_cluster_labels(clusters, config['linkage_analysis']['save_referring_url_metrics_output'], config['linkage_analysis']['path_to_save_referring_url_metrics'])

#cluster pages classifier
category_clusters = naive_bayes_classifier.keywords_naive_bayes_classifier(sc,config['cluster_pages_classifier']['training_path_cluster_classifier'], int(math.pow(2,int(config['cluster_pages_classifier']['number_of_features_exponent']))),referring_url_metrics,config['cluster_pages_classifier']['prepare_training_input_cluster_page'],config['cluster_pages_classifier']['ouput_train_cluster_page_path_parquet'],config['cluster_pages_classifier']['output_eval_cluster_page_path_parquet'],config['cluster_pages_classifier']['save_cluster_page_evaluation'],config['cluster_pages_classifier']['path_to_save_cluster_page'],'cluster_pages')

#xpath generalization
xpaths = xpath_patterns.xpath_sequencies(category_clusters,config['xpath_generalization']['save_xpath'],config['xpath_generalization']['path_to_save_xpath'])
generalized_xpath = xpath_patterns.generalize_xpath(sc,xpaths,config['xpath_generalization']['save_xpath_generalized'],config['xpath_generalization']['path_to_save_xpath_generalized'])

sc.stop()