from pyspark import SparkContext, SparkConf
from web_discovery_aux import web_discovery_searx, bayes_model, parsing_aux
from shingler import structural_clustering,product_finder_structural_clustering
from mongodb_middleware import mongodb_interface
from crawler import crawler_product_finder
from shingler import product_finder_shingler
from metrics import cluster_metrics
import math


conf = SparkConf().setAppName('product_finder')
sc = SparkContext(conf=conf)


#----------------
#WEB DISCOVERY CONFIG
#----------------
id_seed_path = 'hdfs:///user/maria_dev/data/test.txt'
training_path = 'hdfs:///user/maria_dev/data/test_training_set_products.txt'
training_path_product_site = 'hdfs:///user/maria_dev/data/test_training_set_products.txt'
path_to_save_classifier_output = 'hdfs:///user/maria_dev/data/evaluation'
path_to_save_web_discovery_output = 'hdfs:///user/maria_dev/data/searx_discovery'
num_of_searx_result_pages = 1
save_web_discovery_output = False
save_classifier_output = True
filters_site = [1.0]
number_of_features = int(math.pow(2,18))
#-------------------------------------------------------------------------------------

#----------------
#CRAWLER CONFIG
#----------------
depth_limit = 7
download_delay = 0.3
closesspider_pagecount = 40000
autothrottle_enable = True
autothrottle_target_concurrency = 10
save_product_site_crawled_output = False
path_to_save_crawler_output = 'hdfs:///user/maria_dev/data/product_sites_crawled'
#------------------------------------------------------------------------------------


#----------------
#SHINGLE CONFIG
#----------------
shingle_window = 4
save_product_site_shingled_output = True
path_to_save_shingled_output = 'hdfs:///user/maria_dev/data/product_sites_shingled'
#------------------------------------------------------------------------------------

#----------------
#STRUCTURAL_CLUSTERING CONFIG
#----------------
save_structural_clustering_output = True
path_to_save_structural_clustering_output = 'hdfs:///user/maria_dev/data/structural_clustering'

training_path_product_page = 'hdfs:///user/maria_dev/data/test_training_set_page_product_cameras.txt'

filters_product_pages = [0.0,1.0,2.0]

path_to_save_cluster_metrics = 'hdfs:///user/maria_dev/data/cluster_metrics_output'
save_cluster_metrics_output = True
thresold_number_element_in_cluster = 75
#------------------------------------------------------------------------------------

#----------------
#IN_DEGREES CONFIG
#----------------
save_in_degree_output = True
path_to_save_in_degree_output = 'hdfs:///user/maria_dev/data/in_degree'
#------------------------------------------------------------------------------------

#----------------
#INFER PAGE CONFIG
#----------------
save_infer_page_output = False
path_to_save_infer_page_output = 'hdfs:///user/maria_dev/data/infer_page'
threshold_in_degrees = 0.01
#------------------------------------------------------------------------------------


#----------------
#SPECS_EXTRACTION_CONFIG
#----------------
threshold_specs_extraction = 0.9
save_specs_extraction_output = True
path_to_save_specs_extraction_output = 'hdfs:///user/maria_dev/data/specs_extraction'
#------------------------------------------------------------------------------------

sites = web_discovery_searx.web_discovery_with_searx(id_seed_path, sc, num_of_searx_result_pages, save_web_discovery_output, path_to_save_web_discovery_output)

#training product site
#(model,idf) = bayes_model.create_naive_bayes_model(sc,training_path,number_of_features)

#product_sites = bayes_model.classify_with_naive_bayes(sc, model, idf, number_of_features, sites, save_classifier_output, path_to_save_classifier_output,filters_site)
#product_sites = list(product_sites.keys().take(3))
sc.stop()

#product_sites = ['http://www.camerachums.com','https://www.camerahouse.com.au','https://www.camerapro.com.au','https://www.42photo.com','http://www.thecamerastore.com','https://www.nikknusa.com','https://thecamerashoponline.com','https://www.lazada.com.my']
#product_sites = ['http://www.cameracanada.com','https://rockandsoul.com','https://cameracompany.com','https://www.camerasdirect.com.au','http://www.camerajungle.co.uk','https://www.digitalcamerawarehouse.com.au']
#product_sites = ['https://www.beachcamera.com','https://www.cliftoncameras.co.uk','https://www.focuscamera.com','https://www.gmcamera.com']
#product_sites = ['http://www.egyp.it','https://www.camerapro.co.au','http://www.camerachums.com','https://www.camerahouse.com.au','https://www.lazada.com.my']
#product_sites = ['https://www.aliexpress.com/','https://www.zalando.com','https://www.bookchor.com','https://bookoutlet.com','https://www.ritzcamera.com']
#product_sites_crawled = crawler_product_finder.intrasite_crawling_iterative(product_sites,depth_limit,download_delay,closesspider_pagecount,autothrottle_enable,autothrottle_target_concurrency,save_product_site_crawled_output,path_to_save_crawler_output)
#product_sites_shingled = product_finder_shingler.generate_shingles(sc,shingle_window,save_product_site_shingled_output,path_to_save_shingled_output)

conf = SparkConf().setAppName('product_finder')

sc = SparkContext(conf=conf)

#ToDo togliere, rimettere in sc parallelize di sotto al posto della comprehension
collections = mongodb_interface.get_all_collections()
clusters = product_finder_structural_clustering.all_sites_structural_clustering(sc, sc.parallelize(collections),save_structural_clustering_output,path_to_save_structural_clustering_output,thresold_number_element_in_cluster)

clusters_metrics = cluster_metrics.calculate_cluster_metrics(sc,clusters,save_cluster_metrics_output,path_to_save_cluster_metrics)

#in_degrees = calculate_inDegrees.start_calculate_in_degrees(sc,sc.parallelize(mongodb_interface.get_all_collections()),save_in_degree_output,path_to_save_in_degree_output)

#Infer page sembrerebbe non funzionare piu, forse colpa di mongodb?
#infer_pages = infer_product_pages.infer_product_pages(sc,clusters,in_degrees,threshold_in_degrees,save_infer_page_output,path_to_save_infer_page_output)
#specs_extracted = product_finder_specs_extraction.start_specs_extraction(sc,in_degrees,threshold_specs_extraction,save_specs_extraction_output,path_to_save_specs_extraction_output)

#print str(specs_extracted.take(3))

sc.stop()






