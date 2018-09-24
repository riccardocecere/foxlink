from pyspark import SparkContext, SparkConf
from web_discovery_aux import web_discovery_searx, bayes_model, parsing_aux
from shingler import structural_clustering,product_finder_structural_clustering
from mongodb_middleware import mongodb_interface
from crawler import crawler_product_finder
from shingler import product_finder_shingler
from metrics import cluster_metrics, referring_url_metrics
from classifier import naive_bayes_classifier
import math


conf = SparkConf().setAppName('product_finder')
sc = SparkContext(conf=conf)


#----------------
#WEB DISCOVERY CONFIG
#----------------
id_seed_path = 'hdfs:///user/maria_dev/data/test.txt'
training_path = 'hdfs:///user/maria_dev/data/training_set_cameras_home_pages.txt'
training_path_product_site = 'hdfs:///user/maria_dev/data/test_training_set_cameras.txt'
path_to_save_classifier_output = 'hdfs:///user/maria_dev/data/evaluation'
path_to_save_web_discovery_output = 'hdfs:///user/maria_dev/data/searx_discovery'
num_of_searx_result_pages = 5
save_web_discovery_output = True
save_classifier_output = True
filters_site = [1.0]
number_of_features = int(math.pow(2,18))
prepare_training_input = False
output_train_path_parquet = 'hdfs:///user/maria_dev/data/classifier/train_camera_sites.parquet'
output_eval_path_parquet = 'hdfs:///user/maria_dev/data/classifier/eval_camera_sites.parquet'
#-------------------------------------------------------------------------------------

#----------------
#CRAWLER CONFIG
#----------------
depth_limit = 8
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

path_to_save_referring_url_metrics = 'hdfs:///user/maria_dev/data/cluster_referring_url_output'
save_referring_url_metrics_output = True

thresold_number_element_in_cluster = 50

training_path_specifications_product_page = 'hdfs:///user/maria_dev/data/training_set_product_details_camera.txt'

save_labelled_url = True

path_to_labelled_url = 'hdfs:///user/maria_dev/data/lebelled_url_output'

save_classifier_labelled_output = True
path_to_save_classifier_labelled_output = 'hdfs:///user/maria_dev/data/lebelled_url_output_classified'
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

#training and evaluation product sites

product_sites = naive_bayes_classifier.keywords_naive_bayes_classifier(sc,training_path, sites, prepare_training_input, output_train_path_parquet, output_eval_path_parquet, save_classifier_output, path_to_save_classifier_output)
product_sites = product_sites.map(lambda row:row.domain).collect()
'''
(model,idf) = bayes_model.create_naive_bayes_model(sc,training_path_product_site,number_of_features)
product_sites = bayes_model.classify_with_naive_bayes(sc, model, idf, number_of_features, sites, save_classifier_output, path_to_save_classifier_output,filters_site)
product_sites = list(product_sites.keys().take(3))
'''

sc.stop()

#product_sites = ['http://www.camerachums.com','https://www.camerahouse.com.au','https://www.camerapro.com.au','https://www.42photo.com','http://www.thecamerastore.com','https://www.nikknusa.com','https://thecamerashoponline.com','https://www.lazada.com.my']
#product_sites = ['http://www.cameracanada.com','https://rockandsoul.com','https://cameracompany.com','https://www.camerasdirect.com.au','http://www.camerajungle.co.uk','https://www.digitalcamerawarehouse.com.au']
#product_sites = ['https://www.camerapro.com.au','http://www.camerachums.com','https://www.cliftoncameras.co.uk','https://www.adencamera.com','https://bookoutlet.com','https://www.ritzcamera.com', 'https://www.camerajungle.co.uk','https://www.digitalcamerawarehouse.com.au']
#product_sites = ['https://www.beachcamera.com','https://www.cliftoncameras.co.uk','https://www.focuscamera.com','https://www.gmcamera.com']
#product_sites = ['http://www.egyp.it','https://www.camerapro.co.au','http://www.camerachums.com','https://www.camerahouse.com.au','https://www.lazada.com.my']
#product_sites = ['https://www.aliexpress.com/','https://www.zalando.com','https://www.bookchor.com','https://bookoutlet.com','https://www.ritzcamera.com']

#product_sites = ['https://www.ritzcamera.com', 'https://www.camerajungle.co.uk', 'https://www.cliftoncameras.co.uk']
product_sites_crawled = crawler_product_finder.intrasite_crawling_iterative(product_sites,depth_limit,download_delay,closesspider_pagecount,autothrottle_enable,autothrottle_target_concurrency,save_product_site_crawled_output,path_to_save_crawler_output)
product_sites_shingled = product_finder_shingler.generate_shingles(sc,shingle_window,save_product_site_shingled_output,path_to_save_shingled_output)


conf = SparkConf().setAppName('product_finder')

sc = SparkContext(conf=conf)

#ToDo togliere, rimettere in sc parallelize di sotto al posto della comprehension
collections = mongodb_interface.get_all_collections()
clusters = product_finder_structural_clustering.all_sites_structural_clustering(sc, sc.parallelize(collections),save_structural_clustering_output,path_to_save_structural_clustering_output,thresold_number_element_in_cluster)

#clusters_metrics = cluster_metrics.calculate_cluster_metrics(sc,clusters,save_cluster_metrics_output,path_to_save_cluster_metrics)

referring_url_metrics = referring_url_metrics.calculate_referring_url_metrics(sc,clusters,save_referring_url_metrics_output,path_to_save_referring_url_metrics)

#in_degrees = calculate_inDegrees.start_calculate_in_degrees(sc,sc.parallelize(mongodb_interface.get_all_collections()),save_in_degree_output,path_to_save_in_degree_output)

#Infer page sembrerebbe non funzionare piu, forse colpa di mongodb?
#infer_pages = infer_product_pages.infer_product_pages(sc,clusters,in_degrees,threshold_in_degrees,save_infer_page_output,path_to_save_infer_page_output)
#specs_extracted = product_finder_specs_extraction.start_specs_extraction(sc,in_degrees,threshold_specs_extraction,save_specs_extraction_output,path_to_save_specs_extraction_output)

#Training on product specifications and indexes page
#(model_details_page,idf_detail_page) = bayes_model.create_naive_bayes_model(sc,training_path_specifications_product_page,number_of_features)
#labelled_url = classifier_utils.prepare_input_for_classifier(sc,referring_url_metrics,save_labelled_url, path_to_labelled_url)
#category_url = bayes_model.classify_labelled_url_in_cluster_with_naive_bayes(sc, model_details_page, idf_detail_page, number_of_features, labelled_url, save_classifier_labelled_output, path_to_save_classifier_labelled_output,filters_site)



sc.stop()






