from pyspark import SparkContext, SparkConf
from web_discovery_aux import web_discovery_searx, bayes_model
import math


conf = SparkConf().setAppName('product_finder')
sc = SparkContext(conf=conf)

id_seed_path = 'hdfs:///user/maria_dev/data/test.txt'
training_path = 'hdfs:///user/maria_dev/data/test_training_set_products.txt'
path_to_save_classifier_output = 'hdfs:///user/maria_dev/data/evaluation'
path_to_save_web_discovery_output = 'hdfs:///user/maria_dev/data/searx_discovery'
num_of_searx_result_pages = 1
save_web_discovery_output = True
save_classifier_output = False
number_of_features = int(math.pow(2,12))

sites = web_discovery_searx.web_discovery_with_searx(id_seed_path, sc, num_of_searx_result_pages, save_web_discovery_output, path_to_save_web_discovery_output)
(model,idf) = bayes_model.create_naive_bayes_model(sc,training_path,number_of_features)
bayes_model.classify_with_naive_bayes(sc, model, idf, number_of_features, sites, save_classifier_output, path_to_save_classifier_output)


sc.stop()






