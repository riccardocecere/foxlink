from pyspark import SparkContext, SparkConf
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
from web_discovery_aux import parsing_aux, searx_aux, web_discovery_searx, bayes_model
import nltk
import math


conf = SparkConf().setAppName('product_finder')
sc = SparkContext(conf=conf)

path = 'hdfs:///user/maria_dev/data/test.txt'
training_path = 'hdfs:///user/maria_dev/data/test_training_set_products.txt'
path_to_save_evaluation ='hdfs:///user/maria_dev/data/evaluation'
path_to_save_sites = 'hdfs:///user/maria_dev/data/searx_discovery'
num_of_pages = 1
save_sites = False
save_evaluation = True
number_of_features = int(math.pow(2,12))

sites = web_discovery_searx.web_discovery_with_searx(path,sc,num_of_pages,save_sites,path_to_save_sites)

(model,idf) = bayes_model.create_naive_bayes_model(sc,training_path,number_of_features)
bayes_model.evaluate_with_naive_bayes(sc,model,idf,number_of_features,sites,save_evaluation,path_to_save_evaluation)


sc.stop()






