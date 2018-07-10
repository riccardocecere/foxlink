import math

from pyspark import SparkContext, SparkConf
from web_discovery_aux import parsing_aux
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
import nltk
import datetime



conf = SparkConf().setAppName('product_finder')
sc = SparkContext(conf=conf)

path = 'hdfs:///user/maria_dev/data/test_training_set_cameras.txt'
test_path = 'hdfs:///user/maria_dev/data/test_model_cameras.txt'
input = sc.textFile(path)
number_of_features = int(math.pow(2,12))

try:
    nltk.download('punkt')
    nltk.download('stopwords')
except:
    print 'error to take nltk packages:'


training_set = input.map(lambda line: line.split('\t')) \
.map(lambda (site,label):(site,label,parsing_aux.extract_clean_text_from_page(site)))\
    .filter(lambda (site,label,words):isinstance(words, list))\
    .filter(lambda (site,label,words):len(words)>2)



#output = training_set.saveAsTextFile('hdfs:///user/maria_dev/data/output')


# Split data into labels and features, transform
# preservesPartitioning is not really required
# since map without partitioner shouldn't trigger repartitiong
labels = training_set.map(lambda (site,label,words):label)


# Indicates the length of the vector to use, and apply for each element an Hash function
# the result will be an RDD VECTOR
tf = HashingTF(numFeatures=number_of_features).transform(
    training_set.map(lambda (site,label,words): words))

tf.cache()

# I creates the IDF based on the previous TF
idf = IDF().fit(tf)

# Trasform idf in our tfidf RDD vectors
tfidf = idf.transform(tf)


# Combine using zip
training = labels.zip(tfidf).map(lambda x: LabeledPoint(x[0], x[1]))

# Train and check
model = NaiveBayes.train(training)


test_set = sc.textFile(test_path).map(lambda line: line.split('\t')) \
.map(lambda (site,label):(site,label,parsing_aux.extract_clean_text_from_page(site)))\
    .filter(lambda (site,label,words):isinstance(words, list))\
    .filter(lambda (site,label,words):len(words)>2)

test_labels = test_set.map(lambda (site,label,words):(site,label))
test_tf = HashingTF(numFeatures=number_of_features).transform(test_set.map(lambda(site,label,words):words))
test_tfidf = idf.transform(test_tf)



labels_and_preds = test_labels.zip(model.predict(test_tfidf)).map(
    lambda ((url,actual),predicted): {"url":url,"actual": actual, "predicted": float(predicted)})

labels_and_preds.saveAsTextFile('hdfs:///user/maria_dev/data/output')



