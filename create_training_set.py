from pyspark import SparkContext, SparkConf
from web_discovery_aux import parsing_aux
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
import nltk


conf = SparkConf().setAppName('product_finder')
sc = SparkContext(conf=conf)

path = 'hdfs:///user/maria_dev/data/training_set.txt'
input = sc.textFile(path)

try:
    nltk.download('punkt')
    nltk.download('stopwords')
except:
    print 'error to take nltk packages:'


training_set = input.map(lambda line: line.split('\t')) \
.map(lambda (site,label):(site,label,parsing_aux.extract_clean_text_from_page(site)))\
    .filter(lambda (site,lable,words):isinstance(words, list))\
    ,filter(lambda (site,lable,words):len(words)>0)


#output = training_set.saveAsTextFile('hdfs:///user/maria_dev/data/output')



# Split data into labels and features, transform
# preservesPartitioning is not really required
# since map without partitioner shouldn't trigger repartitiong
labels = training_set.map(lambda (site,label,words):label)

tf = HashingTF(numFeatures=100).transform(
    training_set.map(lambda (site,label,words): words))

#Analizzare meglio
idf = IDF().fit(tf)
tfidf = idf.transform(tf)

# Combine using zip
training = labels.zip(tfidf).map(lambda x: LabeledPoint(x[0], x[1]))

# Train and check
model = NaiveBayes.train(training)
labels_and_preds = labels.zip(model.predict(tfidf)).map(
    lambda x: {"actual": x[0], "predicted": float(x[1])})

labels_and_preds.saveAsTextFile('hdfs:///user/maria_dev/data/output')
