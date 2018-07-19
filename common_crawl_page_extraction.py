from pyspark import SparkContext, SparkConf
import re


# Set context
conf = SparkConf().setAppName('product_finder').set("textinputformat.record.delimiter","fetchTimeMs")
sc = SparkContext(conf=conf)
sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter","fetchTimeMs")


# Take inputs from file on hdfs
path_to_warcs = 'hdfs:///user/maria_dev/data/commoncrawl/warcs'
path_to_output = 'hdfs:///user/maria_dev/data/commoncrawl/page_extracted'

warcs = sc.textFile(path_to_warcs)
warcs = warcs.map(lambda warc:str(warc.encode('utf-8')))\
    .filter(lambda warc: re.search('WARC-Target-URI: http:\/\/amazon\.com',warc) != None)

warcs.saveAsTextFile(path_to_output)

sc.stop()