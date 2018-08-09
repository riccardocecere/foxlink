# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark.sql import SparkSession
from collections import defaultdict
from mongodb_middleware import mongodb_interface
import json,re,itertools


def inDegrees(destinations):
    inDegrees = defaultdict(int)
    for destination in destinations:
        inDegrees[destination] += 1
    return inDegrees


collections = mongodb_interface.get_all_collections()

#!!!!! qui bisogna togliere lo sparkContext perché poi girerà con il resto del codice e non da solo!!!!!
sc = SparkContext('local','prova')

ss = SparkSession \
    .builder \
    .appName("prova") \
    .config("spark.mongodb.input.uri", "mongodb://172.17.0.2:27017/ProductFinderCrawlerData."+str(collections[0])) \
    .config("spark.mongodb.output.uri", "mongodb://172.17.0.2:27017/ProductFinderCrawlerData."+str(collections[0])) \
    .getOrCreate()


adjacency_lists = []

#qui mi porto dietro anche l'url della singola pagina anche se alla fine di fatto non serve, capire se può essere ancora utile in futuro altrimenti di toglie.
for collection in collections:

    df = ss.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://172.17.0.2:27017/ProductFinderCrawlerData."+str(collection)).load()
    df.createOrReplaceTempView('table')
    answer = ss.sql('select url_page,links from table').collect()

    adjacency_lists.append((str(collection),[(x[0],x[1]) for x in answer]))


output = sc.parallelize(adjacency_lists) \
    .map(lambda (domain,destinations): (domain,[x[1] for x in destinations])) \
    .map(lambda (domain,destinations): (domain,[re.sub('\[|\]','',x).split(',') for x in destinations])) \
    .map(lambda (domain,destinations): (domain,list(itertools.chain().from_iterable(destinations)))) \
    .map(lambda (domain,destinations): (domain,inDegrees(destinations).items()))

print output.collect()

sc.stop()