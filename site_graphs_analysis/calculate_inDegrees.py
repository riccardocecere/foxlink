# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from collections import defaultdict
from mongodb_middleware import mongodb_interface
import re,itertools,ast

def destinations(collection_name):
    collection = mongodb_interface.get_collection(collection_name)
    destionations_list = []

    for post in collection.find():
        links = post['page_relevant_links']

        if links == None or links == '' or links =='[]':
            continue
        links = ast.literal_eval(links)
        destionations_list += links
    return (collection_name,destionations_list)

def inDegrees(destinations):
    inDegrees = defaultdict(int)
    for destination in destinations:
        inDegrees[destination] += 1
    return inDegrees

def normalize(in_degrees):
    if in_degrees == None or in_degrees == '':
        return 'ERROR: Normalize in_degree'

    in_degrees_output = []

    for in_degree in in_degrees:
        if in_degree[1]>0:
            normalized_in_degree = (in_degree[0],float(1.0/float(in_degree[1])))
            in_degrees_output.append(normalized_in_degree)
    return in_degrees_output

def start_calculate_in_degrees(sc,collections,save,path_to_save):

    output = collections.map(lambda collection: destinations(collection))\
            .mapValues(inDegrees).map(lambda (domain,in_degrees):(domain,normalize(in_degrees.items())))
    if save and path_to_save != '' and path_to_save != None:
        output.saveAsTextFile(path_to_save)
    return output


'''
def start_calculate_in_degrees(sc):

    collections = mongodb_interface.get_all_collections()

    ss = SparkSession \
        .builder \
        .appName("prova") \
        .config("spark.mongodb.input.uri", "mongodb://172.17.0.2:27017/ProductFinderCrawlerData."+str(collections[0])) \
        .config("spark.mongodb.output.uri", "mongodb://172.17.0.2:27017/ProductFinderCrawlerData."+str(collections[0])) \
        .getOrCreate()


    adjacency_lists = []

    #qui mi porto dietro anche l'url della singola pagina anche se alla fine di fatto non serve, capire se può essere ancora utile in futuro altrimenti di toglie.
    for collection in collections:

        df = ss.read.option('allowDiskUse','true').format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://172.17.0.2:27017/ProductFinderCrawlerData."+str(collection)).load()
        df.createOrReplaceTempView('table')
        answer = ss.sql('select url_page,page_relevant_links from table').collect()

        adjacency_lists.append((str(collection),[(x[0],x[1]) for x in answer]))


    output = sc.parallelize(adjacency_lists) \
        .map(lambda (domain,destinations): (domain,[x[1] for x in destinations])) \
        .map(lambda (domain,destinations): (domain,[re.sub('\[|\]','',x).split(',') for x in destinations])) \
        .map(lambda (domain,destinations): (domain,list(itertools.chain().from_iterable(destinations)))) \
        .map(lambda (domain,destinations): (domain,inDegrees(destinations).items())) \
        .mapValues(normalize)

    return output

'''
