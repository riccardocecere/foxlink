# -*- coding: utf-8 -*-
from pyspark.sql import Row
from web_discovery_aux import parsing_aux
from mongodb_middleware import mongodb_interface
from collections import defaultdict
import random
import ast
import pattern.web

def take_random_from_cluster_list(cluster, random_number):
    number_elements = len(cluster[1])
    random_cluster = random.sample(cluster[1],int(number_elements*random_number))
    return random_cluster


def prepare_input_for_home_page_classifier(sc, sqlContext, training_path, evaluation_rdd, prepare_training_input, output_train_path_parquet, output_eval_path_parquet):

    if prepare_training_input:
        train_rdd = sc.textFile(training_path).map(lambda line: line.split('\t'))
        generate_parquet(sqlContext, train_rdd, output_train_path_parquet)

    evaluation_rdd = evaluation_rdd.map(lambda(domain,values):(domain,1))
    generate_parquet(sqlContext, evaluation_rdd, output_eval_path_parquet)
    return None



def generate_parquet(sqlContext, rdd_data, ouput_path):
    rdd_data = rdd_data.map(lambda p: Row(domain=p[0], category=p[1], text=parsing_aux.get_html_text(str(p[0]))))\
            .filter(lambda row: row.text != 'Error')
    schema = sqlContext.createDataFrame(rdd_data)
    schema.write.save(ouput_path, format="parquet")
    return None


def prepare_input_for_cluster_page_classifier(sc, sqlContext, training_path, evaluation_rdd, prepare_training_input, output_train_path_parquet, output_eval_path_parquet):

    if prepare_training_input:
        train_rdd = sc.textFile(training_path).map(lambda line: line.split('\t'))
        generate_parquet(sqlContext, train_rdd, output_train_path_parquet)

    evaluation_rdd = evaluation_rdd.flatMap(lambda(domain,clusters):((domain,cluster) for cluster in clusters))\
                    .map(lambda (domain,cluster): (domain,(cluster['cluster_elements'],cluster['label'])))\
                    .flatMap(lambda (domain,cluster): ((domain,(cluster_element[0],cluster_element[2],cluster[1],1,parsing_aux.get_clean_text_from_html(mongodb_interface.get_html_page(domain,cluster_element[0])))) for cluster_element in cluster[0]))
    generate_parquet_for_cluster_pages(sqlContext, evaluation_rdd, output_eval_path_parquet)
    return None

# (domain, (url,referreing_url,label,1,clean_text))
def generate_parquet_for_cluster_pages(sqlContext, rdd_data, output_eval_path_parquet):
    rdd_data = rdd_data.map(lambda p: Row(domain=p[0], category=p[1][3], text=p[1][4], url=p[1][0], referring_url=p[1][1], cluster_label=p[1][2]))\
            .filter(lambda row: row.text != 'Error')

    schema = sqlContext.createDataFrame(rdd_data)
    schema.write.save(output_eval_path_parquet, format="parquet")
    return None

# (domain, [([cluster],[(url,depth,referring_url)), (), ...],)

def prepare_input_for_cluster_classifier(sc, sqlContext, training_path, evaluation_rdd, prepare_training_input, output_train_path_parquet, output_eval_path_parquet):

    if prepare_training_input:
        train_rdd = sc.textFile(training_path).map(lambda line: line.split('\t'))
        generate_parquet(sqlContext, train_rdd, output_train_path_parquet)

    evaluation_rdd = evaluation_rdd.flatMap(lambda (domain,clusters):((domain,cluster) for cluster in clusters))\
        .map(lambda (domain,cluster): ((domain,cluster[0]), take_random_from_cluster_list(cluster, 0.2)))\
        .flatMap(lambda (key,cluster): ((key,cluster_element) for cluster_element in cluster))\
        .map(lambda (key,cluster_element): (key, (cluster_element[0],cluster_element[1],cluster_element[2],parsing_aux.get_clean_text_from_html(mongodb_interface.get_html_page(key[0],cluster_element[0])),1)))
    generate_parquet_for_cluster(sqlContext,evaluation_rdd,output_eval_path_parquet)
    return None



def generate_parquet_for_cluster(sqlContext, rdd_data, output_eval_path_parquet):
    rdd_data = rdd_data.map(lambda p: Row(domain_cluster=p[0], category=p[1][4], text=p[1][3]))\
            .filter(lambda row: row.text != 'Error')

    schema = sqlContext.createDataFrame(rdd_data)
    schema.write.save(output_eval_path_parquet, format="parquet")
    return None


def prediction_counts(predictions):
    result = defaultdict(int)
    for prediction in predictions:
        result[prediction] += 1
    return max(result.items(),key=lambda e:e[1])[0]


def evaluate_cluster_labels(output):
    output = output.map(lambda row: (str(row.domain_cluster._1)+'/-----/'+str(row.domain_cluster._2),row.prediction))\
            .groupByKey()\
            .map(lambda (key,prediction):((key.split('/-----/')[0],ast.literal_eval(key.split('/-----/')[1])),prediction))\
            .mapValues(prediction_counts)
    return output

def add_label_to_domain_clusters(domain,clusters,labels):
    output_clusters = []

    for cluster in clusters:
        for label in labels:
            if label[0] == (domain,cluster[0]):
                new_cluster = {'label': label[1], 'cluster_elements':cluster[1]}
                output_clusters.append(new_cluster)
    return output_clusters


def add_label_to_clusters(evaluation_rdd,labels):
    output = evaluation_rdd.map(lambda (domain,clusters):(domain, add_label_to_domain_clusters(domain,clusters,labels)))
    return output