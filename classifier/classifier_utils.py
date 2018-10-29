# -*- coding: utf-8 -*-
from pyspark.sql import Row
from web_discovery import parser
from mongodb_middleware import mongodb_interface
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


# Print metrcis for effectivness of classifier
def print_metrics(prediction,expected_field,prediction_field,measure):
    evaluator = MulticlassClassificationEvaluator(labelCol=expected_field, predictionCol=prediction_field, metricName=measure)
    metric = evaluator.evaluate(prediction)
    print '---------------F metric-----------------'
    # print prediction.show()
    print "F1 metric = %g" % metric
    return None


# Function to train homepage classifier if needed and to make predictions on the evaluation set
def prepare_input_for_home_page_classifier(sc, sqlContext, training_path, evaluation_rdd, prepare_training_input, output_train_path_parquet, output_eval_path_parquet):

    if prepare_training_input:
        train_rdd = sc.textFile(training_path).map(lambda line: line.split('\t'))
        generate_parquet(sqlContext, train_rdd, output_train_path_parquet)

    evaluation_rdd = evaluation_rdd.map(lambda(domain,values):(domain,1))
    generate_parquet(sqlContext, evaluation_rdd, output_eval_path_parquet)
    return None


# Function to save dataframe in parquet format
def generate_parquet(sqlContext, rdd_data, ouput_path):
    rdd_data = rdd_data.map(lambda p: Row(domain=p[0], category=p[1], text=parser.get_html_text(str(p[0]))))\
            .filter(lambda row: row.text != 'Error')
    schema = sqlContext.createDataFrame(rdd_data)
    schema.write.save(ouput_path, format="parquet")
    return None

# Function to train custer pages classifier if needed and to make predictions on the evaluation set
def prepare_input_for_cluster_page_classifier(sc, sqlContext, training_path, evaluation_rdd, prepare_training_input, output_train_path_parquet, output_eval_path_parquet):

    if prepare_training_input:
        train_rdd = sc.textFile(training_path).map(lambda line: line.split('\t'))
        generate_parquet(sqlContext, train_rdd, output_train_path_parquet)

    evaluation_rdd = evaluation_rdd.flatMap(lambda(domain,clusters):((domain,cluster) for cluster in clusters))\
                    .map(lambda (domain,cluster): (domain,(cluster['cluster_elements'],cluster['label'])))\
                    .flatMap(lambda (domain,cluster): ((domain,(cluster_element[0],cluster_element[2],cluster[1],1,parser.get_clean_text_from_html(mongodb_interface.get_html_page(domain,cluster_element[0])))) for cluster_element in cluster[0]))
    generate_parquet_for_cluster_pages(sqlContext, evaluation_rdd, output_eval_path_parquet)
    return None

# Function to save dataframe in parquet format from clusters rdd
def generate_parquet_for_cluster_pages(sqlContext, rdd_data, output_eval_path_parquet):
    rdd_data = rdd_data.map(lambda p: Row(domain=p[0], category=p[1][3], text=p[1][4], url=p[1][0], referring_url=p[1][1], cluster_label=p[1][2]))\
            .filter(lambda row: row.text != 'Error')

    schema = sqlContext.createDataFrame(rdd_data)
    schema.write.save(output_eval_path_parquet, format="parquet")
    return None