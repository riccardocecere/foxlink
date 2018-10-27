# -*- coding: utf-8 -*-

from pyspark.sql import SQLContext
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.feature import HashingTF, Tokenizer, StringIndexer, IndexToString
from pyspark.ml import Pipeline
from general_utils import rdd_utils
import classifier_utils


#Multinomial Naive Bayes classifier based on keyword
def keywords_naive_bayes_classifier(sc, training_path, number_of_features, evaluation_rdd, prepare_training_input, train_path_parquet, eval_path_parquet, save, path_to_save, classification_type):

    sqlContext = SQLContext(sc)

    if classification_type == 'home_pages':
        classifier_utils.prepare_input_for_home_page_classifier(sc, sqlContext, training_path, evaluation_rdd, prepare_training_input, train_path_parquet, eval_path_parquet)
    elif classification_type == 'cluster_pages':
        classifier_utils.prepare_input_for_cluster_page_classifier(sc, sqlContext, training_path, evaluation_rdd, prepare_training_input, train_path_parquet, eval_path_parquet)

    schemaTrain = sqlContext.read.load(train_path_parquet)
    schemaEval = sqlContext.read.load(eval_path_parquet)


    categoryIndexer = StringIndexer(inputCol="category", outputCol="label")
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    hashingTF = HashingTF(inputCol="words", outputCol="features", numFeatures=number_of_features)
    nb = NaiveBayes(smoothing=1.0, modelType="multinomial")
    categoryConverter = IndexToString(inputCol="label", outputCol="pred")
    pipeline = Pipeline(stages=[categoryIndexer, tokenizer, hashingTF, nb, categoryConverter])

    model = pipeline.fit(schemaTrain)
    pr = model.transform(schemaEval)

    classifier_utils.print_metrics(pr,'label','prediction','f1')

    output = pr.rdd

    if classification_type == 'home_pages' or classification_type == 'cluster_pages':
        output = output.filter(lambda row: row.prediction == 1.0)
    if classification_type == 'cluster_pages':
        output = output.map(lambda row: {'url':row.url,'cluster_label':row.cluster_label,'referring_url':row.referring_url,'domain':row.domain})

    rdd_utils.save_rdd(output,save,path_to_save)

    return output