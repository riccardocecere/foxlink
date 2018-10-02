# -*- coding: utf-8 -*-

from pyspark.sql import SQLContext
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import HashingTF, Tokenizer, StringIndexer, IndexToString
from pyspark.ml import Pipeline
import classifier_utils


def keywords_naive_bayes_classifier(sc, training_path, evaluation_rdd, prepare_training_input, output_train_path_parquet, output_eval_path_parquet, save, path_to_save, classification_type):

    sqlContext = SQLContext(sc)

    if classification_type == 'home_pages':
        classifier_utils.prepare_input_for_home_page_classifier(sc, sqlContext, training_path, evaluation_rdd, prepare_training_input, output_train_path_parquet, output_eval_path_parquet)
    elif classification_type == 'cluster_pages':
        classifier_utils.prepare_input_for_cluster_page_classifier(sc, sqlContext, training_path, evaluation_rdd, prepare_training_input, output_train_path_parquet, output_eval_path_parquet)
    elif classification_type == 'cluster_labels':
        classifier_utils.prepare_input_for_cluster_classifier(sc, sqlContext, training_path, evaluation_rdd,prepare_training_input, output_train_path_parquet,output_eval_path_parquet)

    schemaTrain = sqlContext.read.load(output_train_path_parquet)
    schemaEval = sqlContext.read.load(output_eval_path_parquet)


    categoryIndexer = StringIndexer(inputCol="category", outputCol="label")
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    hashingTF = HashingTF(inputCol="words", outputCol="features", numFeatures=10000)
    nb = NaiveBayes(smoothing=1.0, modelType="multinomial")
    categoryConverter = IndexToString(inputCol="label", outputCol="pred")
    pipeline = Pipeline(stages=[categoryIndexer, tokenizer, hashingTF, nb, categoryConverter])

    model = pipeline.fit(schemaTrain)
    pr = model.transform(schemaEval)

    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
    metric = evaluator.evaluate(pr)

    print '---------------F metric-----------------'
    #print pr.show()
    print "F1 metric = %g" % metric

    output = pr.rdd
    if classification_type == 'home_pages' or classification_type == 'cluster_pages':
        output = output.filter(lambda row: row.prediction == 1.0)
    if classification_type == 'cluster_pages':
        output = output.map(lambda row: {'url':row.url,'cluster_label':row.cluster_label,'referring_url':row.referring_url,'domain':row.domain})
    elif classification_type == 'cluster_labels':
        output = classifier_utils.evaluate_cluster_labels(output)
        output = classifier_utils.add_label_to_clusters(evaluation_rdd,output.collect())

    if (save):
        if path_to_save != None and path_to_save != '':
            output.saveAsTextFile(path_to_save)

    return output