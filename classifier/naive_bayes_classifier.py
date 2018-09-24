# -*- coding: utf-8 -*-

from pyspark.sql import SQLContext
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import HashingTF, Tokenizer, StringIndexer, IndexToString
from pyspark.ml import Pipeline
import classifier_utils


def keywords_naive_bayes_classifier(sc,training_path, evaluation_rdd, prepare_training_input,output_train_path_parquet, output_eval_path_parquet, save, path_to_save):

    sqlContext = SQLContext(sc)

    classifier_utils.prepare_input_for_classifier(sc,sqlContext,training_path, evaluation_rdd, prepare_training_input,output_train_path_parquet, output_eval_path_parquet)

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

    output = pr.rdd.filter(lambda row: row.prediction == 1.0)

    if (save):
        if path_to_save != None and path_to_save != '':
            output.saveAsTextFile(path_to_save)

    return output