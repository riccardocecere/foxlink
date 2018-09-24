# -*- coding: utf-8 -*-
from pyspark.sql import Row
from web_discovery_aux import parsing_aux

def prepare_input_for_classifier(sc,sqlContext,training_path, evaluation_rdd, prepare_training_input,output_train_path_parquet, output_eval_path_parquet):

    if prepare_training_input:
        train_rdd = sc.textFile(training_path).map(lambda line: line.split('\t'))
        generate_parquet(sqlContext,train_rdd,output_train_path_parquet)

    evaluation_rdd = evaluation_rdd.map(lambda(domain,values):(domain,1))
    generate_parquet(sqlContext,evaluation_rdd,output_eval_path_parquet)
    return None



def generate_parquet(sqlContext,rdd_data,ouput_path):
    rdd_data = rdd_data.map(lambda p: Row(domain=p[0], category=p[1], text=parsing_aux.get_html_text(str(p[0]))))\
            .filter(lambda row: row.text != 'Error')
    schema = sqlContext.createDataFrame(rdd_data)
    schema.write.save(ouput_path, format="parquet")
    return None