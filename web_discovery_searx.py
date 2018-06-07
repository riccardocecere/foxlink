from pyspark import SparkContext, SparkConf
from web_discovery_aux import parsing_aux,searx_aux


conf = SparkConf().setAppName('product_finder')
sc = SparkContext(conf=conf)

path = 'hdfs:///user/maria_dev/data/after_cleaning.txt'
input = sc.textFile(path)

input.persist()
seedLength = float(input.count())

# FlatMap 1 Function that returns all urls from the results of Searx from single id, for every url of an id it creates a line in format (id, url)
# map 1 calculate the rank for each domain
# sortBy 1 sort the RDD based on its rank
output = input.flatMap(lambda id: ((id,searx_aux.searx_request(id,pageno)) for pageno in range(5)))\
    .flatMap(lambda (id,response): (parsing_aux.find_domain_url(id, result) for result in response['results']))\
    .groupByKey()\
    .map(lambda (domain,values): (domain,{'pages':list(values),'score':len(list(values))/seedLength}))\
    .filter(lambda (domain,values): values['score']>0.009)\
    .sortBy(lambda (domain,values): -values['score'])

output.saveAsTextFile('hdfs:///user/maria_dev/data/results')

sc.stop()