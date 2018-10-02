from pyspark import SparkContext, SparkConf
from web_discovery_aux import searx_aux, back_links_aux



conf = SparkConf().setAppName('product_finder')
sc = SparkContext(conf=conf)

path = 'hdfs:///user/maria_dev/data/test_siti_prodotti_dexter.txt'
input = sc.textFile(path)

output = input.flatMap(lambda link: (searx_aux.searx_request(link,pageno) for pageno in range (0,1)))\
    .flatMap(lambda response: ((result['url'],back_links_aux.getLinks(result['url'])) for result in response['results']))

output.saveAsTextFile('hdfs:///user/maria_dev/data/results')

sc.stop()