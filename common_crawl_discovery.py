from pyspark import SparkContext, SparkConf
from web_discovery_aux import common_crawl_aux

# Set context
conf = SparkConf().setAppName('product_finder')
sc = SparkContext(conf=conf)

# Take input from file on hdfs
path = 'hdfs:///user/maria_dev/data/siti_dexter.txt'
input = sc.textFile(path)


# map 1 for every url it makes a query to the common crawl repository ----> (url, response)
# flatMap 1 for every warcs returned in the response it will create a key associate with the url value, it creates couple with (warcname, url)
# map 2 transform the value in a json with two keys, the length of the list of urls associate with a single warcfile and the list itself
warc_urls = input.map(lambda url:common_crawl_aux.warc_discovery(url))\
    .flatMap(lambda (url,response): (common_crawl_aux.id_warc_selection(url,single_response) for single_response in response))\
    .groupByKey()\
    .mapValues(list)\
    .map(lambda (warc_file_name,urls):(warc_file_name,{'num_of_urls':len(urls),'urls':urls}))

warc_urls.persist()


print('NUMBER OF WARC FILE FOUND: '+str(warc_urls.count()))

warc_urls.saveAsTextFile('hdfs:///user/maria_dev/data/results')

sc.stop()