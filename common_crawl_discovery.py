from pyspark import SparkContext, SparkConf
from web_discovery_aux import common_crawl_aux,parsing_aux


# Set context
conf = SparkConf().setAppName('product_finder')
sc = SparkContext(conf=conf)

# Take inputs from file on hdfs
path = 'hdfs:///user/maria_dev/data/lista_siti_prodotti_dexter.txt'
path_common_crawl = 'hdfs:///user/maria_dev/data/common_crawl_snapshots.txt'

input = sc.textFile(path)
common_crawl_snapshots = sc.textFile(path_common_crawl)


print '---------------------------------------'
print(common_crawl_snapshots)
print '---------------------------------------'

# cartesian1 to create a new rdd with the cartesian product betweeen input and common_crawl_snapshots, in the form (url, snapshotname)
# map 1 for every url it makes a query to the common crawl repository ----> (url, response)
# flatMap 1 for every warcs returned in the response it will create a key associate with the url value, it creates couple with (warcname, url)
# filter1 filters out all the warc name with the incorrect name, since they are not useful for analysis
# map 2 transform the value in a json with two keys, the length of the list of urls associate with a single warcfile and the list itself
# sort for the num_of_urls
warc_urls = input.cartesian(common_crawl_snapshots)\
    .map(lambda (url,snapshot):common_crawl_aux.warc_discovery(url,snapshot))\
    .flatMap(lambda (url,response): (common_crawl_aux.id_warc_selection(url,single_response) for single_response in response))\
    .filter(lambda (warcname,url): parsing_aux.correct_warc_name(warcname))\
    .groupByKey()\
    .mapValues(list)\
    .map(lambda (warc_file_name,urls):(warc_file_name,{'num_of_urls':len(urls),'urls':urls}))\
    .sortBy(lambda (warc_file_name,dict_urls): desc(dict_urls['num_of_urls']))



warc_urls.persist()


print('NUMBER OF WARC FILE FOUND: '+str(warc_urls.count()))

warc_urls.saveAsTextFile('hdfs:///user/maria_dev/data/results')

sc.stop()