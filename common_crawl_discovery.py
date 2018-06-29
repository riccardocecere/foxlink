from pyspark import SparkContext, SparkConf
from web_discovery_aux import common_crawl_aux,parsing_aux


# Set context
conf = SparkConf().setAppName('product_finder')
sc = SparkContext(conf=conf)

# Take inputs from file on hdfs
path = 'hdfs:///user/maria_dev/data/cameras.csv'
path_common_crawl = 'hdfs:///user/maria_dev/data/common_crawl_snapshots.txt'

site_list = sc.textFile(path)
common_crawl_snapshots = sc.textFile(path_common_crawl)

#Remove the header from csv and remove the ',' at the end of each url
header = site_list.first()
site_list = site_list.filter(lambda row: row != header)\
    .map(lambda row: row[:-1])

# cartesian1 to create a new rdd with the cartesian product betweeen input and common_crawl_snapshots, in the form (url, snapshotname)
# map 1 for every url it makes a query to the common crawl repository ----> (url, response)
# map 2 for every warcs returned in the response it will create a key associate with the url value, it creates couple with (url, warcs)
# reduceByKey aggregate all the same keys (url) and combine the list of warcs (making the set)
# map 3 transform the value in a json with two keys, the length of the list of warc name associate with a single url and the list itself
# filter out all the value with less than 1 num_of_warcs, it means that the url is not on common crawl, or there were some errors
# sort for the num_of_warcs
url_warcs = site_list.cartesian(common_crawl_snapshots)\
    .map(lambda (url,snapshot):common_crawl_aux.warc_discovery(url,snapshot))\
    .map(lambda (url,response): common_crawl_aux.id_multiple_warc_selection(url,response)) \
    .mapValues(list) \
    .reduceByKey(lambda warcs1,warcs2: list(set(list(warcs1+warcs2))))\
    .map(lambda (url, warcs): (url, {'num_of_warcs': len(warcs), 'warc_names': warcs}))\
    .filter(lambda (url,warcs):warcs['num_of_warcs']>0)\
    .sortBy((lambda (url,dict_warcs): dict_warcs['num_of_warcs']),False)

#Calculate the sum of warcs necessary to be downloaded
count_warcs = url_warcs
count_warcs = count_warcs.values()\
    .map(lambda dict_warcs: dict_warcs['num_of_warcs'])\
    .sum()

print '------------------------'
print 'Total number of warcs: '+str(count_warcs)
print '------------------------'
url_warcs.saveAsTextFile('hdfs:///user/maria_dev/data/results')

sc.stop()