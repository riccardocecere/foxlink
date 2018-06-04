import requests,re
from pyspark import SparkContext, SparkConf



# Function that takes an id and mades a request to the meta searx engine to find page related to that id
def searx_request(id):

    if id == None or id =='':
        return {'results':[{'url':'ERROR://empty_query/','error':'empty query'}]}

    try:
        response = requests.get("http://172.17.0.3:8888/?format=json&engines=yahoo,bing,duckduckgo,qwant,faroo,swisscows&q="+str(id))
        response = response.json()
        return response

    except:
        return {'results':[{'url':'ERROR://searx_error/','id':id,'error':'searx error'}]}


# Function that takes id and a result in a response to generate  (domain, (page,id))
def find_domain_urls(id, result):

    # Check the id
    if id == None or id == '':
        return (id,'ERROR://empty_query')

    try:
        # Use a regex to extract the domain from url
        domain = re.findall(r'.*://.*?/',result['url'])[0][:-1]

        # Use a regex to extract only the 'final' part of url page es: https://amazon.com/vapur ----> vapur
        page = re.sub('.*://.*?/', '', result['url'])
        return (domain, (page,id))

    except:
        return (id,'ERROR://parsing_problem')



conf = SparkConf().setAppName('product_finder')
sc = SparkContext(conf=conf)

path = 'hdfs:///user/maria_dev/data/after_cleaning.txt'
input = sc.textFile(path)

input.persist()
seedLength = float(input.count())

# FlatMap 1 Function that returns all urls from the results of Searx from single id, for every url of an id it creates a line in format (id, url)
# map 1 calculate the rank for each domain
# sortBy 1 sort the RDD based on its rank
output = input.map(lambda id: (id,searx_request(id)))\
    .flatMap(lambda (id,response): (find_domain_urls(id, result) for result in response['results']))\
    .groupByKey()\
    .map(lambda (domain,values): (domain,(list(values),len(list(values))/seedLength)))\
    .filter(lambda (domain,(values,rank)): rank>0.088)\
    .sortBy(lambda (domain,(values,rank)): -rank)

output.saveAsTextFile('hdfs:///user/maria_dev/data/results')

sc.stop()