import requests,re
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('ProvaSpark')
sc = SparkContext(conf=conf)

path = 'hdfs:///user/maria_dev/data/test.txt'
input = sc.textFile(path)

def searx_request(id):
    if id == None or id =='':
        return {'results':[{'url':'ERROR://emptyquery/','error':'empty query'}]}
    response = requests.get("http://172.17.0.3:8888/?format=json&engines=yahoo,bing,duckduckgo,qwant,faroo,swisscows&q=" + str(id))
    try:
        response = response.json()
        return response
    except:
        return {'results':[{'url':'ERROR://searxerror/','id':id,'error':'searx error'}]}


input.persist()
seedLength = float(input.count())

# Function that returns all urls from the results of Searx from single id, for every url of an id it create a line is in format (id, url)
output = input.map(lambda id: (id,searx_request(id)))\
    .flatMap(lambda (id,response): ((re.findall(r'.*://.*?/',result['url'])[0][:-1],(re.sub('.*://.*?/', '', result['url']),id)) for result in response['results']))\
    .groupByKey()\
    .mapValues(list)\
    .map(lambda (domain,values): (domain,(values,len(values)/seedLength)))\
    .sortBy(lambda (domain,(values,rank)): -rank)
output.saveAsTextFile('hdfs:///user/maria_dev/data/results')

sc.stop()