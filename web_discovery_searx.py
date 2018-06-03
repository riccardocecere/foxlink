import requests,re
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('ProvaSpark')
sc = SparkContext(conf=conf)

path = 'hdfs:///user/maria_dev/data/after_cleaning.txt'
input = sc.textFile(path)

# Function that takes an id and mades a request to the meta searx engine to find page related to that id
def searx_request(id):
    if id == None or id =='':
        return {'results':[{'url':'ERROR://emptyquery/','error':'empty query'}]}
    response = requests.get("http://172.17.0.3:8888/?format=json&engines=yahoo,bing,duckduckgo,qwant,faroo,swisscows&q=" + str(id))
    try:
        response = response.json()
        return response
    except:
        return {'results':[{'url':'ERROR://searxerror/','id':id,'error':'searx error'}]}

# Qui potevo mettere un for interno? problema con il return
# It takes id and a result in a response to generate  domain, (page,id)
def find_domain_urls(id, result):

    # Check the id
    if id == None or id == '':
        return (id,'ERROR://emptyquery')
    try:
        # Use a regex for extract the domain from url
        domain = re.findall(r'.*://.*?/',result['url'])[0][:-1]

        # Use a regex for extract only the 'final' part of url page es: https://amazon.com/vapur ----> vapur
        page = re.sub('.*://.*?/', '', result['url'])
        return (domain, (page,id))
    except:
        return (id,'ERROR://parsingProblem')


input.persist()
seedLength = float(input.count())

# FlatMap 1 Function that returns all urls from the results of Searx from single id, for every url of an id it create a line is in format (id, url)
# map 1 calculate the rank for each domain
# sortBy 1 sort the RDD based on its rank
output = input.map(lambda id: (id,searx_request(id)))\
    .flatMap(lambda (id,response): ((find_domain_urls(id, result)) for result in response['results']))\
    .groupByKey()\
    .map(lambda (domain,values): (domain,(list(values),len(list(values))/seedLength)))\
    .sortBy(lambda (domain,(values,rank)): -rank)
output.saveAsTextFile('hdfs:///user/maria_dev/data/results')


sc.stop()