import requests

# Function that takes an id and makes a request to the meta-search engine to find pages related to that id.
# Pageno is the number of result pages of searx to return.
def searx_request(id,pageno):

    if id == None or id =='':
        return {'results':[{'url':'ERROR://empty_query/','error':'empty query'}]}

    try:
        pageno += 1
        response = requests.get("http://172.17.0.4:8888/?format=json&pageno="+str(pageno)+"&engines=yahoo,bing,duckduckgo,qwant,faroo,swisscows&q="+str(id))
        response = response.json()
        return response

    except:
        return {'results':[{'url':'ERROR://searx_error/','id':id,'error':'searx error'}]}