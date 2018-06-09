import requests,json

# Function to query common crawl and discover the useful warc for the given url
def warc_discovery(url):
    try:
        response = requests.get('http://index.commoncrawl.org/CC-MAIN-2018-22-index?url=' + url + '&output=json')
        response = response.text
        response = response.split('\n')
        return (url,response)
    except:
        return ("Errors", url)

# given the url and the single_response from common crawl it finds the warc filename, it returns couple (warc_name,url),
# where the url is the original used to query common crawl
def id_warc_selection(url,single_response):
    try:
        single_response = json.loads(single_response)
        warc_name = single_response['filename']
        return (warc_name,url)
    except:
        return ('Errors',url)