
import requests, json
from web_discovery_aux import parsing_aux


# Function to query common crawl and discover the useful warc for the given url
def warc_discovery(url,snapshot):
    try:
        response = requests.get('http://index.commoncrawl.org/'+str(snapshot)+'-index?url='+ url + '&output=json')
        response = response.text
        response = response.split('\n')
        return (url, response)
    except:
        return ("Errors", url)

    # given the url and the single_response from common crawl it finds the warc filename, it returns couple (warc_name,url),


# where the url is the original used to query common crawl
def id_single_warc_selection(url, single_response):
    try:
        single_response = json.loads(single_response)
        warc_name = single_response['filename']
        return (warc_name, url)
    except:
        return ('Errors', url)

# where the url is the original used to query common crawl
# it return (urls, [warcs_names])
def id_multiple_warc_selection(url, response):
    warcs = []
    for single_response in response:
        try:
            single_response = json.loads(single_response)
            warc_name = single_response['filename']
            if parsing_aux.correct_warc_name(warc_name):
                warcs.append(warc_name)
        except:
            continue
    return (url,warcs)
