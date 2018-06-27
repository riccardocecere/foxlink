import re
import requests
import pattern.web


# Function that takes id and a result in a response to generate  (domain, (page,id))

def find_domain_url(id, result):

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


#function to extract clean text from html pages
def extract_clean_text_from_page(url):
    try:
        html = requests.get(str(url)).text
    except:
        print 'error to take the page: '+str(url)
        return 'ERROR:to take the page'
    try:
        result = pattern.web.plaintext(html).encode('utf-8','ignore')
    except:
        print 'error extracting plain text from page: '+str(url)
        return 'ERROR: extracting plainj text'
    print 'text extracted from page: '+str(url)
    return result

# Function to select if a warc is useful based on it's name that must be in the form crawl-data/CC-MAIN-2018-22/segments/1526794863689.50/warc/CC-MAIN-20180520205455-20180520225455-00609.warc.gz
# it's important that has /warc/ inside the string

def correct_warc_name(warcname):
    if warcname == '' or warcname == None:
        return False
    return re.match('crawl.*\/warc\/.*gz',warcname)

