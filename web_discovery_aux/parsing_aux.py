# -*- coding: utf-8 -*-

import re
import requests
import pattern.web,nltk


#Function to verify if a domain is present in the given url
def find_domain_in_url(domain,url):
    return bool(re.match('.*://[www.]*'+domain, url))

#Remove http*://www in case of domain like http://www.egyp.it and produce output in form egyp.it
def remove_www_domain(domain):

    if domain == None or domain == '':
        return 'ERROR_DOMAIN'

    domain_without_www = domain.split('//')
    domain_without_www = domain_without_www[1]

    if 'www.' in domain_without_www:
        domain_without_www = domain_without_www[4:]

    return domain_without_www


# Add www in case of domain like http://egyp.it and produce output in form http://www.egyp.it
def add_www_domain(domain):

    if domain == None or domain == '':
        return 'ERROR_DOMAIN'

    # if .www is already present return the domain
    if '://www.' in domain:
        return domain

    # Split the domain on //, (creating an array) the insert //www.
    domain_with_www = domain.split('//')
    domain_with_www.insert(1,'//www.')

    # Join the array to return the domain including www
    return ''.join(domain_with_www)


# Function that take an url and extract the domain e.g. wwww.vapur.it/rko ----> www.vapur.it
def find_domain_url(url):

        if url==None or url== '':
            return 'ERROR_DOMAIN'

        try:
            return re.findall(r'.*://.*?/', url)[0][:-1]
        except:
            return str(url)


# Function that takes id and a result in a response to generate  (domain, (page,id))
def domain2page_id(id, url):

    # Check the id
    if id == None or id == '':
        return (id,'ERROR://empty_query')

    try:
        # Use a regex to extract the domain from url
        domain = find_domain_url(url['url'])

        # Use a regex to extract only the 'final' part of url page es: https://amazon.com/vapur ----> vapur
        page = re.sub('.*://.*?/', '', url['url'])
        return (domain, (page,id))

    except:
        print 'ERRORE: '+str(url)
        return (id,'ERROR://parsing_problem')



#function to extract clean text from html pages
def extract_clean_text_from_page(url):
    if url == None or url == '':
        return 'ERROR: empty url'
    try:
        #Take html of the page
        html = requests.get(str(url), timeout=5).text
    except:
        print 'error to take the page: '+str(url)
        return 'ERROR:to take the page'
    try:
        if html == None or html == '':
            return 'ERROR: extraction html'
        #Remove all html tags
        home_page_text = pattern.web.plaintext(html)
    except:
        print 'error extracting plain text from page: '+str(url)
        return 'ERROR: extracting plain text'

    if home_page_text == '' or home_page_text== None:
        return 'ERROR: before tokenization'
    try:
        # Remove
        home_page_text = re.sub("[^A-Za-z0-9' ']", "", home_page_text).lower()
    except:
        return 'ERROR: removing special charachter'

    try:
        # Tokenization of the words
        tokens = nltk.word_tokenize(home_page_text)

        #Remove all the non words
        words = [word for word in tokens if word.isalpha()]
    except:
        return 'ERROR: during tokenization'

    try:
        stop_words = set(nltk.corpus.stopwords.words('english'))
        words = [w for w in words if not w in stop_words]
    except:
        return 'ERROR: during stopwords process'

    try:
        #Stemming
        porter = nltk.stem.PorterStemmer()
        stemmed = [porter.stem(word) for word in words]
    except:
        return 'ERROR: during stemming process'

    return stemmed


# Function to select if a warc is useful based on it's name that must be in the form crawl-data/CC-MAIN-2018-22/segments/1526794863689.50/warc/CC-MAIN-20180520205455-20180520225455-00609.warc.gz
# it's important that has /warc/ inside the string
def correct_warc_name(warcname):
    if warcname == '' or warcname == None:
        return False
    return re.match('crawl.*\/warc\/.*gz',warcname)

