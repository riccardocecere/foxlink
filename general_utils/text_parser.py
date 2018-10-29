# -*- coding: utf-8 -*-

import pattern.web,requests,re
from bs4 import Comment

#Function to verify if a domain is present in the given url
def find_domain_in_url(domain,url):
    return bool(re.match('.*://[www.]*'+domain, url))

#Function to verify if the link brings to a page of the same site
def in_site_links(domain,url):
    return find_domain_in_url(domain,url) or bool(re.match('^/.*', url)) or bool(re.match('^\./.*', url))

#Obtain the complete link adding http or https to the given url
def normalize_link(url,domain):

    if bool(re.match('^/{2}.*', url)):
        if bool(re.match('^https', domain)):
            return str('https:')+str(url)
        else:
            return str('http:') + str(url)

    if bool(re.match('^/.*', url)):
        return str(domain)+str(url)

    if bool(re.match('^\./.*', url)):
        return str(domain)+str(url)[1:]

    return url

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

    # Split the domain on //, (creating an array) than insert //www.
    domain_with_www = domain.split('//')
    domain_with_www.insert(1,'//www.')

    # Join the array to return the domain including www
    return ''.join(domain_with_www)


# Function that take an url and extract the domain e.g. wwww.vapur.it/rko ----> www.vapur.it
def extract_domain_from_url(url):

        if url==None or url== '':
            return 'ERROR_DOMAIN'

        try:
            return re.findall(r'.*://.*?/', url)[0][:-1]
        except:
            return str(url)

# Extract only the 'final' part of url page es: https://amazon.com/vapur ----> vapur
def cut_domain_from_url(url):
    if url == None or url == '':
        return 'ERROR_DOMAIN'
    try:
        return re.sub('.*://.*?/', '', url)
    except:
        return str(url)


# Function that takes id and a result in a response to generate  (domain, (page,id))
def domain2page_id(id, response):

    # Check the id
    if id == None or id == '':
        return (id,'ERROR://empty_query')

    try:
        # Use a regex to separate domain and the rest of the url
        domain = extract_domain_from_url(response['url'])
        page = cut_domain_from_url(response['url'])
        return (domain, (page,id))

    except:
        print 'ERROR: '+str(response)
        return (id,'ERROR://parsing_problem')


# Get clean text from html taken from given url
def get_html_text(url):
    if url == None or url == '':
        return 'Error'
    try:
        return pattern.web.plaintext(requests.get(url,timeout=5).text)
    except:
        return 'Error'

#Get clean text from given html
def get_clean_text_from_html(html):
    if html == None or html == '':
        return 'Error'
    try:
        return pattern.web.plaintext(html)
    except:
        return 'Error'

# Verify if an element is not under an invisible tag or a comment
def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True

# Extract all text of visible tags
def extract_text(page):
    texts = page.findAll(text=True)
    visible_texts = filter(tag_visible, texts)
    return " ".join(t.strip() for t in visible_texts)

