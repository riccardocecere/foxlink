from bs4 import BeautifulSoup
from urlparse import urldefrag
import re
from web_discovery_aux import parsing_aux

# Function that takes the body of n html page and the domain
# It returns all the relevant links on the page
# A relevant link is a link that brings in another page of the same domain
def extract_relevant_links(html_page, domain):

    # Parse the html body in html
    html_body = BeautifulSoup(html_page, 'html.parser').body

    if html_body == None or html_body =='':
        return []
    # Find all the links on the page
    all_links = [url['href'] for url in html_body.find_all("a") if 'href' in url.attrs]

    # Take only the links 'inside' the site
    in_site_links = [link for link in all_links if link and (parsing_aux.find_domain_in_url(domain,link))]
    # heuristically remove whishlist / cart links
    defrag_links = [urldefrag(link)[0] for link in in_site_links]
    relevants = [link for link in defrag_links if not re.search('(W|w)ishlist', link) and not re.search('(C|c)art', link) and not re.search('(C|c)ompare', link)]
    return relevants
