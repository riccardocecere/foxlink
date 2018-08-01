from bs4 import BeautifulSoup
from urlparse import urldefrag
import re

def extract_relevant_links(html_page, domain):

    html_body = BeautifulSoup(html_page, 'html.parser').body
    all_links = [url['href'] for url in html_body.find_all("a") if 'href' in url.attrs]
    in_site_links = [link for link in all_links if link and (domain in link)]
    # heuristically remove whishlist / cart links
    defrag_links = [urldefrag(link)[0] for link in in_site_links]
    relevants = [link for link in defrag_links if not re.search('(W|w)ishlist', link) and not re.search('(C|c)art', link) and not re.search('(C|c)ompare', link)]
    return relevants
