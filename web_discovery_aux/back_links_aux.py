from bs4 import BeautifulSoup
import urllib2
import re


def getLinks(url):
    links = []
    try:
        print('tentativo per la pagina: ' + url.encode('utf-8'))
        html_page = urllib2.urlopen(url)
        soup = BeautifulSoup(html_page)
    except:
        print('problemi nel prendere la pagina')
        return links
    try:
        for link in soup.findAll('a', attrs={'href': re.compile("^http://")}):
            links.append(link.get('href'))
    except:
        print('problemi nel trovare i link')
        return links
    return links