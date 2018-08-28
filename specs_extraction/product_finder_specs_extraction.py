# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from collections import defaultdict
from mongodb_middleware import mongodb_interface
from bs4 import BeautifulSoup

#Method that start the extraction of the specs from the page, it takes in input an rdd in the form {(domain1, [(url1,degre1),...]),...}
def start_specs_extraction(sc,in_degrees_rdd,threshold,save,path_to_save):
    output = in_degrees_rdd.flatMap(lambda (domain,list_degrees): ((url,(domain,degree)) for (url,degree) in list_degrees))\
            .filter(lambda (url,(domain,degree)): float(degree)>float(threshold))\
            .map(lambda (url,(domain,degree)): (url,extract_specs(domain,url)))\
            .filter(lambda (url,element): element != [] and element[1][0] > 6)
    if save and path_to_save != '' and path_to_save != None:
        output.saveAsTextFile(path_to_save)
    return output

# Function that extract the area of the page that has the specific of the product
# It can be a table or a list, depending on the score
# It takes in input the domain and the url of the page
def extract_specs(domain, url):


    key_words_camera = ['sensor','iso','frame','focal length','lens','angle','megapixel','viewfinder','magnification','eyepoint','diopter','focus','reflex','mirror','metering','exposure','flash','price','specification','$','£','product','detail','aspect ratio','shutter','sensitivity','white balance','shoot','resolution','metering','stabilisation','zoom','aperture','macro','ovf','uw depth','fps','evf','range','pixel']

    try:
        page = mongodb_interface.get_html_page(domain, url)
        page = BeautifulSoup(page,'html.parser')
        tables = page.find_all("table")

        #takes bot ordered and unordered lists
        unordered_lists = page.find_all("ul")
        ordered_lists = page.find_all("ol")

        tables_and_lists = tables+unordered_lists+ordered_lists


        most_probable_element = most_probable_spec_table_or_list(tables_and_lists, key_words_camera)
        return most_probable_element

    except:
        return []

#Function that calculate the most probable part of html, table or list that contains the specifics

def most_probable_spec_table_or_list(tables_and_lists, key_words):

    tables_and_lists_scored = {}

    #if tables is empy return a dummy result
    if tables_and_lists == []:
        return []

    for element in tables_and_lists:

        try:

            # Extract first the clean text of the element
            #element_clean_text = str(BeautifulSoup(element).get_text()).lower()

            #print'---------ELEMENT------------'
            #print str(element)
            #print'----------------------'
            element_clean_text = BeautifulSoup(str(element),'html.parser')

            #print'---------BF------------'
            #print str(element_clean_text)
            #print'----------------------'
            element_clean_text = element_clean_text.get_text()

            #print'----------TEXT---------'
            #print element_clean_text.encode('utf-8')
            #print'----------------------'

            element_clean_text = element_clean_text.encode('utf-8')
            element_clean_text = element_clean_text.lower()
            for key_word in key_words:
                #check if the word is in the table/list clean text
                if key_word in element_clean_text:
                    if element in tables_and_lists_scored:
                        score,hits = tables_and_lists_scored[element]
                        score += 1
                        hits.append(key_word)
                        tables_and_lists_scored[element] = (score,hits)
                    else:
                        tables_and_lists_scored[element] = (1,[key_word])

        except:
            return []

    #Order the list based on the scoring
    tables_and_lists_scored = sorted(tables_and_lists_scored.items(), key = lambda tup: tup[1][0], reverse=True)
    return tables_and_lists_scored[0]



'''

    tables = []

    # Parse the html body in html, it suppose to receive directly the body
    html_body = BeautifulSoup(page, 'html.parser')

    if html_body == None or html_body =='':
        return []
    # Find all the links on the page
    all_links = [url['href'] for url in html_body.find_all("a") if 'href' in url.attrs]

    # Take only the links 'inside' the site
    in_site_links = [link for link in all_links if link and (parsing_aux.find_domain_in_url(domain,link))]

    # heuristically remove whishlist / cart links
    defrag_links = [urldefrag(link)[0] for link in in_site_links]
    relevants = [link for link in defrag_links if not re.search('(W|w)ishlist', link) and not re.search('(C|c)art', link) and not re.search('(C|c)ompare', link)]
    

'''