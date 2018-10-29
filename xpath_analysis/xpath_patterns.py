# -*- coding: utf-8 -*-
from mongodb_middleware import mongodb_interface
from lxml import etree,html
from web_discovery import parser
import re, ast, itertools
from general_utils import rdd_utils


# Function to calculate xpath sequencies
def xpath_sequencies(clusters_rdd, save_xpath, path_to_save_xpath):

    clusters_rdd = clusters_rdd\
                    .map(lambda row: (row['domain'],(row['url'],row['cluster_label'],row['referring_url'])))\
                    .map(lambda (domain,cluster_element):(domain,(cluster_element[0],url_2_xpath(domain,cluster_element,[]),cluster_element[1],cluster_element[2])))\
                    .filter(lambda (domain,values): values[1]!=[] and values[1]!=None)\
                    .flatMap(lambda (domain,(url,xpath_list,label,referring_url)):((str(xpath)+'/----/'+str(domain),(url,label,referring_url)) for xpath in xpath_list))\
                    .groupByKey()\
                    .mapValues(set).mapValues(list)\
                    .map(lambda (key,values): (key.split('/----/')[0],(key.split('/----/')[1],values)))\
                    .flatMap(lambda (xpath,(domain,urls)):((xpath,(domain,url)) for url in urls))\
                    .map(lambda (xpath,(domain,url)): (str(domain)+'/----/'+str(url[1]),(ast.literal_eval(xpath),url[0],url[2])))\
                    .groupByKey()\
                    .map(lambda (key,values):((key.split('/----/')[0],key.split('/----/')[1]),list(values)))

    rdd_utils.save_rdd(clusters_rdd, save_xpath, path_to_save_xpath)
    return clusters_rdd

# Function to calculate the sequence of xpaths from home page to the given url
def url_2_xpath(domain,cluster_element,xpath_list,number_of_recursions=9):
    if number_of_recursions==1 or (domain == cluster_element[0] and domain == cluster_element[2]) or ( 'http://'+parser.remove_www_domain(domain)== cluster_element[0] and 'http://'+parser.remove_www_domain(domain) == cluster_element[2])or ( 'https://'+parser.remove_www_domain(domain)== cluster_element[0] and 'https://'+parser.remove_www_domain(domain) == cluster_element[2]):
        result = []
        for element in itertools.product(*xpath_list):
            l = list(element)
            l.reverse()
            result.append(l)
        return result
    try:
        html_text = mongodb_interface.get_html_page(domain, cluster_element[2])
        root = html.fromstring(html_text)
        current_xpath_list = []
    except:
        return None
    tree = etree.ElementTree(root)
    for e in root.iter():
        if e.get('href') == str(cluster_element[0]) or e.get('href') == str('/'+re.sub('.*://.*?/','',str(cluster_element[0]))):
            current_xpath_list.append(tree.getpath(e))

    xpath_list.append(current_xpath_list)
    parent_cluster_element=(cluster_element[2],mongodb_interface.get_depth_level(domain,cluster_element[2]),mongodb_interface.get_referring_url(domain,cluster_element[2]))
    final_result_list = url_2_xpath(domain,parent_cluster_element,xpath_list,number_of_recursions-1)
    return final_result_list


# Generalize all the xpath expressions
def generalize_xpath(sc, xpath_rdd, save_xpath, path_to_save_xpath):
    output = xpath_rdd.filter(lambda (key, values): values != None and values != []) \
        .flatMap(lambda ((domain, label), values): (((domain, label), value) for value in values)) \
        .filter(lambda ((domain, label), value): value != None and value != [] and len(value) > 1 and value != {}) \
        .map(lambda ((domain, label), value): slice_xpath_sequence_to_last_step(domain, label, value)) \
        .filter(lambda ((domain, label), value): value != None and value != [] and value != '' and value != {}) \
        .map(lambda ((domain, label), (xpath_list, last_xpath, url, reffering_url)): ((str(domain) + '/----/' + str(label) + '/----/' + str(xpath_list)), (last_xpath, url, reffering_url))) \
        .groupByKey() \
        .map(lambda (key, value): ((key.split('/----/')[0], key.split('/----/')[1], key.split('/----/')[2]), list(value))) \
        .mapValues(xpath_index_generalization) \
        .filter(lambda ((domain, label, xpath), (generalized_xpath, xpaths, url2ref)): len(generalized_xpath) > 0) \
        .map(lambda ((domain, label, xpath), (generalized_xpath, xpaths, url2ref)): ((domain, label, xpath), (generalized_xpath, len(xpaths), xpaths, url2ref)))

    rdd_utils.save_rdd(output, save_xpath, path_to_save_xpath)

# Generalize the entire xpath expression
def xpath_max_generalization(generalized_xpath):
    path_dict = {}
    output = []

    for path in generalized_xpath:
        current_path = re.sub('\[[0-9]*\]|\[[\*]*\]', '', path)
        if current_path in path_dict:
            path_dict[current_path].append(path)
        else:
            path_dict[current_path] = [path]
    for path in path_dict:
        max_count = max(path_dict[path], key=lambda x: x.count('*'))
        output.append(max_count)
    return output

# Generalize a specific index in the xpath expression
def xpath_index_generalization(xpath_list):
    xpaths = []
    url2ref = []
    generalized_xpath = []
    for xpath in xpath_list:
        xpaths.append(xpath[0])
        url2ref.append((xpath[1], xpath[2]))
    for xpath in xpaths:
        for xpath2 in xpaths:
            if xpath != xpath2 and len(xpath) == len(xpath2):
                xpath_tags_1 = xpath.split('/')[1:]
                xpath_tags_2 = xpath2.split('/')[1:]
                n = len(xpath_tags_1)
                change_indexes = []
                for index in range(0, n):
                    try:
                        if xpath_tags_1[index] != xpath_tags_2[index]:
                            change_indexes.append(index)
                    except:
                        continue
                for index in change_indexes:
                    xpath_tags_1[index] = re.sub('\[.*\]', '[*]', xpath_tags_1[index])
                    generalized_xpath.append('/' + '/'.join(xpath_tags_1))

    generalized_xpath = list(set(generalized_xpath))
    generalized_xpath = list(set(xpath_max_generalization(generalized_xpath)))
    return (generalized_xpath, xpaths, url2ref)

# It returns the xpaths in this form ([xpath1,xpath2,...,xpathN-1],xpathN,url,referring_url)
def slice_xpath_sequence_to_last_step(domain, label, value):
    try:
        output = (value[0][:len(value[0]) - 1], value[0][-1], value[1], value[2])
        return ((domain,label),output)
    except:
        print value
        return ((domain,label),None)