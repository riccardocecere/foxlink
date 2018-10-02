import itertools
from mongodb_middleware import mongodb_interface
from web_discovery_aux import parsing_aux

def extract_labelled_url(labelled_clusters):
    labelled_dictionary = {}
    labelled_dictionary['products'] = []
    labelled_dictionary['indexes'] = []

    for cluster in labelled_clusters:
        if cluster['label'] == 'products':
            labelled_dictionary['products'].append(cluster['url_list'])
        else:
            labelled_dictionary['indexes'].append(cluster['url_list'])

    labelled_dictionary['products'] = list(itertools.chain.from_iterable(labelled_dictionary['products']))
    labelled_dictionary['indexes'] = list(itertools.chain.from_iterable(labelled_dictionary['indexes']))
    return labelled_dictionary


def tokenize_html_pages(domain,labbeled_urls):
    result_list = []

    for label in labbeled_urls:
        labbeled_list = labbeled_urls[label]

        #Todo cancellare assolutamente solo test
        labbeled_list = labbeled_list[:100]
        labbeled_list.append('https://www.ritzcamera.com/product/TNAFA010N700.htm')
        for url in labbeled_list:
            html_page = mongodb_interface.get_html_page(str(domain), str(url))

            if html_page != None and html_page != '':
                tokens = parsing_aux.extract_clean_text_from_html_page(html_page)

                if isinstance(tokens,list):
                    result_list.append((url,tokens,label))

    return result_list






def prepare_input_for_classifier(sc,referring_url_metrics,save_labelled_url, path_to_labelled_url):

    output = referring_url_metrics.mapValues(extract_labelled_url)


    output = output.map(lambda (domain, labelled_urls): (domain, tokenize_html_pages(domain, labelled_urls))) \
            .flatMap(lambda (domain, tokenized_url_with_label): (((domain, label, url), tokens) for (url, tokens, label) in tokenized_url_with_label))

    return output
