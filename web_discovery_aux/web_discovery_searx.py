from pyspark import SparkContext, SparkConf
from web_discovery_aux import parsing_aux,searx_aux

#Input path to id file, sparc context, number of pages for searx, boolean to save the file on hdfs
# path where to save files
def web_discovery_with_searx(path,sc,num_of_pages,save, path_to_save_sites):
    input = sc.textFile(path)

    # FlatMap 1 Function that returns all urls from the results of Searx from single id, for every url of an id it creates a line in format (id, url)
    # FlatMap 2 it takes each line with (id,response) and produce a couple (domain, (id,page))
    # Map1  it takes (domain, [(id1,page1),(id2,page2)...]) -----> (domain, {'pages': [(id1,page1),(id2,page2)...], 'home_page_clean_text': ['token1','token2',...]})
    # the above pages are only the "last part" of the url, the one witouth the domain
    output = input.flatMap(lambda id: ((id, searx_aux.searx_request(id, pageno)) for pageno in range(num_of_pages))) \
        .flatMap(lambda (id, response): (parsing_aux.find_domain_url(id, page) for page in response['results'])) \
        .groupByKey() \
        .map(lambda (domain, values): (domain, {'pages': list(values), 'home_page_clean_text': parsing_aux.extract_clean_text_from_page(domain)})) \
        .filter(lambda (domain, values): isinstance(values['home_page_clean_text'], list)) \
        .filter(lambda (domain, values): len(values['home_page_clean_text']) > 2)

    if (save):
        output.saveAsTextFile(path_to_save_sites)

    return output
