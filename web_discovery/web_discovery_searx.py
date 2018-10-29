from web_discovery import searx
from general_utils import rdd_utils, text_parser


# Input path to id file, spark context, number of pages for searx, boolean to save the file on hdfs,
# path where to save files
def web_discovery_with_searx(path,sc,num_of_pages,save, path_to_save_sites):

    input = sc.textFile(path)

    # FlatMap 1 Function that returns all urls from the results of Searx from single id, for every url of an id it creates a line in format (id, url)
    # FlatMap 2 it takes each line with (id,response) and produce a couple (domain, (id,page))
    # the above pages are only the "last part" of the url, the one without the domain
    output = input.flatMap(lambda id: ((id, searx.searx_request(id, pageno)) for pageno in range(num_of_pages))) \
        .flatMap(lambda (id, response): (text_parser.domain2page_id(id, page) for page in response['results'])) \
        .groupByKey().mapValues(list)

    rdd_utils.save_rdd(output,save,path_to_save_sites)

    return output
