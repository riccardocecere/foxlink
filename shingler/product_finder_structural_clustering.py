from shingler import structural_clustering
from mongodb_middleware import mongodb_interface

# It returns (domain, [(shingle_vector, [url1,url2,...],....)])
def all_sites_structural_clustering(sc, domains, save, path_to_save,thresold):
    output = domains.map(lambda domain: (domain, structural_clustering.structural_clustering(mongodb_interface.get_collection(domain),thresold)))

    if save and path_to_save != '' and path_to_save != None:
        output.saveAsTextFile(path_to_save)
    return output