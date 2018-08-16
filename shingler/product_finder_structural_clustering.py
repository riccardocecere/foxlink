from shingler import structural_clustering
from mongodb_middleware import mongodb_interface

def all_sites_structural_clustering(sc, domains, save, path_to_save):
    output = domains.map(lambda domain: (domain, structural_clustering.structural_clustering(mongodb_interface.get_collection(domain))))

    if save and path_to_save != '' and path_to_save != None:
        output.saveAsTextFile(path_to_save)
    return output