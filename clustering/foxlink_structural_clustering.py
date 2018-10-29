from clustering import structural_clustering
from mongodb_middleware import mongodb_interface
from general_utils import rdd_utils

# It returns (domain, [(shingle_vector, [url1,url2,...],....)])
def all_sites_structural_clustering(domains, save, path_to_save,thresold):

    output = domains.map(lambda domain: (domain, structural_clustering.structural_clustering(mongodb_interface.get_collection(domain),thresold)))

    rdd_utils.save_rdd(output,save,path_to_save)

    return output