from collections import defaultdict

def clusters_hits(inDegrees_clusters):
    inDegrees_list = inDegrees_clusters[0]
    clusters_list = inDegrees_clusters[1]

    hits = defaultdict(int)
    for inDegree in inDegrees_list:
        for cluster in clusters_list:
            if inDegree[0] in cluster[1]:
                hits[str(cluster[0])] += 1
    return (hits.items(),clusters_list)

def infer_product_pages(sc, clusters, in_degrees,threshold,save, path_to_save):

    in_degrees_filtered = in_degrees.map(lambda (domain,list_degrees):(domain,[x for x in list_degrees if x[1]>threshold]))
    output = in_degrees_filtered.join(clusters)\
        .mapValues(clusters_hits)

    if save and path_to_save != '' and path_to_save != None:
        output.saveAsTextFile(path_to_save)

    return output