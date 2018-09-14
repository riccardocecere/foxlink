
def calculate_clusters_deep_average(clusters2average):
    averages_sum = 0

    for cluster in clusters2average:
        averages_sum += float(cluster[1])

    return float(averages_sum)/float(len(clusters2average))

def normalize_clusters_average(clusters2average,total_elements):
    normalized_clusters = []

    for cluster in clusters2average:
        normalized_average = float(float(cluster[1])*(float(cluster[2])/float(total_elements)))
        normalized_clusters.append((cluster[0],normalized_average))
    return normalized_clusters

def calculate_armonic_average(clusters):
    sum_average = float(sum(1.0/float(depth_limit) for _,depth_limit in clusters))
    return float(len(clusters))/sum_average

def calculate_arithmetic_average(clusters):
    sum_average = float(sum(float(depth_limit) for _,depth_limit in clusters))
    return sum_average/float(len(clusters))


#Method for labelling the clusters based on the average of their average deep
# Input is a list of clusters [()]
def label_cluster(clusters, clusters_deep_average):

    if clusters == [] or clusters == None or clusters == '':
        return None

    clusters_result_list = []

    if len(clusters) == 1:
        try:
            cluster_result = {}
            clusters = list(clusters)
            if float(clusters[0][1]) < 5.4:
                cluster_result['label'] = 'indexes'
            else:
                cluster_result['label'] = 'products'
            cluster_result['vector'] = clusters[0][0]
            cluster_result['average'] = clusters[0][1]
            clusters_result_list.append(cluster_result)
            return clusters_result_list
        except:
            return None


    for cluster in clusters:

        try:
            cluster_result = {}
            if float(cluster[1]) > float(clusters_deep_average):
                cluster_result['label'] = 'products'
            else:
                cluster_result['label'] = 'indexes'
            cluster_result['vector'] = cluster[0]
            cluster_result['average'] = cluster[1]
            clusters_result_list.append(cluster_result)
        except:
            continue
    if clusters_result_list == [] or clusters_result_list == None:
        return None
    return clusters_result_list




def calculate_cluster_metrics(sc,clusters,save,path_to_save):
    output = clusters.flatMap(lambda (domain,values): ((domain,cluster) for cluster in values))\
    .map(lambda (domain,cluster): ((domain,(cluster[0], calculate_arithmetic_average(cluster[1]),len(cluster[1])))))

    clusters_labeled = output.groupByKey()\
        .map(lambda (domain,clusters2average):(domain, {'clusters2average':clusters2average,'total_elements':sum(cluster_elements for _,_,cluster_elements in clusters2average)}))\
        .map(lambda (domain,clusters2average): (domain, {'clusters2average':normalize_clusters_average(clusters2average['clusters2average'],clusters2average['total_elements'])}))\
        .map(lambda (domain,clusters2average):(domain, {'clusters_deep_average':calculate_clusters_deep_average(clusters2average['clusters2average']),'clusters':clusters2average['clusters2average']}))\
        .map(lambda (domain,clusters_value):(domain,label_cluster(clusters_value['clusters'], clusters_value['clusters_deep_average'])))\
        .filter(lambda (domain,clusters_value):clusters_value != None)


    if save and path_to_save != '' and path_to_save != None:
        clusters_labeled.saveAsTextFile(path_to_save)
    return clusters_labeled