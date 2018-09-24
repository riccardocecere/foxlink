def generate_referring_lists(clusters):
    result = []
    for cluster in clusters:
        cluster_referers = {}
        cluster_referers['cluster'] = cluster[0]
        cluster_elements = cluster[1]
        cluster_referers['cluster_elements'] = cluster_elements
        referring_url_list = []
        url_list = []
        for cluster_element in cluster_elements:
            url_list.append(cluster_element[0])
            referring_url_list.append(cluster_element[2])
        cluster_referers['url_list'] = url_list
        cluster_referers['referring_url_list'] = referring_url_list
        result.append(cluster_referers)

    return result


def calculate_referers(clusters):
    result = []
    for current_cluster_referer in clusters:
        cluster_referer = current_cluster_referer
        cluster_referer['same_cluster_refer_cont'] = 0
        cluster_referer['other_cluster_refer_cont'] = 0
        cluster_referer['no_match_cluster_refer_cont'] = 0
        for referring_url in cluster_referer['referring_url_list']:
            found = False
            for cluster in clusters:
                if referring_url in cluster['url_list']:
                    found = True
                    if cluster_referer['cluster'] == cluster['cluster']:
                        cluster_referer['same_cluster_refer_cont'] += 1
                    else:
                        cluster_referer['other_cluster_refer_cont'] += 1
                    break
            if found == False:
                cluster_referer['no_match_cluster_refer_cont'] += 1
        result.append(cluster_referer)
    return result

def label_cluster_on_referring(clusters):
    result = []

    for current_cluster in clusters:
        cluster = current_cluster

        if (int(cluster['no_match_cluster_refer_cont'])+int(cluster['same_cluster_refer_cont'])) < (int(cluster['other_cluster_refer_cont'])):
            cluster['label'] = 'products'
        else:
            cluster['label'] = 'indexes'
        result.append(cluster)

    return result



def calculate_referring_url_metrics(sc,clusters,save,path_to_save):
    output = clusters.map(lambda (domain,cluster_list): (domain,generate_referring_lists(cluster_list))) \
        .mapValues(calculate_referers).mapValues(label_cluster_on_referring)


    if save and path_to_save != '' and path_to_save != None:
        output.saveAsTextFile(path_to_save)


    return output