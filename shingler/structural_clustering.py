import operator
import ast
from mongodb_middleware import mongodb_interface


def structural_clustering(collection, threshold=20):

    # initialize empty hash table H
    hash_table = {}

    # initialize helper structures for second pass
    eight_eight_vectors_counts = {}
    covering_vectors = {}

    # initialize helper map for third pass
    # inverted index: v -> list of pages with shingle vector v
    inverted_index = {}

    # initialize outcoming clusters and not assigned pages
    clusters = {}
    not_assigned_pages = []

    # dictionary containing the sample of the site
    sample = {}

    # build the sample dictionary from the relative mongodb collection data
    for post in collection.find():
        try:
            sample[post['url_page']] = post['shingle_vector']
        except:
            continue

    #   *FIRST PASS*
    # sample.items dictionary ---> [(key1,value1), ...]
    for page in sample.items():

        url, shingle_vector = page
        if shingle_vector == '[]' or shingle_vector == None:
            continue
        # update inverted index v -> pages for third pass
        if shingle_vector in inverted_index:
            inverted_index[shingle_vector].append(url)
        else:
            inverted_index[shingle_vector] = [url]

        # shingle_vector variable is an 8/8 shingle vector
        # if already processed
        if shingle_vector in eight_eight_vectors_counts:
            eight_eight_vectors_counts[shingle_vector] += 1
            # no need for computing covering 6/8, 7/8 vectors again
            # just increment counts in hash table H
            increment_counts(covering_vectors[shingle_vector], hash_table)
        else:
            # never seen this vector, init 8/8 count to 1
            eight_eight_vectors_counts[shingle_vector] = 1
            # 6/8 and 7/8 vectors need to be computed
            # increment counts while computing them (only one pass)
            covering_vectors[shingle_vector] = compute_covering_vectors_and_increment_counts(shingle_vector, hash_table)

    #   *SECOND PASS*
    # sort 8/8 vectors by count
    sorted_eight_eight = sorted(eight_eight_vectors_counts.items(), key=operator.itemgetter(1), reverse=True)

    for shingle_vector, count in sorted_eight_eight:
        v_prime = covering_vector_with_maximum_count(covering_vectors[shingle_vector], hash_table)
        decrement_all_but_v_prime_by_v_count(covering_vectors[shingle_vector], v_prime, count, hash_table)

    hash_table = delete_by_threshold(hash_table, threshold)

    not_clustered = 0
    old_sample_size = len(sample)

    #   *THIRD PASS*
    # for each page p with shingle vector v
    for shingle_vector, urls in inverted_index.items():
            v_prime = covering_vector_with_maximum_count(covering_vectors[shingle_vector], hash_table)
            # Due to threshold deletion a page could be not assigned to any cluster
            if v_prime is not None:
                if v_prime in clusters:
                    clusters[v_prime] += urls
                else:
                    clusters[v_prime] = urls
            else:
                # all pages with shingle vector v will not be assigned to any cluster.
                not_assigned_pages.append((shingle_vector, urls))
                for u in urls:
                    not_clustered += 1
                    del sample[u]

    print("Vertex clusters: " + str(len(clusters)) + ' clusters.')

    print("Not classified: " + str(not_clustered)) + ' urls.'

    print('Classified %.3f' % (100 - ((not_clustered * 100)/old_sample_size))) + '%.'

    # build algorithm result with a short iteration
    result = []
    for shingle_vector, cluster in clusters.items():
        result.append((to_vec(shingle_vector), cluster))

    result = sorted(result, key=lambda x: len(x[1]), reverse=True)
    print result
    return result


# Function to increment the counting of each shingle vector in covering vectors list
def increment_counts(covering_vectors, hash_table):
    for v_prime in covering_vectors:
        hash_table[v_prime] += 1


# Function to calculate covering vectors and increment their counts
def compute_covering_vectors_and_increment_counts(shingle_vector, hash_table):

    wildcard = -1
    covering_vectors = []

    for i in range(0, 8):

        # convert to list to enable index access
        shingle_vector_list = ast.literal_eval(shingle_vector)
        curr_seven_aux = list(shingle_vector_list)
        curr_seven_aux[i] = wildcard
        curr_seven = tuple(curr_seven_aux)
        covering_vectors.append(curr_seven)

        if curr_seven in hash_table:
            hash_table[curr_seven] += 1
        else:
            hash_table[curr_seven] = 1

        for j in range(i+1, 8):
            curr_six_aux = list(curr_seven_aux)
            curr_six_aux[j] = wildcard
            curr_six = tuple(curr_six_aux)
            covering_vectors.append(curr_six)

            if curr_six in hash_table:
                hash_table[curr_six] += 1
            else:
                hash_table[curr_six] = 1

    return covering_vectors


# Function that returns the covering vector with max count
def covering_vector_with_maximum_count(covering_vectors, hash_table):

    m = 0
    maximum_count_vector = covering_vectors[0]
    found = 0

    for v_prime in covering_vectors:
        if v_prime in hash_table:
            found = 1
            if hash_table[v_prime] > m:
                m = hash_table[v_prime]
                maximum_count_vector = v_prime

    if found == 1:
        return maximum_count_vector
    else:
        return None


# Function that decrements coutns of all shingle vectors by count value, but the prime shingle vector
def decrement_all_but_v_prime_by_v_count(covering_vectors, v_prime, count, hash_table):
    for v in covering_vectors:
        if not v == v_prime:
            hash_table[v] -= count


# Function that considers only clusters composed by a number of elements greater than a threshold
def delete_by_threshold(hash_table, threshold):
    hash_table = {k: v for k, v in hash_table.items() if v >= threshold}
    return hash_table


# Function that transform a tuple in a vector
def to_vec(tuple):
    return [x for x in tuple]
