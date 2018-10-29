import hash_functions_applier
import numpy as np
from general_utils import text_parser
from itertools import islice


# Function to verify if an html node has the attribute type=hidden
def hidden(child):
    if 'type' in child.attrs:
        return child.attrs['type'] == 'hidden'
    return False


# Computes shingle vector for given html tree
def compute_shingle_vector(html, shingle_window_size):

    shingles_set = compute_shingles_set(html, shingle_window_size)

    shingles_hashed_vectors = []
    for shingle in shingles_set:
        shingles_hashed_vectors.append(hash_functions_applier.apply(' '.join(shingle)))

    try:
        shingles_matrix = np.vstack(shingles_hashed_vectors)
        return tuple(shingles_matrix.min(axis=0))
    except:
        return []


# Returns set of all possible shingles in the given page
# Shingles are sequences of consecutive html tags of given size
def compute_shingles_set(html, shingle_window_size):
    tags = []
    # descendants performs a depth first visit of the DOM
    for child in html.descendants:
        if child.name is not None and child.name and text_parser.tag_visible(child) and child.name not in ['script', 'link', 'meta'] and not hidden(child):
            tags.append(child.name)
    return window(tags, shingle_window_size)

#Function to handle an n size window
def window(seq, n):
    it = iter(seq)
    result = tuple(islice(it, n))
    if len(result) == n:
        yield result
    for elem in it:
        result = result[1:] + (elem,)
        yield result




