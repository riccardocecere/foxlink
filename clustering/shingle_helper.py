import hash_helper
import numpy as np
from itertools import islice
import text_extractor
from mongodb_middleware import mongodb_interface
from bs4 import BeautifulSoup


def hidden(child):
    if 'type' in child.attrs:
        return child.attrs['type'] == 'hidden'
    return False


# computes shingle vector for given html tree
def compute_shingle_vector(html, shingle_window_size):

    shingles_set = compute_shingles_set(html, shingle_window_size)

    shingles_hashed_vectors = []
    for shingle in shingles_set:
        shingles_hashed_vectors.append(hash_helper.apply(' '.join(shingle)))

    try:
        shingles_matrix = np.vstack(shingles_hashed_vectors)
        return tuple(shingles_matrix.min(axis=0))
    except:
        return []


# returns set of all possible shingles in the given page
# shingles are sequences of consecutive html tags of given size
def compute_shingles_set(html, shingle_window_size):
    tags = []
    # descendants performs a depth first visit of the DOM
    for child in html.descendants:
        if child.name is not None and child.name and text_extractor.tag_visible(child)\
                and child.name not in ['script', 'link', 'meta'] and not hidden(child):
            # child.name not in ['script', 'link'] and not hidden(child):
            tags.append(child.name)

    return window(tags, shingle_window_size)


def window(seq, n):
    it = iter(seq)
    result = tuple(islice(it, n))
    if len(result) == n:
        yield result
    for elem in it:
        result = result[1:] + (elem,)
        yield result




