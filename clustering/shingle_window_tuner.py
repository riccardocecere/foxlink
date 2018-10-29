import shingler
from scipy.spatial import distance
from operator import itemgetter
from mongodb_middleware import mongodb_interface
from bs4 import BeautifulSoup


#method for tuning the window for calculate the shingle
def tune_shingle_window(pages):
    avgs = []
    max_hammings = []

    for window in range(3,11):

        try:
            shingle_vectors = [shingler.compute_shingle_vector(page, window) for page in pages]

        except:
            print "errore"
            return 4

        all_hammings = []
        for j,page in enumerate(shingle_vectors):
            u = [el for el in page]
            for p2 in shingle_vectors[j+1:]:
                v = [el for el in p2]
                x = distance.hamming(u,v)
                all_hammings.append(x)

            max_hammings.append((window,max(all_hammings)))
            avg = round(sum(all_hammings)/len(all_hammings),1)
            avgs.append((window,avg))

        best_w,best_h = max(avgs,key=itemgetter(1))

        if best_h>= 0.3:
            all_bests = [w for w,h in avgs if h == best_h]
            output = max(all_bests)
            if output == None:
                ouput = 4
            return output
    return 3


collections = mongodb_interface.get_all_collections()
for collection in collections:
    pages = []
    for n in range(1,100):
        page = mongodb_interface.get_random_html(collection)
        if page != None:
            pages.append(BeautifulSoup(page,'html.parser'))

    window = tune_shingle_window(pages)
    print collection
    print window

