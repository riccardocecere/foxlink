# Class for managing analytics in site discovery phase

import re

# it updates the dictinary containing the counting of site discovered
# input the single response in json format from searx and the dictionary (site: count)
def siteCounting(response, urls_counts):
    for result in response['results']:
        try:
            url = result['url'].encode('utf-8')
            url = re.findall(r'.*://.*?/', url)[0][:-1]
            if url in urls_counts:
                urls_counts[url] += 1
            else:
                urls_counts[url] = 1
            return urls_counts
        except:
            print('counting site problem')
            return urls_counts