import re

# Function that takes id and a result in a response to generate  (domain, (page,id))
def find_domain_url(id, result):

    # Check the id
    if id == None or id == '':
        return (id,'ERROR://empty_query')

    try:
        # Use a regex to extract the domain from url
        domain = re.findall(r'.*://.*?/',result['url'])[0][:-1]

        # Use a regex to extract only the 'final' part of url page es: https://amazon.com/vapur ----> vapur
        page = re.sub('.*://.*?/', '', result['url'])
        return (domain, (page,id))

    except:
        return (id,'ERROR://parsing_problem')