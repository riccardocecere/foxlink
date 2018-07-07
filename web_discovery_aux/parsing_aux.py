import re
import requests
import pattern.web,nltk


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


#function to extract clean text from html pages
def extract_clean_text_from_page(url):
    if url == None or url == '':
        return 'ERROR: empty url'
    try:
        #Take html of the page
        html = requests.get(str(url)).text
    except:
        print 'error to take the page: '+str(url)
        return 'ERROR:to take the page'
    try:
        #Remove all html tags
        home_page_text = pattern.web.plaintext(html)

        #Remove
        home_page_text = re.sub("[^A-Za-z0-9' ']", "", home_page_text).lower()
    except:
        print 'error extracting plain text from page: '+str(url)
        return 'ERROR: extracting plain text'

    if home_page_text == '' or home_page_text== None:
        return('ERROR: before tokenization')
    try:
        # Tokenization of the words
        words = nltk.word_tokenize(home_page_text)
        print words
        #Remove all the non words, probabily is not necessary
        #words = [word for word in tokens if word.isalnum()]


    except:
        return 'ERROR: during tokenization'

    try:
        stop_words = set(nltk.corpus.stopwords.words('english'))
        words = [w for w in words if not w in stop_words]
    except:
        return 'ERROR: during stopwords process'

    try:
        #Stemming
        porter = nltk.stem.PorterStemmer()
        stemmed = [porter.stem(word) for word in words]
    except:
        return 'ERROR: during stemming process'

    return stemmed

# Function to select if a warc is useful based on it's name that must be in the form crawl-data/CC-MAIN-2018-22/segments/1526794863689.50/warc/CC-MAIN-20180520205455-20180520225455-00609.warc.gz
# it's important that has /warc/ inside the string

def correct_warc_name(warcname):
    if warcname == '' or warcname == None:
        return False
    return re.match('crawl.*\/warc\/.*gz',warcname)

