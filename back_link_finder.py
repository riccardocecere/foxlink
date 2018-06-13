from mozscape import Mozscape
from time import sleep
import datetime


client = Mozscape('mozscape-1979941676','22a529a19b166a7220544ad976d35b7a')

print 'Starts at: ' + str(datetime.datetime.now().time())

f = open('./id_list/input_id/lista_siti_prodotti_dexter_test.txt','r')
urls = f.read()
f.close()

urls = urls.split('\n')

cont = 1
urls_len = len(urls)

backlinks_founded = {'Errors': []}

for url in urls:

    percent = float((cont*100)/urls_len)

    if int(percent) % 5 == 0:
        print 'Percentuale di completamento ' + str(percent) + '%'
        f = open('./outputs/backlinks_result' + str(percent) + '_percent.txt', 'w')
        f.write('Percent: ' + str(percent) + '%    Time: ' + str(datetime.datetime.now().time()) + '\n')
        f.write(str(backlinks_founded))
        f.close()

    if url == None or url == '':
        continue
    else:
        url = url.encode('utf-8')
        sleep(10.00)
        try:
            backlinks = client.links(
                str(url), scope='page_to_domain', sort='domains_linking_page',
                filters=['external'], limit=50, sourceCols=4, targetCols=4, linkCols=4)
        except:
            print 'Moz: ' + str(url) + ' not found'
            backlinks_founded['Errors'].append(str(url))
            cont += 1
            continue
    backlinks_list = []
    for backlink in backlinks:
        backlinks_list.append(backlink['uu'])
    print (url,backlinks_list)
    backlinks_founded[str(url)] = backlinks_list
    cont += 1

print 'Finish at: ' + str(datetime.datetime.now().time())