import datetime
from configurations import variable_values
from filePackage import fileManager
from serverPackage import searxManager
from analytics import siteDiscoveryAnalytics


print 'Starts at: '+str(datetime.datetime.now().time())
ids = fileManager.takeInputId()
cont = 0
percent = 1
percent_step =len(ids)*0.01
percent_limit = percent_step
urls_counts = {}

for id in ids:
    if id == None or id == '':
        continue
    else:
        try:
            response = searxManager.simpleQuerySearx(id)
        except:
            print('Json: ' + str(id) + ' not found')
            continue
    siteDiscoveryAnalytics.siteCounting(response, urls_counts)
    cont += 1

    if cont > percent_limit:
        print 'Percentuale di completamento: '+ str(percent) + '%    Orario: ' + str(datetime.datetime.now().time())
        percent_limit = percent_limit + percent_step
        percent += 1

    if percent%5 == 0:
        f = open(variable_values.root_output_path+str(percent)+'_percent.txt', 'w')
        f.write('Percent: '+ str(percent) + '%    Time: ' + str(datetime.datetime.now().time())+'\n')
        urls_counts_step = sorted(urls_counts.items(), key=lambda x: -x[1])
        f.write(str(urls_counts_step))
        f.close()

fileManager.siteCountingOutput(urls_counts)
print 'Finish at: '+str(datetime.datetime.now().time())
