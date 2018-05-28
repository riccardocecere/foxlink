# Class for managing input/output files
from configurations import variable_values
import datetime

# Takes the product ids from input file, return an array
def takeInputId():
    f = open(variable_values.input_path, 'r')
    ids = f.read()
    f.close()
    ids = ids.split('\n')
    return ids

# Write on the file the sorted site counting
# Input  is a dictionary (site,counting)
def siteCountingOutput(urls_counts):
    urls_counts = sorted(urls_counts.items(), key=lambda x: -x[1])
    f = open(variable_values.root_output_path + '100_percent.txt', 'w')
    f.write('Percent: 100%    Time: ' + str(datetime.datetime.now().time()) + '\n')
    f.write(str(urls_counts))
    f.close()