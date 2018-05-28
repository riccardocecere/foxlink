# Class for managing the comunication with Searx
import requests
from configurations import variable_values

# Make a simple query to searx, input product id it return the response in json format
def simpleQuerySearx(product_id):
    response = requests.get(variable_values.searx_address + variable_values.searx_query + str(product_id))
    response = response.json()
    return response
