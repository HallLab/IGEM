import requests
response = requests.head('https://hmdb.ca/system/downloads/current/hmdb_metabolites.zip')
print(response.headers['Content-Length'])

v_version = requests.head('https://hmdb.ca/system/downloads/current/hmdb_metabolites.zip').headers['Content-Length']
print('version sera:', v_version)