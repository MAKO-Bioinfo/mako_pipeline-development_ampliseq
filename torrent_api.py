import json
import requests

base_url = 'http://10.0.1.74/rundb/api/v1/'
resp = requests.get('%s/results/17?format=json'%base_url, auth=('ionadmin', 'ionadmin'))
resp_json = resp.json()

resp = requests.get('http://10.0.1.74/%s'%resp_json['fastqlink'],auth=('ionadmin', 'ionadmin'))

print resp_json['fastqlink']
print resp.content

#(The FASTQ file contents.)tp://10.0.1.74/%s'%resp_json['fastqlink'], auth=('ionadmin', 'ionadmin'))


