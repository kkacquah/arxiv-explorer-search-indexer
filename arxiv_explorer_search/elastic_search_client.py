#!/usr/bin/env python
from config import ES_ENDPOINT
import requests
import json, os

"""
Post entry to ElasticSearch instance

Args:
    bulk_index_body: String of bulk index body
"""
def post_entry_to_elastic_search(bulk_index_body):
    headers = {'Content-Type': 'application/x-ndjson'}
    response = requests.post(
        os.path.join(ES_ENDPOINT, "_bulk"), data=bulk_index_body, headers=headers)

"""
Put a mapping index to ES instance
Args:
    mapping_body: JSON mapping body

References: https://www.elastic.co/guide/en/elasticsearch/reference/2.3/indices-put-mapping.html
"""
def put_mapping_to_elastic_search(mapping_body, index):
    headers = {'Content-Type': 'application/x-ndjson'}
    response = requests.put(
        os.path.join(ES_ENDPOINT, str(index)), data=mapping_body, headers=headers)
    if response.status_code == 200:
        print('Successfully mapped index')
    else: 
        raise Exception("Error mapping index: ", json.loads(response.text))