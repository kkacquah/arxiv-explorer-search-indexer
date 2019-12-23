from flags import ES_ENDPOINT
import requests

"""
Post entry to ElasticSearch instance

Args:
    bulk_index_body: String of bulk index body
"""


def post_entry_to_elastic_search(bulk_index_body):
    headers = {'Content-Type': 'application/x-ndjson'}
    response = requests.post(
        ES_ENDPOINT, data=bulk_index_body, headers=headers)
