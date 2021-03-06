from config import S3_BUCKET
from config import ES_ENDPOINT
from elastic_search_client import post_entry_to_elastic_search, put_mapping_to_elastic_search
from progressbar import ProgressBar
import datetime
import zipfile
import gzip
import json
import boto3


INDEX_NAME = "arxiv_documents"

"""
Runs scripts to unzip zipped ArXiv metadata, and add them to Arxiv Explorers
Amazon ElasticSearch Service
"""
def index_compressed_files():
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(S3_BUCKET)

    print("Setting up index mapping")
    put_mapping_to_elastic_search(generateMapping(), INDEX_NAME)

    pbar = ProgressBar() #So we know how long this takes    
    print("Indexing metadata objects:")
    for compressed_metadata_object in pbar(list(bucket.objects.all())):
        bulk_index_body = index_compressed_file(compressed_metadata_object.get()['Body'])
        post_entry_to_elastic_search(bulk_index_body)
    

"""
Unzip zipped ArXiv metadata,
and make post request to add to ES index
"""
def index_compressed_file(compressed_metadata):
    metadata_objects = unzip(compressed_metadata)
    entry_strings = [get_entry_string(metadata) for metadata in metadata_objects]
    return '\n'.join(entry_strings) + '\n'

"""
Get entry string from metadata

Args:
    metadata: Metadata object to generate string

returns: Raw entry string to be concatenatd into
bulk ElasticSearch POST
"""
def get_entry_string(metadata):
    entry_and_index = [create_index_key(metadata['id']),create_es_entry(metadata)]
    return '\n'.join(entry_and_index)

"""
Unzip File

Args:
    filename: Name of file to unzip.

returns: List of metadata JSONs from zipped file.
"""
def unzip(filename):
    with gzip.open(filename, 'rt', encoding='utf-8') as fin:
        metadata = [json.loads(line) for line in fin.readlines()]
        return metadata

"""
Create an index key string from *id*

Args:
    id: *id* of the index_key string created

returns: index key string with information from *id*
"""
def create_index_key(id):
    index_key = dict()
    index_value = dict()
    index_value["_index"] = INDEX_NAME
    index_value["_type"] = "arxiv_document"
    index_value["_id"] = id
    index_key["index"] =index_value
    return json.dumps(index_key)

"""
Create an ElasticSearch entry from *metadata*

Args:
    metadata: Metadata object to generate string

returns: ElasticSearch entry string with information from *metadata*
"""
def create_es_entry(metadata):
    es_entry = dict()
    fields = dict()
    fields["published_date"] = convert_date_string_to_timestamp(metadata["timestamp"])
    fields["title"] = metadata["title"]
    fields["abstract"] = metadata["abstract"]
    es_entry["fields"] =fields
    return json.dumps(es_entry)

"""
Create an ElasticSearch entry from *metadata*

Args:
    date_string: Date string of format 'Mon, 20 Aug 2007 03:00:20 GMT'

returns: ElasticSearch entry string with information from *metadata*
"""
def convert_date_string_to_timestamp(date_string):
    arxiv_date_format = '%a, %d %b %Y %H:%M:%S %Z'
    date = datetime.datetime.strptime(date_string, arxiv_date_format)
    return int(date.timestamp())

"""
Define ES mapping
"""
def generateMapping():
    mappingBody = {
        "mappings": {
            "arxiv_document": {
                "properties": {
                    "fields": {
                        "properties": {
                            "abstract": {
                                "type": "string"
                            },
                            "title": {
                                "type": "string"
                            },
                            "published_date": {
                                "type": "date",
                                "format": "epoch_second"
                            }
                        }
                    }
                }
            }
        }
    }
    return json.dumps(mappingBody)

if __name__ == "__main__":
    index_compressed_files()
