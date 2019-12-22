from arxiv_explorer_search.flags import ES_ENDPOINT
from arxiv_explorer_search.flags import FILEPATH
from arxiv_explorer_search.flags import DATE_FORMAT
from arxiv_explorer_search.elastic_search_client import post_entry_to_elastic_search
import datetime
import zipfile
import gzip
import json
import glob

"""
Runs scripts to unzip zipped ArXiv metadata, and add them to Arxiv Explorers
Amazon ElasticSearch Service
"""
def index_compressed_files():
    glob_query = FILEPATH + "/*"
    metadata_filenames = glob.glob(glob_query)
    for compressed_metadata_filename in metadata_filenames:
        bulk_index_body = index_compressed_file(compressed_metadata_filename)
        post_entry_to_elastic_search(bulk_index_body)
"""
Unzip zipped ArXiv metadata,
and make post request to add to ES index
"""
def index_compressed_file(compressed_metadata_filename):
    metadata_objects = unzip(compressed_metadata_filename)
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
    index_value["_index"] = "arxiv_documents"
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
    fields["published_date"] = convert_date_time_to_sql_format(metadata["timestamp"])
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
def convert_date_time_to_sql_format(date_string):
    sql_date = datetime.datetime.strptime(date_string, DATE_FORMAT)
    return sql_date.strftime('%Y-%m-%dT%H:%M:%SZ')
