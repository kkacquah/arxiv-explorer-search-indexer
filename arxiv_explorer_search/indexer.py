from arxiv_explorer_search.flags import ES_ENDPOINT
from arxiv_explorer_search.flags import FILEPATH
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
    for metadata_filename in metadata_filenames:
        index_compressed_file(metadata_filename)
"""
Unzip zipped ArXiv metadata,
and make post request to add to ES index
"""
def index_compressed_file(metadata_filename):
    metadata = unzip(metadata_filename)
    index_key = create_index_key(metadata['id'])


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

returns: List of metadata JSONs from zipped file.
"""
def create_index_key(id):
    index_key = dict()
    index_value = dict()
    index_value["_index"] = "arxiv_documents"
    index_value["_type"] = "arxiv_document"
    index_key["index"] =
    return `{ "index": { "_index": "arxiv_doocuments", "_type": "arxiv_doocument", "_id": "` + id + `" } }`
