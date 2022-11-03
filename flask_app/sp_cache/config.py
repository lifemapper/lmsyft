"""Solr configuration parameters."""
import os

try:
    SOLR_SERVER = os.environ["SOLR_SERVER"]
except:
    SOLR_SERVER = "http://localhost"

try:
    SOLR_PORT = os.environ["SOLR_PORT"]
except:
    SOLR_PORT = 8983

try:
    WORKING_DIRECTORY = os.environ["WORKING_DIRECTORY"]
except:
    WORKING_DIRECTORY = "/scratch-path"

try:
    SERVER_URL = os.environ["FQDN"]
except:
    SERVER_URL = "localhost"

COLLECTIONS_URL = '{}:{}/solr/sp_collections'.format(SOLR_SERVER, SOLR_PORT)
SPECIMENS_URL = '{}:{}/solr/specimen_records'.format(SOLR_SERVER, SOLR_PORT)
OCCURRENCES_URL = '{}:{}/solr/occurrences'.format(SOLR_SERVER, SOLR_PORT)

COLLECTION_BACKUP_PATH = os.path.join(WORKING_DIRECTORY, 'collections')
DWCA_PATH = os.path.join(WORKING_DIRECTORY, 'new_dwcas')
PROCESSED_DWCA_PATH = os.path.join(WORKING_DIRECTORY, 'processed_dwcas')
ERROR_DWCA_PATH = os.path.join(WORKING_DIRECTORY, 'error_dwcas')
