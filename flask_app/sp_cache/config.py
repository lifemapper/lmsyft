"""Solr configuration parameters."""
import os


COLLECTIONS_URL = '{}:{}/solr/sp_collections'.format(
    os.environ['SOLR_SERVER'],
    os.environ['SOLR_PORT']
)
SPECIMENS_URL = '{}:{}/solr/specimen_records'.format(
    os.environ['SOLR_SERVER'],
    os.environ['SOLR_PORT']
)

COLLECTION_BACKUP_PATH = os.path.join(
    os.environ['WORKING_DIRECTORY'],
    'collections'
)
DWCA_PATH = os.path.join(os.environ['WORKING_DIRECTORY'], 'new_dwcas')
PROCESSED_DWCA_PATH = os.path.join(
    os.environ['WORKING_DIRECTORY'],
    'processed_dwcas'
)
ERROR_DWCA_PATH = os.path.join(
    os.environ['WORKING_DIRECTORY'],
    'error_dwcas'
)
