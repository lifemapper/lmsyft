import os

import flask_app.sp_cache.solr_controller as app_solr


# From Docker env_file for resolver and sp_cache containers
SOLR_SERVER="http://localhost"
SOLR_PORT=8983
SECRET_KEY="dev"
WORKING_DIRECTORY="/tmp/scratch-path"

COLLECTIONS_URL = '{}:{}/solr/sp_collections'.format(SOLR_SERVER, SOLR_PORT)
SPECIMENS_URL = '{}:{}/solr/specimen_records'.format(SOLR_SERVER, SOLR_PORT)
RESOLVER_URL = '{}:{}/solr/spcoco'.format(SOLR_SERVER, SOLR_PORT)

COLLECTION_BACKUP_PATH = os.path.join(WORKING_DIRECTORY, "collections")
DWCA_PATH = os.path.join(WORKING_DIRECTORY, "new_dwcas")
PROCESSED_DWCA_PATH = os.path.join(WORKING_DIRECTORY, "processed_dwcas")
ERROR_DWCA_PATH = os.path.join(WORKING_DIRECTORY, "error_dwcas")

collection_id = 'test_collection'
collection_data = {
    'collection_id': collection_id,
    'institution_name': 'test institution',
    'last_updated': '2021-05-03T11:06:00Z',
    'public_key': 'specify_pub_key',
    'collection_location': 'Specify HQ',
    'contact_name': 'Test User',
    'contact_email': 'test@sfytorium.org',
}
app_solr.post_collection(collection_data)
_ = app_solr.get_collection(collection_id)
app_solr.delete_collection(collection_id)
_ = app_solr.get_collection(collection_id)
