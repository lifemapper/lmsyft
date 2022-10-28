import os

from flask_app.sp_cache.solr_controller as app_solr


# From Docker env_file for resolver and sp_cache containers
SOLR_SERVER="http://localhost"
SOLR_PORT=8983
SECRET_KEY="dev"
WORKING_DIRECTORY="/tmp/scratch-path"

COLLECTIONS_URL = '{}:{}/solr/sp_collections'.format(SOLR_SERVER, SOLR_PORT)
SPECIMENS_URL = '{}:{}/solr/specimen_records'.format(SOLR_SERVER, SOLR_PORT)

COLLECTION_BACKUP_PATH = os.path.join(WORKING_DIRECTORY, "collections")
DWCA_PATH = os.path.join(WORKING_DIRECTORY, "new_dwcas")
PROCESSED_DWCA_PATH = os.path.join(WORKING_DIRECTORY, "processed_dwcas")
ERROR_DWCA_PATH = os.path.join(WORKING_DIRECTORY, "error_dwcas")


