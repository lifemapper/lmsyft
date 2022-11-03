import datetime
import glob
import json
import os
from flask import request

import flask_app.sp_cache.config as config
import flask_app.sp_cache.models as models
import flask_app.sp_cache.process_dwca as dwca_proc
import flask_app.sp_cache.solr_controller as solr

datadir = os.path.join(os.getcwd(), "test_data")

tst_collections = [
    (os.path.join(datadir, "kui_coll.json"), os.path.join(datadir, "kui-dwca.zip")),
    (os.path.join(datadir, "kuit_coll.json"), os.path.join(datadir, "kuit-dwca.zip"))
]

# .....................................................................................
def test_post_get_collection():
    """Test post collection function."""
    for meta_fname, dwca_fname in tst_collections:
        f = open(meta_fname)
        collection_json = json.load(f)
        f.close()

        collection_id = collection_json['collection_id']
        collection = models.Collection(collection_json)
        solr.post_collection(collection)
        retval = solr.get_collection(collection_id)

        dwca_proc.process_dwca(dwca_fname, collection_id=collection_id)

# .....................................................................................
def test_submit_dwca():
    """Test various specimen operations."""
    for coll_id, dwca_fname in tst_collections:
        dwca_proc.process_dwca(dwca_fname, collection_id=coll_id)

# .....................................................................................
def test_process_dwca():
    """Test various specimen operations."""
    dwca_proc.main()


# .....................................................................................
def test_post_get_collection_occurrences():
    """Test post collection function."""
    pass


# .............................................................................
if __name__ == '__main__':
    test_post_get_collection()