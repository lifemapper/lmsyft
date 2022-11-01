import os
import json
import requests

import flask_app.sp_cache.solr_controller as app_solr
from flask_app.sp_cache.models import Collection, SpecimenRecord
from flask_app.sp_cache.config import (
    COLLECTIONS_URL, SPECIMENS_URL, COLLECTION_BACKUP_PATH, DWCA_PATH,
    PROCESSED_DWCA_PATH, ERROR_DWCA_PATH)

from tests.test_cache.sp_cache_tests import SpCacheCollectionTest
from tests.test_cache.sp_cache_tests import SpCacheCollectionOccurrencePostTest

kui_coll_fname = "tests/test_data/kui_coll.json"
kuit_coll_fname = "tests/test_data/kuit_coll.json"

f = open(kui_coll_fname)
kui = json.load(f)
f.close()
kui_coll = Collection(kui)
conn = app_solr.get_collection_solr()
resp = conn.add(kui_coll.serialize_json(), commit=True)


resp = app_solr.post_collection(kui_coll)



# _ = app_solr.get_collection(coll_id)
# app_solr.delete_collection(coll_id)
# _ = app_solr.get_collection(coll_id)

# tst = SpCacheCollectionTest(coll_vals, endpt, do_verify=False)
# tst.run_test()