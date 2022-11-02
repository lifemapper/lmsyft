import os
import json
import requests
from flask import Blueprint, request
from werkzeug.exceptions import NotFound

import flask_app.sp_cache.config as config
import flask_app.sp_cache.models as models
import flask_app.sp_cache.solr_controller as controller

import flask_app.sp_cache.solr_controller as app_solr
from flask_app.sp_cache.models import Collection, SpecimenRecord
from flask_app.sp_cache.config import (
    COLLECTIONS_URL, SPECIMENS_URL, COLLECTION_BACKUP_PATH, DWCA_PATH,
    PROCESSED_DWCA_PATH, ERROR_DWCA_PATH)

from tests.test_cache.sp_cache_tests import SpCacheCollectionTest
from tests.test_cache.sp_cache_tests import SpCacheCollectionOccurrencePostTest
pwd = os.getcwd()

kui_coll_fname = os.path.join(pwd, "test_data/kui_coll.json")
kuit_coll_fname = os.path.join(pwd, "test_data/kuit_coll.json")


f = open(kui_coll_fname)
collection_json = json.load(f)
f.close()

# kui_coll = Collection(kui)
# conn = app_solr.get_collection_solr()
# resp = conn.add(kui_coll.serialize_json(), commit=True)
# resp = app_solr.post_collection(kui_coll)

# collection_json = request.get_json(force=True)
collection = models.Collection(collection_json)
controller.post_collection(collection, do_verify=False)


# Write collection information backup file
collection_id = collection.attributes['collection_id']
collection_filename = os.path.join(
    config.COLLECTION_BACKUP_PATH, '{}.json'.format(collection_id)
)

with open(collection_filename, mode='wt') as out_json:
    json.dump(collection_json, out_json)
print(controller.get_collection(collection_id).docs[0])


# _ = app_solr.get_collection(coll_id)
# app_solr.delete_collection(coll_id)
# _ = app_solr.get_collection(coll_id)

# tst = SpCacheCollectionTest(coll_vals, endpt, do_verify=False)
# tst.run_test()