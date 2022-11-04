import certifi
import json
import os
import requests
from requests_toolbelt.multipart import encoder
import urllib3

from flask_app.sp_cache.config import COLLECTIONS_URL, OCCURRENCES_URL
import flask_app.sp_cache.models as models
import flask_app.sp_cache.process_dwca as dwca_proc
import flask_app.sp_cache.solr_controller as solr

datadir = os.path.join(os.getcwd(), "test_data")

LOCAL_CACHE_API = "https://localhost/sp_cache/api/v1/collection"


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


# .....................................................................................
def test_submit_dwca():
    """Test various specimen operations."""
    http_mgr = urllib3.PoolManager(
        cert_reqs='CERT_REQUIRED',
        ca_certs="/home/astewart/self-signed-certificates",
        headers={"Prefer": "respond-async"}
    )
    for meta_fname, dwca_fname in tst_collections:
        f = open(meta_fname)
        collection_json = json.load(f)
        f.close()
        collection_id = collection_json['collection_id']
        url = f"{LOCAL_CACHE_API}/{collection_id}/occurrences/post"
        # fileobj = open(dwca_fname, 'rb')

        with open(dwca_fname, "rb") as f:
            file_data = f.read()

        # Sending the request.
        response = http_mgr.request(
            "post", url, fields={"file": (dwca_fname, file_data)})

        # form = encoder.MultipartEncoder({
        #     "documents": (dwca_fname, f, "application/octet-stream"),
        #     "composite": "NONE"
        # })
        # response = http.request("post", url, fields=form)

        # fileobj = open(dwca_fname, 'rb')
        # response = requests.post(
        #     url, files={"archive": (dwca_fname, fileobj)}, verify=False,
        #     cert_reqs = 'CERT_NONE')
        print("status is ", response.status)
        # dwca_filename = dwca_proc.move_to_queue("post", collection_id, request.data)
        # dwca_proc.process_dwca(dwca_filename)
        # r = requests.post(url, auth=HTTPDigestAuth('dev', 'dev'), data={"mysubmit": "Go"},
        #                   files={"archive": ("test.zip", fileobj)})


# .....................................................................................
def test_submit_dwca_now():
    """Test various specimen operations."""
    http = urllib3.PoolManager(
        cert_reqs='CERT_REQUIRED',
        ca_certs=certifi.where()
    )
    for meta_fname, dwca_fname in tst_collections:
        f = open(meta_fname)
        collection_json = json.load(f)
        f.close()
        collection_id = collection_json['collection_id']

        url = f"{LOCAL_CACHE_API}/{collection_id}/occurrence_post"
        form = encoder.MultipartEncoder({
            "documents": (dwca_fname, f, "application/octet-stream"),
            "composite": "NONE"
        })
        response = http.request("post", url, fields=form)
        # response = http.request("post", url, preload_content=False)

        # http.connection_from_url(url)
        # session = requests.Session()
        # with open(dwca_fname, 'rb') as f:
        #     form = encoder.MultipartEncoder({
        #         "documents": (dwca_fname, f, "application/octet-stream"),
        #         "composite": "NONE",
        #     })
        #     headers = {"Prefer": "respond-async", "Content-Type": form.content_type}
        #     response = session.post(url, headers=headers, data=form)
        # session.close()
        print(response.status_code)


# .....................................................................................
def test_post_get_collection_occurrences():
    """Test post collection function."""
    pass


# .............................................................................
if __name__ == '__main__':
    # test_post_get_collection()
    test_submit_dwca()