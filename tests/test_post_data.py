import certifi
import json
import os
import requests
from requests_toolbelt.multipart import encoder
import urllib3

datadir = os.path.join(os.getcwd(), "test_data")

# LOCAL_PUBLIC_KEY = "/home/astewart/self-signed-certificates/fullchain.pem"
# DEFAULT_SSL_KEY = "/etc/ssl/certs/ca-certificates.crt"
# DEFAULT_SSL_KEY = "/home/astewart/git/specify_cache/venv/lib/python3.8/site-packages/certifi/cacert.pem"
LOCAL_CACHE_API = "https://localhost/sp_cache/api/v1"
OLD_CACHE_API = "https://syftorium.org/api/v1/sp_cache"

COLLECTION_ENDPOINT = f"{LOCAL_CACHE_API}/collection"

TEST_COLLECTIONS = [
    (os.path.join(datadir, "kui_coll.json"), os.path.join(datadir, "kui-dwca.zip")),
    (os.path.join(datadir, "kuit_coll.json"), os.path.join(datadir, "kuit-dwca.zip"))
]

# .....................................................................................
def test_get_collection():
    """Test post collection function."""
    for meta_fname, dwca_fname in TEST_COLLECTIONS:
        f = open(meta_fname)
        collection_json = json.load(f)
        collection_id = collection_json['collection_id']
        f.close()

        url = f"{COLLECTION_ENDPOINT}/{collection_id}"
        resp = requests.get(url, verify=False)
        if resp.status_code == 200:
            data = resp.json()

            print(f"Get collection data {collection_id} ok")
        else:
            print(
                'Get collection data at {} responded with code {}'.format(
                    url, resp.status_code)
            )



# .....................................................................................
def test_post_collection():
    """Test post collection function.

    Note:
        Returns 500 response when testing locally with verify=False, even though
        it accepts the data.
    """
    for meta_fname, dwca_fname in TEST_COLLECTIONS:
        f = open(meta_fname)
        collection_json = json.load(f)
        f.close()
        resp = requests.post(
            COLLECTION_ENDPOINT, json=collection_json, verify=False)
        if resp.status_code != 204:
            print(
                'Collection data post at {} responded with code {}'.format(
                    COLLECTION_ENDPOINT, resp.status_code)
            )


# .....................................................................................
def test_delete_collection():
    """Test post collection function.

    Note:
        Returns 404 response when testing locally with verify=False, even though
        it accepts the data.
    """
    for meta_fname, dwca_fname in TEST_COLLECTIONS:
        f = open(meta_fname)
        collection_json = json.load(f)
        collection_id = collection_json['collection_id']
        f.close()
        url = f"{COLLECTION_ENDPOINT}/{collection_id}"

        resp = requests.delete(url, verify=False)
        if resp.status_code != 204:
            print(
                'Collection delete at {} responded with code {}'.format(
                    url, resp.status_code)
            )


# .....................................................................................
def test_count_collection(expected_count=None):
    """Test post collection function."""
    url = f"{LOCAL_CACHE_API}/"
    if expected_count is None:
        expected_count = len(TEST_COLLECTIONS)
    resp = requests.get(url, verify=False)
    if resp.status_code == 200:
        data = resp.json()
        print(f"Expected {expected_count}, actual count is {data['num_collections']}")
    else:
        print(
            'Collection count at {} responded with code {}'.format(
                url, resp.status_code)
        )



# # .....................................................................................
# def test_post_get_collection():
#     """Test post collection function."""
#     for meta_fname, dwca_fname in TEST_COLLECTIONS:
#         f = open(meta_fname)
#         collection_json = json.load(f)
#         f.close()
#
#         collection_id = collection_json['collection_id']
#         collection = models.Collection(collection_json)
#         solr.post_collection(collection)
#         retval = solr.get_collection(collection_id)


# .....................................................................................
def test_submit_dwca():
    """Test various specimen operations."""
    for meta_fname, dwca_fname in TEST_COLLECTIONS:
        f = open(meta_fname)
        collection_json = json.load(f)
        f.close()
        collection_id = collection_json['collection_id']
        occurrence_endpoint = f"{LOCAL_CACHE_API}/collection/{collection_id}/occurrences/"

        with open(dwca_fname, mode='rb') as dwca_file:
            data = dwca_file.read()
        resp = requests.post(occurrence_endpoint, data=data, verify=False)
        if resp.status_code != 204:
            print(
                'Occurrence data post at {} responded with code {}'.format(
                    occurrence_endpoint, resp.status_code)
            )



# .....................................................................................
def test_submit_dwca_now():
    """Test various specimen operations."""
    http = urllib3.PoolManager(
        # cert_reqs='CERT_REQUIRED',
        # ca_certs=certifi.where()
    )
    for meta_fname, dwca_fname in TEST_COLLECTIONS:
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
    try:
        del os.environ['http_proxy']
    except:
        print("Proxy not set")
    test_count_collection()
    test_post_collection()
    test_count_collection(expected_count=len(TEST_COLLECTIONS))
    test_get_collection()
    # test_delete_collection()
    # test_count_collection(expected_count=0)
    test_submit_dwca()
