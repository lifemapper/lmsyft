"""OBSOLETE: Tools for local or remote Solr collections."""
import requests
import subprocess

from flask_app.broker.constants import SPECIFY, TST_VALUES
from flask_app.common.s2n_type import S2nKey, S2nOutput

from sppy.tools.provider.api import APIQuery

SOLR_POST_COMMAND = "/opt/solr/bin/post"
SOLR_COMMAND = "/opt/solr/bin/solr"
CURL_COMMAND = "/usr/bin/curl"
ENCODING = "utf-8"
"""
Defined solrcores in /var/solr/data/cores/
"""


# ......................................................
def _get_record_format(collection):
    return "Solr schema {} TBD".format(collection)


# ......................................................
def count_docs(collection, solr_location):
    """Count the number of documents in a Solr collection.

    Args:
        collection: collection for counting
        solr_location: URL to solr instance (i.e. http://localhost:8983/solr)

    Returns:
        Number of documents found.
    """
    output = query(collection, solr_location, query_term="*")
    # Remove records from output
    output.set_value(S2nKey.RECORDS, [])
    return output


# ...............................................
def _post_remote(collection, fname, solr_location, headers=None):
    response = output = retcode = None

    solr_endpt = f"http://{solr_location}:8983/solr"
    url = f"{solr_endpt}/{collection}/update"
    params = {"commit" : "true"}
    with open(fname, "r", encoding=ENCODING) as in_file:
        data = in_file.read()

    try:
        response = requests.post(url, data=data, params=params, headers=headers)
    except Exception as e:
        if response is not None:
            retcode = response.status_code
        else:
            print(f"Failed on URL {url} ({e})")
    else:
        if response.ok:
            retcode = response.status_code
            try:
                output = response.json()
            except Exception as e:
                try:
                    output = response.content
                except Exception:
                    output = response.text
                else:
                    print(f"Failed to interpret output of URL {url} ({e})")
        else:
            try:
                retcode = response.status_code
                reason = response.reason
            except Exception:
                print(f"Failed to get status_code from {url}")
            else:
                print(f"Failed on URL {url} ({retcode}: {reason}")
                print("Full response:")
                print(response.text)
    return retcode, output


# .............................................................................
def _post_local(fname, collection):
    cmd = f"{SOLR_POST_COMMAND} -c {collection} {fname} "
    output, _ = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    return output


# .............................................................................
def post(fname, collection, solr_location, headers=None):
    """Post a document to a Solr index.

    Args:
        fname: Full path the file containing data to be indexed in Solr
        collection: name of the Solr collection to be posted to
        solr_location: URL to solr instance (i.e. http://localhost:8983/solr)
        headers: optional keyword/values to send server

    Returns:
        retcode: HTTP code from the server.
        output: response from the server.
    """
    retcode = 0
    if solr_location is not None:
        retcode, output = _post_remote(fname, collection, solr_location, headers)
    else:
        output = _post_local(fname, collection)
    return retcode, output


# .............................................................................
def query_guid(guid, collection, solr_location):
    """Query a Solr index and return results for a guid.

    Args:
        guid: Unique identifier for record of interest
        collection: name of the Solr index
        solr_location: URL to solr instance (i.e. http://localhost:8983/solr)

    Returns:
        a dictionary containing one or more keys: count, docs, error
    """
    return query(collection, solr_location, filters={"id": guid}, query_term=guid)


# .............................................................................
def query(collection, solr_location, filters=None, query_term="*"):
    """Query a solr index and return results in JSON format.

    Args:
        collection: solr index for query
        solr_location: URL to solr instance (i.e. http://localhost:8983/solr)
        filters: q filters for solr query
        query_term: string for querying the index.

    Returns:
        a flask_app.broker.s2n_type.S2nOutput object
    """
    errmsgs = []
    if filters is None:
        filters = {"*": "*"}

    solr_endpt = f"http://{solr_location}:8983/solr/{collection}/select"
    api = APIQuery(solr_endpt, q_filters=filters)
    api.query_by_get(output_type="json")
    try:
        response = api.output["response"]
    except KeyError:
        errmsgs.append("Missing `response` element")
    else:
        try:
            count = response["numFound"]
        except KeyError:
            errmsgs.append("Failed to return numFound from solr")
        try:
            recs = response["docs"]
        except KeyError:
            errmsgs.append("Failed to return docs from solr")

    service = provider = ""
    record_format = _get_record_format(collection)
    std_output = S2nOutput(
        count, service, provider, record_format=record_format, records=recs,
        errors=errmsgs
    )

    return std_output


# .............................................................................
def update(collection, solr_location):
    """Update a solr index and return results in JSON format.

    Args:
        collection: solr index for query
        solr_location: URL to solr instance (i.e. http://localhost:8983/solr)

    Returns:
        the Solr response.
    """
    url = f"{solr_location}/{collection}/update"
    cmd = f"{CURL_COMMAND} {url}"
    output, _ = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    return output


# .............................................................................
if __name__ == "__main__":
    # test
    ct = count_docs(SPECIFY.RESOLVER_COLLECTION, SPECIFY.RESOLVER_LOCATION)
    print(f"Found {ct} records in {SPECIFY.RESOLVER_COLLECTION}")
    for guid in TST_VALUES.GUIDS_W_SPECIFY_ACCESS:
        doc = query_guid(
            guid, SPECIFY.RESOLVER_COLLECTION, SPECIFY.RESOLVER_LOCATION)
        print(f"Found {ct} record for guid {guid}")

"""
Post:
/opt/solr/bin/post -c spcoco /state/partition1/git/t-rex/data/solrtest/occurrence.solr.csv

Query:
curl http://notyeti-192.lifemapper.org:8983/solr/spcoco/select
 ?q=occurrence_guid:47d04f7e-73fa-4cc7-b50a-89eeefdcd162
curl http://notyeti-192.lifemapper.org:8983/solr/spcoco/select?q=*:*
"""
