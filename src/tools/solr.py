import json
import requests
import subprocess

SOLR_POST_COMMAND = '/opt/solr/bin/post'
SOLR_COMMAND = '/opt/solr/bin/solr'
ENCODING='utf-8'

"""
Defined solrcores in /var/solr/data/cores/
"""

# ...............................................
def _post_remote(collection, fname, solr_location='localhost', headers={}):
    response = output = None
    solr_endpt = 'http://{}:8983/solr'.format(solr_location)
    url = '{}/{}/update?commit=true'.format(solr_endpt, collection)
    # read doc as bytes
    with open(fname, 'rb', encoding=ENCODING) as in_file:
        data_bytes= in_file.read()
    try:
        response = requests.post(url, json=data_bytes, headers=headers)
    except Exception as e:
        if response is not None:
            retcode = response.status_code
        else:
            print('Failed on URL {} ({})'.format(url, str(e)))
    else:
        if response.ok:
            try:
                output = response.json()
            except Exception as e:
                try:
                    output = response.content
                except Exception:
                    output = response.text
                else:
                    print('Failed to interpret output of URL {} ({})'
                        .format(url, str(e)))
        else:
            try:
                retcode = response.status_code        
                reason = response.reason
            except:
                print('Failed to find failure reason for URL {} ({})'
                    .format(url, str(e)))
            else:
                print('Failed on URL {} ({}: {})'
                        .format(url, retcode, reason))
    return output


# .............................................................................
def _post_local(collection, fname):
    """Post a document to a Solr index.
    
    Args:
        collection: name of the Solr collection to be posted to 
        fname: Full path the file containing data to be indexed in Solr
        solr_location: URL to solr instance (i.e. http://localhost:8983/solr)
    """
    cmd = '{} -c {} {} '.format(SOLR_POST_COMMAND, collection, fname)
    output, _ = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    return output

# .............................................................................
def post(collection, fname, solr_location=None, headers=None):
    """Post a document to a Solr index.
    
    Args:
        collection: name of the Solr collection to be posted to 
        fname: Full path the file containing data to be indexed in Solr
        solr_location: URL to solr instance (i.e. http://localhost:8983/solr)
    """
    if solr_location is not None:
        _post_remote(collection, fname, solr_location, headers)
    else:
        _post_local(collection, fname)

# .............................................................................
def query_guid(collection, guid):
    output = query(collection, 'id:{}'.format(guid))
    jtemp = output.json()
    response = jtemp['response']
    if response['numFound'] == 1: 
        ret = response['docs'][0]
    return ret
    
# .............................................................................
def query(collection, qstr):
    url = '{}/{}/select?q={}'.format(SOLR_SERVER, collection, qstr)
    cmd = 'curl {}'.format(url)
    output, _ = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    return output

# .............................................................................
def update(collection):
    url = '{}/{}/update'.format(SOLR_SERVER, collection)
    cmd = 'curl {}'.format(url)
    output, _ = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    return output



"""
Post:
/opt/solr/bin/post -c spcoco /state/partition1/git/t-rex/data/solrtest/occurrence.solr.csv

Query:
curl http://localhost:8983/solr/spcoco/select?q=occurrence_guid:47d04f7e-73fa-4cc7-b50a-89eeefdcd162
curl http://notyeti-192.lifemapper.org:8983/solr/spcoco/select?q=*:*
"""
