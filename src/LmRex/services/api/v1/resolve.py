import cherrypy

from LmRex.common.lmconstants import (ServiceProvider, APIService)
import LmRex.tools.solr as SpSolr
from LmRex.services.api.v1.base import _S2nService

collection = 'spcoco'
solr_location = 'notyeti-192.lifemapper.org'

# .............................................................................
@cherrypy.expose
class _ResolveSvc(_S2nService):
    SERVICE_TYPE = APIService.Resolve

# .............................................................................
@cherrypy.expose
class SpecifyResolve(_ResolveSvc):
    """Query the Specify ARK resolver for a GUID"""
    PROVIDER = ServiceProvider.Specify

    # ...............................................
    @staticmethod
    def get_url_from_spark(solr_output):
        url = msg = None
        try:
            solr_doc = solr_output['docs'][0]
        except:
            pass
        else:
            # Get url from ARK for Specify query
            try:
                url = solr_doc['url']
            except Exception as e:
                pass
            else:
                if not url.startswith('http'):
                    msg = (
                        'Invalid URL {} returned from ARK for Specify record access'
                        .format(url))
                    url = None
        return (url, msg)
    
    # ...............................................
    def get_specify_arc_rec(self, occid):
        output = SpSolr.query_guid(collection, occid, solr_location=solr_location)
        try:
            output['count']
        except:
            output['error'] = 'Failed to return count from Specify Resolve'
        return output

    # ...............................................
    def count_specify_arc_recs(self):
        return SpSolr.count_docs(collection, solr_location)

    # ...............................................
    @cherrypy.tools.json_out()
    def GET(self, occid=None):
        """Get a single Specify ARK record for a GUID or count the total number 
        of records
        
        Args:
            occid: a Specify occurrence GUID, from the occurrenceId field
        Return:
            a single dictionary with a
             * count of records in the resolver or a
             * Specify ARK record
        """
        if occid is None:
            return self.count_specify_arc_recs()
        else:
            return self.get_specify_arc_rec(occid)

# .............................................................................
if __name__ == '__main__':
    # test
    from LmRex.common.lmconstants import TST_VALUES
    
    for occid in TST_VALUES.BIRD_OCC_GUIDS[:1]:
        print(occid)
        # Specify ARK Record
        spark = SpecifyResolve()
        solr_output = spark.GET(occid)
