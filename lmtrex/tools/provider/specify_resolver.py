from http import HTTPStatus

from lmtrex.common.lmconstants import (APIService, SYFTER, ServiceProvider)
from lmtrex.common.s2n_type import COMMUNITY_SCHEMA, S2nEndpoint, S2nKey, S2nOutput, S2nSchema
from lmtrex.tools.provider.api import APIQuery
from lmtrex.tools.s2n.utils import get_traceback, add_errinfo

# .............................................................................
class SpecifyResolverAPI(APIQuery):
    """Class to query Lifemapper portal APIs and return results"""
    PROVIDER = ServiceProvider.Specify
    RES_MAP = S2nSchema.get_specify_resolver_map()
    
    # ...............................................
    def __init__(self, ident=None, resource=SYFTER.RESOLVE_RESOURCE, logger=None):
        """Constructor
        
        Args:
            resource: Syftorium service to query
            ident: a Syftorium key for the specified resource.  If 
                ident is None, list using other_filters
            command: optional 'count' to query with other_filters
            other_filters: optional filters
            logger: optional logger for info and error messages.  If None, 
                prints to stdout    
        """
        base_url = SYFTER.REST_URL
        url = '{}/{}'.format(base_url, resource)
        if ident is not None:
            url = '{}/{}'.format(url, ident)
        APIQuery.__init__(self, url, logger=logger)
        
    # ...............................................
    @classmethod
    def _standardize_record(cls, rec):
        newrec = {}
        for stdfld, provfld in cls.RES_MAP.items():
            try:
                val = rec[provfld]
            except:
                val = None
            newrec[stdfld] =  val
            
        return newrec
    
    # ...............................................
    @classmethod
    def _standardize_output(
            cls, output, query_status=None, query_urls=[], errinfo={}):
        stdrecs = []
        total = 0
        if output:
            try:
                stdrecs.append(cls._standardize_record(output))
                total = 1
            except Exception as e:
                errinfo = add_errinfo(errinfo, 'error', cls._get_error_message(err=e))
                
        prov_meta = cls._get_provider_response_elt(query_status=query_status, query_urls=query_urls)
        std_output = S2nOutput(
            total, S2nEndpoint.Resolve, provider=prov_meta, 
            records=stdrecs, errors=errinfo)

        return std_output

    
# ...............................................
    @classmethod
    def count_docs(cls, logger=None):
        """Return an ARK record for a guid using the Specify resolver service.
        
        Args:
            guid: a unique identifier for a speciment record
            logger: optional logger for info and error messages.  If None, 
                prints to stdout    

        Return: 
            a dictionary containing one or more keys: 
                count, records, error, warning
            
        Example URL: 
            http://services.itis.gov/?q=nameWOInd:Spinus\%20tristis&wt=json
        """
        errinfo = {}
        api = SpecifyResolverAPI(logger=logger)

        try:
            api.query_by_get(output_type='json')
        except Exception as e:
            tb = get_traceback()
            errinfo = add_errinfo(errinfo, 'error', cls._get_error_message(err=tb))
            std_output = cls.get_api_failure(
                S2nEndpoint.Resolve, HTTPStatus.INTERNAL_SERVER_ERROR, errinfo=errinfo)
        else:
            try:
                count = api.output['count']
            except:
                errinfo = add_errinfo(errinfo, 'error', cls._get_error_message(msg='Missing `response` element'))
                std_output = cls.get_api_failure(
                    S2nEndpoint.Resolve, HTTPStatus.INTERNAL_SERVER_ERROR, errinfo=errinfo)
            else:
                errinfo = add_errinfo(errinfo, 'error', api.error)
            
            # Standardize output from provider response
            prov_meta = cls._get_provider_response_elt(query_status=api.status_code, query_urls=[api.url])
            std_output = S2nOutput(
                count, S2nEndpoint.Resolve, provider=prov_meta, errors=errinfo)
        return std_output

    # ...............................................
    @classmethod
    def resolve_guid_to_url(cls, occid):
        try:
            std_output = cls.query_for_guid(occid)
        except Exception as e:
            traceback = get_traceback()
            return traceback
        else:
            url = None
            if std_output.response[S2nKey.COUNT] > 0:
                rec = std_output.response['records'][0]
                fldname ='{}:api_url'.format(COMMUNITY_SCHEMA.S2N['code'])
                url = rec[fldname]
        return url

# ...............................................
    @classmethod
    def query_for_guid(cls, guid, logger=None):
        """Return an ARK record for a guid using the Specify resolver service.
        
        Args:
            guid: a unique identifier for a speciment record
            logger: optional logger for info and error messages.  If None, 
                prints to stdout    

        Return: 
            a dictionary containing one or more keys: 
                count, records, error, warning
            
        Example URL: 
            http://services.itis.gov/?q=nameWOInd:Spinus\%20tristis&wt=json
        """
        errinfo = {}
        api = SpecifyResolverAPI(ident=guid, logger=logger)

        try:
            api.query_by_get(output_type='json')
        except Exception as e:
            errinfo = add_errinfo(errinfo, 'error', cls._get_error_message(err=e))
            std_output = cls.get_api_failure(
                S2nEndpoint.Resolve, HTTPStatus.INTERNAL_SERVER_ERROR, errinfo=errinfo)
        else:
            if api.error:
                errinfo['error'] =  [api.error]
            # Standardize output from provider response
            std_output = cls._standardize_output(
                api.output, query_status=api.status_code, query_urls=[api.url], errinfo=errinfo)
        return std_output

    
# ...............................................

"""

curl https://dev.syftorium.org/api/v1/resolve/bffe655b-ea32-4838-8e80-a80e391d5b11

solr query through API: https://dev.syftorium.org/api/v1/resolve/2facc7a2-dd88-44af-b95a-733cc27527d4
response:
{"_version_":1701562470690193418,
"dataset_guid":"University of Kansas Biodiversity Institute Fish Tissue Collection",
"id":"2facc7a2-dd88-44af-b95a-733cc27527d4",
"url":"https://notyeti-195.lifemapper.org/api/v1/sp_cache/collection/ku_fish_tissue_test_1/specimens/2facc7a2-dd88-44af-b95a-733cc27527d4",
"what":"MaterialSample",
"when":"2021-06-03",
"where":"KU",
"who":"KUIT"}

direct solr query from localhost:  http://localhost:8983/solr/spcoco/select?q=2facc7a2-dd88-44af-b95a-733cc27527d4
response:
{
  "responseHeader":{
    "status":0,
    "QTime":0,
    "params":{
      "q":"2facc7a2-dd88-44af-b95a-733cc27527d4"}},
  "response":{"numFound":1,"start":0,"numFoundExact":true,"docs":[
      {
        "id":"2facc7a2-dd88-44af-b95a-733cc27527d4",
        "dataset_guid":"University of Kansas Biodiversity Institute Fish Tissue Collection",
        "who":"KUIT",
        "what":"MaterialSample",
        "when":"2021-06-03",
        "where":"KU",
        "url":"https://notyeti-195.lifemapper.org/api/v1/sp_cache/collection/ku_fish_tissue_test_1/specimens/2facc7a2-dd88-44af-b95a-733cc27527d4",
        "_version_":1701562470690193418}]
  }}

"""