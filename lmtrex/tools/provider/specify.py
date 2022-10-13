from http import HTTPStatus

from lmtrex.common.lmconstants import (JSON_HEADERS, ServiceProvider)
from lmtrex.common.s2n_type import S2nEndpoint, S2nOutput, S2nSchema
from lmtrex.tools.provider.api import APIQuery
from lmtrex.tools.s2n.utils import add_errinfo

# .............................................................................
class SpecifyPortalAPI(APIQuery):
    """Class to query Specify portal APIs and return results"""
    PROVIDER = ServiceProvider.Specify
    OCCURRENCE_MAP = S2nSchema.get_specify_occurrence_map()
    # ...............................................
    def __init__(self, url=None, logger=None):
        """Constructor for SpecifyPortalAPI class"""
        if url is None:
            url = 'http://preview.specifycloud.org/export/record'
        APIQuery.__init__(self, url, headers=JSON_HEADERS, logger=logger)

    # ...............................................
    @classmethod
    def _standardize_sp7_record(cls, rec):
        newrec = {}
        to_str_fields = ['dwc:year', 'dwc:month', 'dwc:day']
        for stdfld, provfld in cls.OCCURRENCE_MAP.items():
            try:
                val = rec[provfld]
            except:
                val = None

            if val and provfld in to_str_fields:
                newrec[stdfld] = str(val)
            else:
                newrec[stdfld] = val
        return newrec
                
    # ...............................................
    @classmethod
    def _standardize_sp6_record(cls, rec):
        newrec = {}
        to_str_prov_fields = ['year', 'month', 'day', 'decimalLongitude', 'decimalLatitude']
        mapping = S2nSchema.get_specifycache_occurrence_map()
        for stdfld, provfld in mapping.items():
            try:
                val = rec[provfld]
            except:
                val = None
            # Modify int date elements to string (to match iDigBio)
            if val and provfld in to_str_prov_fields:
                newrec[stdfld] = str(val)
            else:
                newrec[stdfld] =  val
        return newrec
                
    # ...............................................
    @classmethod
    def _standardize_output(
            cls, output, service, query_status=None, query_urls=[], count_only=False, errinfo={}):
        stdrecs = []
        total = 0
        is_specify_cache = False
        # Count
        if output:
            try:
                # Specify 7 record
                rec = output['core']
            except Exception as e:
                rec = output
                is_specify_cache = True
                
            if rec:
                total = 1
                # Records
                if not count_only:
                    if is_specify_cache:
                        stdrecs.append(cls._standardize_sp6_record(rec))
                    else:
                        stdrecs.append(cls._standardize_sp7_record(rec))
                        
        prov_meta = cls._get_provider_response_elt(query_status=query_status, query_urls=query_urls)
        std_output = S2nOutput(
            total, service, provider=prov_meta, records=stdrecs, errors=errinfo)

        return std_output

    # ...............................................
    @classmethod
    def get_specify_record(cls, occid, url, count_only, logger=None):
        """Return Specify record published at this url.  
        
        Args:
            url: direct url endpoint for source Specify occurrence record
            
        Note:                # Leave out fields without value

            Specify records/datasets without a server endpoint may be cataloged
            in the Solr Specify Resolver but are not resolvable to the host 
            database.  URLs returned for these records begin with 'unknown_url'.
        """
        errinfo = {}
        if url is None:
            errinfo = add_errinfo(errinfo, 'info', 'No URL to Specify record')
            std_output = cls._standardize_output(
                {}, S2nEndpoint.Occurrence, count_only=count_only, 
                errinfo=errinfo)
        elif url.startswith('http'):
            api = APIQuery(url, headers=JSON_HEADERS, logger=logger)
    
            try:
                api.query_by_get()
            except Exception as e:
                errinfo = add_errinfo(errinfo,'error', cls._get_error_message(err=e))
                std_output = cls.get_api_failure(
                    S2nEndpoint.Occurrence, HTTPStatus.INTERNAL_SERVER_ERROR, errinfo=errinfo)
            else:
                errinfo = add_errinfo(errinfo,'error', api.error)
                # Standardize output from provider response
                std_output = cls._standardize_output(
                    api.output, S2nEndpoint.Occurrence, query_status=api.status_code, 
                    query_urls=[url], count_only=count_only, errinfo=errinfo)
        
        return std_output
