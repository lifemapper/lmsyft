from lmtrex.common.lmconstants import (
    APIService, DWC, JSON_HEADERS, ServiceProvider, TST_VALUES)
from lmtrex.services.api.v1.s2n_type import S2nKey, S2nOutput
from lmtrex.tools.provider.api import APIQuery

# .............................................................................
class SpecifyPortalAPI(APIQuery):
    """Class to query Specify portal APIs and return results"""
    PROVIDER = ServiceProvider.Specify[S2nKey.NAME]
    # ...............................................
    def __init__(self, url=None, logger=None):
        """Constructor for SpecifyPortalAPI class"""
        if url is None:
            url = 'http://preview.specifycloud.org/export/record'
        APIQuery.__init__(self, url, headers=JSON_HEADERS, logger=logger)


    # ...............................................
    @classmethod
    def _standardize_record(cls, rec):
        # todo: standardize gbif output to DWC, DSO, etc
        return rec
    
    # ...............................................
    @classmethod
    def _standardize_output(
            cls, output, query_term, service, provider_query=[], count_only=False, err=None):
        stdrecs = []
        total = 0
        errmsgs = []
        if err is not None:
            errmsgs.append(err)
        # Count
        try:
            recs = list(output)
        except Exception as e:
            errmsgs.append(cls._get_error_message(err=e))
        else:
            total = len(recs)
        # Records
        if not count_only:
            for r in recs:
                try:
                    stdrecs.append(cls._standardize_record(r))
                except Exception as e:
                    msg = cls._get_error_message(err=e)
                    errmsgs.append(msg)

        # TODO: make sure Specify is using full DWC              
#         out = cls._standardize_output(
#             total, Idigbio.COUNT_KEY, Idigbio.RECORDS_KEY, 
#             Idigbio.RECORD_FORMAT, occid, APIService.Occurrence, 
#             count_only=count_only, err=api.error)
        std_output = S2nOutput(
            total, query_term, service, cls.PROVIDER, 
            provider_query=provider_query, record_format=DWC.SCHEMA, 
            records=stdrecs, errors=errmsgs)

        return std_output

    # ...............................................
    @classmethod
    def get_specify_record(cls, occid, url, count_only, logger=None):
        """Return Specify record published at this url.  
        
        Args:
            url: direct url endpoint for source Specify occurrence record
            
        Note:
            Specify records/datasets without a server endpoint may be cataloged
            in the Solr Specify Resolver but are not resolvable to the host 
            database.  URLs returned for these records begin with 'unknown_url'.
        """
        if url is None:
            recs = []
            std_output = cls._standardize_output(
                recs, occid, APIService.Occurrence, provider_query=[], 
                count_only=count_only)
        elif url.startswith('http'):
            api = APIQuery(url, headers=JSON_HEADERS, logger=logger)
    
            try:
                api.query_by_get()
            except Exception as e:
                std_output = cls.get_failure(errors=[cls._get_error_message(err=e)])
            # Standardize output from provider response
            std_output = cls._standardize_output(
                api.output, occid, APIService.Occurrence, 
                provider_query=[url], count_only=count_only, err=api.error)
        
#         full_out = S2nOutput(
#             count=out.count, record_format=out.record_format, 
#             records=out.records, provider=cls.PROVIDER, errors=out.errors, 
#             provider_query=[url], query_term=occid, 
#             service=APIService.Occurrence)
        return std_output
