from http import HTTPStatus

from lmtrex.common.lmconstants import (MorphoSource, ServiceProvider, TST_VALUES)
from lmtrex.common.s2n_type import S2nEndpoint, S2nKey, S2nSchema
from lmtrex.tools.fileop.logtools import (log_info)
from lmtrex.tools.provider.api import APIQuery
from lmtrex.tools.s2n.utils import add_errinfo, get_traceback

# .............................................................................
class MorphoSourceAPI(APIQuery):
    """Class to query Specify portal APIs and return results"""
    PROVIDER = ServiceProvider.MorphoSource
    OCCURRENCE_MAP = S2nSchema.get_mopho_occurrence_map()
    
    # ...............................................
    def __init__(
            self, resource=MorphoSource.OCC_RESOURCE, q_filters={}, 
            other_filters={}, logger=None):
        """Constructor for MorphoSourceAPI class"""
        url = '{}/{}/{}'.format(
            MorphoSource.REST_URL, MorphoSource.COMMAND, resource)
        APIQuery.__init__(
            self, url, q_filters=q_filters, 
            other_filters=other_filters, logger=logger)

    # ...............................................
    @classmethod
    def _standardize_record(cls, rec):
        newrec = {}
        view_std_fld = S2nSchema.get_view_url_fld()
        data_std_fld = S2nSchema.get_data_url_fld()
        for stdfld, provfld in cls.OCCURRENCE_MAP.items():
            try:
                val = rec[provfld]
            except:
                val = None

            # Save ID field, plus use to construct URLs
            if provfld == MorphoSource.DWC_ID_FIELD:
                newrec[stdfld] =  val
                newrec[data_std_fld] = MorphoSource.get_occurrence_data(val)
                
            # Use local ID field to also construct webpage url
            elif provfld == MorphoSource.LOCAL_ID_FIELD:
                newrec[view_std_fld] = MorphoSource.get_occurrence_view(val)
                
            # all others
            else:
                newrec[stdfld] =  val
        return newrec
    
    # ...............................................
    @classmethod
    def get_occurrences_by_occid_page1(cls, occid, count_only=False, logger=None):
        start = 0
        errinfo = {}
        api = MorphoSourceAPI(
            resource=MorphoSource.OCC_RESOURCE, 
            q_filters={MorphoSource.OCCURRENCEID_KEY: occid},
            other_filters={'start': start, 'limit': MorphoSource.LIMIT})
        # Handle bad SSL certificate on old MorphoSource API until v2 is working
        verify=True
        if api.url.index(MorphoSource.REST_URL) >= 0:
            verify=False
        try:
            api.query_by_get(verify=verify)
        except Exception as e:
            tb = get_traceback()
            errinfo = add_errinfo(errinfo, 'error', cls._get_error_message(err=tb))
            std_out = cls.get_api_failure(
                S2nEndpoint.Occurrence, HTTPStatus.INTERNAL_SERVER_ERROR, errinfo=errinfo)
        else:
            # Standardize output from provider response
            if api.error:
                errinfo['error'] =  [api.error]

            std_out = cls._standardize_output(
                api.output, MorphoSource.TOTAL_KEY, MorphoSource.RECORDS_KEY, 
                MorphoSource.RECORD_FORMAT, S2nEndpoint.Occurrence, 
                query_status=api.status_code, query_urls=[api.url], count_only=count_only, 
                errinfo=errinfo)
        
        return std_out

# .............................................................................
if __name__ == '__main__':
    # test
    
    for guid in TST_VALUES.GUIDS_WO_SPECIFY_ACCESS:
        moutput = MorphoSourceAPI.get_occurrences_by_occid_page1(guid)
        for r in moutput.response[S2nKey.RECORDS]:
            occid = notes = None
            try:
                occid = r['specimen.occurrence_id']
                notes = r['specimen.notes']
            except Exception as e:
                msg = 'Morpho source record exception {}'.format(e)
            else:
                msg = '{}: {}'.format(occid, notes)
            log_info(msg)

"""
https://ms1.morphosource.org/api/v1/find/specimens?start=0&limit=1000&q=occurrence_id%3Aed8cfa5a-7b47-11e4-8ef3-782bcb9cd5b5'
url = 'https://ea-boyerlab-morphosource-01.oit.duke.edu/api/v1/find/specimens?start=0&limit=1000&q=occurrence_id%3Aed8cfa5a-7b47-11e4-8ef3-782bcb9cd5b5'
"""