"""Aimee's Resolver Service class code with minimal modifications.

Note:
    The only modifications are to remove cherrypy references.
"""
from lmtrex.common.lmconstants import (ServiceProvider, APIService, SPECIFY)
import lmtrex.tools.solr as SpSolr
from lmtrex.services.api.v1.base import _S2nService
from lmtrex.services.api.v1.s2n_type import (S2nOutput, S2n, S2nKey, print_s2n_output)
from lmtrex.tools.utils import get_traceback

collection = 'spcoco'
solr_location = 'notyeti-192.lifemapper.org'

# .............................................................................
class ResolveSvc(_S2nService):
    """Query the Specify Resolver with a UUID for a resolvable GUID and URL"""
    SERVICE_TYPE = APIService.Resolve

    # ...............................................
    @staticmethod
    def get_url_from_meta(std_output):
        url = msg = None
        try:
            solr_doc = std_output[S2nKey.RECORDS][0]
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
                    msg = ('No direct record access to {}'.format(url))
                    url = None
        return (url, msg)

    # ...............................................
    def get_specify_records(self, occid):
        try:
            output = SpSolr.query_guid(
                occid, SPECIFY.RESOLVER_COLLECTION, SPECIFY.RESOLVER_LOCATION)
        except Exception as e:
            traceback = get_traceback()
            output = self.get_failure(
                provider=ServiceProvider.Specify[S2nKey.NAME], query_term=occid,
                errors=[traceback])
        return output.response

    # ...............................................
    def count_specify_guid_recs(self):
        std_output = SpSolr.count_docs(
            SPECIFY.RESOLVER_COLLECTION, SPECIFY.RESOLVER_LOCATION)
        return std_output

    # ...............................................
    def _show_online(self, providers=None):
        std_output = self.count_specify_guid_recs()
        std_output.set_value(S2nKey.SERVICE, self.SERVICE_TYPE)
        if providers is None:
            msg = 'S^n {} service is online'.format(self.SERVICE_TYPE)
        else:
            providers_str = ', '.join(providers)
            msg = 'S^n {} service is online for requested providers: '.format(
                    self.SERVICE_TYPE, providers_str)
            std_output.set_value(S2nKey.PROVIDER, providers_str)
            std_output.append_value(S2nKey.ERRORS, msg)
        return std_output

    # ...............................................
    def get_records(self, occid, req_providers):
        allrecs = []
        # for response metadata
        query_term = occid
        provnames = []
        for pr in req_providers:
            # Address single record
            if pr == ServiceProvider.Specify[S2nKey.PARAM]:
                sp_output = self._get_specify_records(occid)
                allrecs.append(sp_output)
                provnames.append(ServiceProvider.Specify[S2nKey.NAME])
        # Assemble
        provstr = ','.join(provnames)
        full_out = S2nOutput(
            len(allrecs), query_term, self.SERVICE_TYPE, provstr, records=allrecs,
            record_format=S2n.RECORD_FORMAT)
        return full_out


    # ...............................................
    def GET(self, occid=None, provider=None, **kwargs):
        """Get zero or one record for an identifier from the resolution
        service du jour (DOI, ARK, etc) or get a count of all records indexed
        by this resolution service.

        Args:
            occid: an occurrenceID, a DarwinCore field intended for a globally
                unique identifier (https://dwc.tdwg.org/list/#dwc_occurrenceID)
            kwargs: any additional keyword arguments are ignored

        Return:
            A dictionary of metadata and a count of records found in GBIF and
            an optional list of records.

        Note:
            There will never be more than one record returned.
        """
        try:
            usr_params = self._standardize_params(occid=occid, provider=provider)
            # Who to query
            valid_providers = self.get_providers()
            req_providers = self.get_valid_requested_providers(
                usr_params['provider'], valid_providers)

            # What to query: address one occurrence record, with optional filters
            occid = usr_params['occid']
            if occid is None:
                output = self._show_online()
            else:
                # What to query: common filters
                output = self.get_records(occid, req_providers)
        except Exception as e:
            traceback = get_traceback()
            output = self.get_failure(query_term=occid, errors=[traceback])
        return output.response


# .............................................................................
if __name__ == '__main__':
    # test
    from lmtrex.common.lmconstants import TST_VALUES

    for occid in TST_VALUES.GUIDS_WO_SPECIFY_ACCESS[:1]:
        print(occid)
        # Specify ARK Record
        svc = ResolveSvc()
        std_output = svc.GET(occid)
        print_s2n_output(std_output)
