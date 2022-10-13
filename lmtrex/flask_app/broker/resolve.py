from http import HTTPStatus
from werkzeug.exceptions import (BadRequest, InternalServerError)

from lmtrex.common.lmconstants import (APIService)
from lmtrex.common.s2n_type import (S2nKey, S2nOutput, S2nSchema, print_s2n_output)
from lmtrex.flask_app.broker.base import _S2nService
from lmtrex.tools.provider.specify_resolver import SpecifyResolverAPI
from lmtrex.tools.s2n.utils import get_traceback

collection = 'spcoco'
solr_location = 'notyeti-192.lifemapper.org'

# .............................................................................
class ResolveSvc(_S2nService):
    """Query the Specify Resolver with a UUID for a resolvable GUID and URL"""
    SERVICE_TYPE = APIService.Resolve
    ORDERED_FIELDNAMES = S2nSchema.get_s2n_fields(APIService.Resolve['endpoint'])

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
    @classmethod
    def resolve_specify_guid(cls, occid):
        try:
            output = SpecifyResolverAPI.query_for_guid(occid)
            output.format_records(cls.ORDERED_FIELDNAMES)
        except Exception as e:
            traceback = get_traceback()
            output = SpecifyResolverAPI.get_api_failure(
                cls.SERVICE_TYPE['endpoint'], HTTPStatus.INTERNAL_SERVER_ERROR, 
                errinfo={'error': [traceback]})
        return output.response

    # ...............................................
    @classmethod
    def count_resolvable_specify_recs(cls):
        std_output = SpecifyResolverAPI.count_docs()
        return std_output.response
    

    # ...............................................
    @classmethod
    def _get_records(cls, occid):
        allrecs = []
        query_term = 'occid={}'.format(occid)
        # Address single record
        sp_output = cls.resolve_specify_guid(occid)
        allrecs.append(sp_output)
        # Assemble
        prov_meta = cls._get_s2n_provider_response_elt(query_term=query_term)
        full_out = S2nOutput(
            len(allrecs), cls.SERVICE_TYPE['endpoint'], provider=prov_meta, 
            records=allrecs, errors={})
        return full_out

        
    # ...............................................
    @classmethod
    def get_guid_resolution(cls, occid=None, **kwargs):
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
        if occid is None:
            return cls.get_endpoint()
        elif occid.lower() == 'count':
            return cls.count_resolvable_specify_recs()
        else:   
            try:
                good_params, errinfo = cls._standardize_params(occid=occid)
                # Bad parameters
                try:
                    error_description = '; '.join(errinfo['error'])                            
                    raise BadRequest(error_description)
                except:
                    pass

            except Exception as e:
                error_description = get_traceback()
                raise InternalServerError(error_description)
            
            try:
                output = cls._get_records(good_params['occid'])

                # Add message on invalid parameters to output
                try:
                    for err in errinfo['warning']:
                        output.append_error('warning', err)
                except:
                    pass

            except Exception as e:
                error_description = get_traceback()
                raise InternalServerError(error_description)
        return output.response


# .............................................................................
if __name__ == '__main__':
    # test
    from lmtrex.common.lmconstants import TST_VALUES
    
    params = [None, TST_VALUES.GUIDS_W_SPECIFY_ACCESS[0]]
    for occid in params:
        print(occid)
        # Specify ARK Record
        svc = ResolveSvc()
        std_output = svc.GET(occid)
        print_s2n_output(std_output, do_print_rec=True)
