from werkzeug.exceptions import BadRequest, InternalServerError

from lmtrex.common.lmconstants import (APIService, ServiceProvider, TST_VALUES)
from lmtrex.common.s2n_type import (S2nKey, S2nOutput, S2nSchema, print_s2n_output)
from lmtrex.flask_app.broker.base import _S2nService
from lmtrex.tools.provider.gbif import GbifAPI
from lmtrex.tools.provider.lifemapper import LifemapperAPI
from lmtrex.tools.s2n.utils import get_traceback, combine_errinfo, add_errinfo

# .............................................................................
class MapSvc(_S2nService):
    SERVICE_TYPE = APIService.Map
    ORDERED_FIELDNAMES = S2nSchema.get_s2n_fields(APIService.Map['endpoint'])
    
    # ...............................................
    @classmethod
    def _match_gbif_names(cls, namestr, is_accepted):
        scinames = []
        errinfo = {}
        try:
            # Get name from Gbif        
            nm_output = GbifAPI.match_name(namestr, is_accepted=is_accepted)
        except Exception as e:
            # emsg = 'Failed to match {} to GBIF accepted name'.format(namestr)
            traceback = get_traceback()
            errinfo = add_errinfo(errinfo, 'error', traceback)
            scinames.append(namestr)
        else:
            for rec in nm_output.records:
                try:
                    scinames.append(rec['s2n:scientific_name'])
                except Exception as e:
                    errinfo['warning'].append(
                        'No scientificName element in GBIF record {} for {}'.format(rec, namestr))
        return scinames, errinfo

    # ...............................................
    @classmethod
    def _get_lifemapper_records(cls, namestr, is_accepted, scenariocodes, color):
        errinfo = {}
        # First: get name(s)
        if is_accepted is False:
            scinames = [namestr] 
        else:
            scinames, errinfo = cls._match_gbif_names(namestr, is_accepted=is_accepted)
        # Second: get completed Lifemapper projections (map layers)
        stdrecs = []
        statii = []
        queries = []
        for sname in scinames:
            # TODO: search on occurrenceset, then also pull projection layers
            try:
                lout = LifemapperAPI.find_map_layers_by_name(
                    sname, prjscenariocodes=scenariocodes, color=color)
            except Exception as e:
                traceback = get_traceback()
                errinfo = add_errinfo(errinfo, 'error', traceback)
                    
            else:
                queries.extend(lout.provider_query)
                errinfo = combine_errinfo(errinfo, lout.errors)
                statii.append(lout.provider_status_code)
                # assemble all records, errors, statuses, queries for provider metadata element
                if len(lout.records) > 0:
                    stdrecs.extend(lout.records)
        prov_meta = LifemapperAPI._get_provider_response_elt(
            query_status=statii, query_urls=queries)
        # query_term = 'namestr={}&is_accepted={}&scenariocodes={}&color={}'.format(
        #     namestr, is_accepted, scenariocodes, color)
        full_out = S2nOutput(
            len(stdrecs), cls.SERVICE_TYPE['endpoint'], prov_meta, 
            records=stdrecs, record_format=cls.SERVICE_TYPE[S2nKey.RECORD_FORMAT], errors=errinfo)
        full_out.format_records(cls.ORDERED_FIELDNAMES)
        return full_out.response

    # ...............................................
    @classmethod
    def _get_records(
            cls, namestr, req_providers, is_accepted, scenariocodes, color):
        allrecs = []
        # for response metadata
        query_term = ''
        if namestr is not None:
            sc = scenariocodes
            if scenariocodes:
                sc = ','.join(scenariocodes)
            query_term = 'namestr={}&provider={}&is_accepted={}&scenariocodes={}&color={}'.format(
                namestr, ','.join(req_providers), is_accepted, sc, color)
        provnames = []
        for pr in req_providers:
            # Lifemapper
            if pr == ServiceProvider.Lifemapper[S2nKey.PARAM]:
                lmoutput = cls._get_lifemapper_records(
                    namestr, is_accepted, scenariocodes, color)
                allrecs.append(lmoutput)
                provnames.append(ServiceProvider.Lifemapper[S2nKey.NAME])
        # Assemble
        prov_meta = cls._get_s2n_provider_response_elt(query_term=query_term)
        # TODO: Figure out why errors are retained from query to query!!!  Resetting to {} works.
        full_out = S2nOutput(
            len(allrecs), cls.SERVICE_TYPE['endpoint'], provider=prov_meta, 
            records=allrecs, errors={})
        return full_out

    # ...............................................
    @classmethod
    def get_map_meta(cls, namestr=None, provider=None, is_accepted=True, gbif_parse=True,  
            scenariocode=None, color=None, **kwargs):
        """Get one or more taxon records for a scientific name string from each
        available name service.
        
        Args:
            namestr: a scientific name
            provider: comma-delimited list of requested provider codes.  Codes are delimited
                for each in lmtrex.common.lmconstants ServiceProvider
            is_accepted: flag to indicate whether to limit to 'valid' or  'accepted' taxa 
                in the ITIS or GBIF Backbone Taxonomy
            gbif_parse: flag to indicate whether to first use the GBIF parser to parse a 
                scientific name into canonical name
            scenariocode: for Lifemapper (lm) provider, filter available SDM (predicted distribution) 
                layer results by the given scenariocode (for past, present, or future predicted 
                distribution)
            color:  for Lifemapper (lm) provider, name of the desired color palette for predicted 
                distribution layers.  Options at lmtrex.common.lmconstants Lifemapper.VALID_PALETTES
        Return:
            A lmtrex.common.s2n_type S2nOutput object containing records for each provider.  Each provider 
            element is a S2nOutput object with records as a list of dictionaries following the 
            lmtrex.common.s2n_type S2nSchema.MAP corresponding to map layers available from the provider.
        """
        if namestr is None:
            return cls.get_endpoint()
        else:   
            try:
                good_params, errinfo = cls._standardize_params(
                    namestr=namestr, provider=provider, gbif_parse=gbif_parse, 
                    is_accepted=is_accepted, scenariocode=scenariocode, color=color)
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
                # Do Query!
                output = cls._get_records(
                    good_params['namestr'], good_params['provider'], good_params['is_accepted'], 
                    good_params['scenariocode'], good_params['color'])
                
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
    names = TST_VALUES.NAMES[5:9]
    names = ['Eucosma raracana', 'Tulipa sylvestris', 'Phlox longifolia Nutt']
    # names.insert(0, None)
    svc = MapSvc()
    for namestr in names:
        for scodes in (None, 'worldclim-curr'):
            for prov in svc.get_providers():
                out = MapSvc.get_map_meta(namestr=namestr, scenariocode=scodes)
                print_s2n_output(out, do_print_rec=True)

"""
http://broker-dev.spcoco.org/api/v1/map/?provider=lm&namestr=test
https://data.lifemapper.org/api/v2/ogc/data_24209?layers=occ_24209&service=wms&request=getmap&styles=&format=image%2Fpng&transparent=true&version=1.0&height=800&srs=EPSG%3A3857&width=800&layerLabel=Occurrence%20Points&bbox=0,0,20037508.342789244,20037508.34278071
"""