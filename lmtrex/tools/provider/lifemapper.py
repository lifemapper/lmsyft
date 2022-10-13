from http import HTTPStatus

from lmtrex.common.lmconstants import (Lifemapper, ServiceProvider)
from lmtrex.common.s2n_type import S2nEndpoint, S2nOutput, S2nSchema
from lmtrex.tools.provider.api import APIQuery
from lmtrex.tools.s2n.utils import get_traceback, add_errinfo

# .............................................................................
class LifemapperAPI(APIQuery):
    """Class to query Lifemapper portal APIs and return results"""
    PROVIDER = ServiceProvider.Lifemapper
    MAP_MAP = S2nSchema.get_lifemapper_map_map()
    
    # ...............................................
    def __init__(
            self, resource=Lifemapper.PROJ_RESOURCE, ident=None, command=None,  
            other_filters={}, logger=None):
        """Constructor
        
        Args:
            resource: Lifemapper service to query
            ident: a Lifemapper database key for the specified resource.  If 
                ident is None, list using other_filters
            command: optional 'count' to query with other_filters
            other_filters: optional filters
            logger: optional logger for info and error messages.  If None, 
                prints to stdout    
        """
        url = '{}/{}'.format(Lifemapper.URL, resource)
        if ident is not None:
            url = '{}/{}'.format(url, ident)
            # do not send filters if retrieving a known object
            other_filters = {}
        elif command in Lifemapper.COMMANDS:
            url = '{}/{}'.format(url, command)
        APIQuery.__init__(self, url, other_filters=other_filters, logger=logger)
        
    # ...............................................
    @classmethod
    def _standardize_layer_record(cls, rec, prjscenariocodes=[], color=None):
        newrec = {}
        lyr_elt = scen_code = scen_link = num_points = pt_bbox = None
        
        link_fld = 'data_link'
        map_fld = 'map'
        endpt_fld = 'endpoint'
        sp_fld = 'species_name'
        type_fld = 'layer_type'
        name_fld = 'layer_name'
        status_fld = 'status'
        pt_count_fld = 'point_count'
        pt_bbox_fld = 'point_bbox'
        scencode_fld = 'sdm_projection_scenario_code'
        scenlink_fld = 'sdm_projection_scenario_link'
        
        # Required top level elements
        try:
            status = rec[status_fld]
            melt = rec[map_fld]
        except Exception:
            return {}
        else:
            # Required value
            if status != Lifemapper.COMPLETE_STAT_VAL:
                return {}
            # Required map level elements
            try:
                mapname = melt['map_name']
                map_url = melt[endpt_fld]
                layer_name = melt['layer_name']
                endpoint = '{}/{}'.format(map_url, mapname)
            except Exception as e:
                return {}
            
            # Required value if present
            try:
                scen_code = rec['projection_scenario']['code']
                scen_link = rec['projection_scenario']['metadata_url']
            except:
                pass
            else:
                # Discard records that do not pass filter
                if prjscenariocodes and scen_code not in prjscenariocodes:
                    return {}
                
            # Handle different field names between Occ and Proj
            try:
                species_name = rec[sp_fld]
            except:
                try:
                    species_name = rec['species_name']
                except: 
                    species_name = None
              
            # Must be spatialRaster or spatialVector
            try:
                lyr_elt = rec['spatial_raster']
                lyrtype = 'raster'
            except:
                try:
                    lyr_elt = rec['spatial_vector']
                    lyrtype = 'vector'
                except:
                    pass
                else:
                    try:
                        num_points = lyr_elt['num_features']
                        pt_bbox = lyr_elt['bbox']
                    except:
                        pass
            
            if lyr_elt:
                data_link = lyr_elt['data_url']

        for stdfld, provfld in cls.MAP_MAP.items():
            try:
                val = rec[provfld]
            except:
                val = None

            if provfld == sp_fld:
                newrec[stdfld] = species_name
                
            elif provfld == endpt_fld:
                newrec[stdfld] = endpoint
                
            elif provfld == type_fld:
                newrec[stdfld] = lyrtype
                
            elif provfld == name_fld:
                newrec[stdfld] = layer_name
                
            elif provfld == pt_count_fld:
                newrec[stdfld] = num_points
                
            elif provfld == pt_bbox_fld:
                newrec[stdfld] = pt_bbox
                
            elif provfld == pt_count_fld:
                newrec[stdfld] = num_points

            elif provfld == status_fld:
                newrec[stdfld] = status
            
            elif provfld == link_fld:
                newrec[stdfld] = data_link
            
            elif provfld == scencode_fld:
                newrec[stdfld] = scen_code
                                
            elif provfld == scenlink_fld:
                newrec[stdfld] = scen_link
                                
            else:
                newrec[stdfld] =  val

        if color is not None:
            newrec['vendor_specific_parameters'] = {'color': color}
        return newrec

    # ...............................................
    @classmethod
    def _standardize_map_output(
            cls, output, service, query_status=None, prjscenariocodes=None, color=None, count_only=False, 
            query_urls=[], errinfo={}):
        occ_layer_rec = None
        stdrecs = []
            
        # Records
        if len(output) > 0:
            try:
                occ_url = output[0]['occurrence_set']['metadata_url']
            except Exception as e:
                msg = cls._get_error_message('Failed to return occurrence URL')
                errinfo = add_errinfo(errinfo, 'error', msg)
            else:
                occ_rec = cls._get_occurrenceset_record(occ_url)
                occ_layer_rec = cls._standardize_layer_record(occ_rec)
        
        if occ_layer_rec and not count_only:
            stdrecs.append(occ_layer_rec)
            for r in output:
                try:
                    r2 = cls._standardize_layer_record(
                        r, prjscenariocodes=prjscenariocodes, color=color)
                    if r2:
                        stdrecs.append(r2)
                except Exception as e:
                    msg = cls._get_error_message(err=e)
                    errinfo = add_errinfo(errinfo, 'error', msg)
        
        # TODO: revisit record format for other map providers
        prov_meta = cls._get_provider_response_elt(query_status=query_status, query_urls=query_urls)
        std_output = S2nOutput(
            len(stdrecs), service, provider=prov_meta, records=stdrecs, errors=errinfo)

        return std_output
    
    # ...............................................
    @classmethod
    def _standardize_occ_output(
            cls, output, query_status=None, query_urls=[], color=None, count_only=False, errinfo={}):
        stdrecs = []
        total = len(output)
        # Records]
        if not count_only:
            for r in output:
                try:
                    stdrecs.append(cls._standardize_occ_record(r, color=color))
                except Exception as e:
                    msg = cls._get_error_message(err=e)
                    errinfo = add_errinfo(errinfo, 'error', msg)
        
        # TODO: revisit record format for other map providers
        prov_meta = cls._get_provider_response_elt(query_status=query_status, query_urls=query_urls)
        std_output = S2nOutput(
            count=total, record_format=Lifemapper.RECORD_FORMAT_OCC, records=stdrecs, 
            provider=prov_meta, errors=errinfo)

        return std_output

    # ...............................................
    @classmethod
    def find_map_layers_by_name(
            cls, name, prjscenariocodes=None, color=None, other_filters={}, 
            logger=None):
        """
        List projections for a given scientific name.  
        
        Args:
            name: a scientific name 'Accepted' according to the GBIF Backbone 
                Taxonomy
            prjscenariocodes: one or more Lifemapper codes indicating whether the 
                environmental data used for creating the projection is 
                observed, or modeled past or future.  Codes are in 
                LmREx.common.lmconstants Lifemapper.*_SCENARIO_CODE*. If the 
                code is None, return a map with only occurrence points
            color: a string indicating a valid color for displaying a predicted
                distribution map 
            logger: optional logger for info and error messages.  If None, 
                prints to stdout    

        Note: 
            Lifemapper contains only 'Accepted' name froms the GBIF Backbone 
            Taxonomy and this method requires them for success.
        """
        errinfo = {}
        other_filters[Lifemapper.NAME_KEY] = name
        other_filters[Lifemapper.ATOM_KEY] = 0
        api = LifemapperAPI(
            resource=Lifemapper.PROJ_RESOURCE, other_filters=other_filters)
        
        try:
            api.query_by_get()
        except Exception as e:
            tb = get_traceback()
            errinfo = add_errinfo(errinfo, 'error', cls._get_error_message(err=tb))
            
            std_output = cls.get_api_failure(
                S2nEndpoint.Map, HTTPStatus.INTERNAL_SERVER_ERROR, errinfo=errinfo)
        else:
            errinfo = add_errinfo(errinfo, 'error', api.error)
            
            std_output = cls._standardize_map_output(
                api.output, S2nEndpoint.Map, query_status=api.status_code, 
                query_urls=[api.url], prjscenariocodes=prjscenariocodes, color=color, 
                count_only=False, errinfo=errinfo)

        return std_output
   
    # ...............................................
    @classmethod
    def _get_occurrenceset_record(cls, url, logger=None):
        """
        Return occurrenceset for a given metadata url.  
        
        Args:
            id: a unique id for a Lifemapper occurrenceSet
            logger: optional logger for info and error messages.  If None, 
                prints to stdout    
        """
        rec = None
        errinfo = {}
        api = APIQuery(url)            
        try:
            api.query_by_get()
        except Exception as e:
            tb = get_traceback()
            errinfo = add_errinfo(errinfo, 'error', cls._get_error_message(err=tb))
            out = cls.get_api_failure(
                S2nEndpoint.Name, HTTPStatus.INTERNAL_SERVER_ERROR, errinfo=errinfo)
        else:
            try:
                rec = api.output
            except:
                pass
        return rec

# .............................................................................
if __name__ == '__main__':
    # test

    namestr = 'Plagioecia patina (Lamarck, 1816)'
    occset = LifemapperAPI.find_occurrencesets_by_name(namestr)

"""
http://client.lifemapper.org/api/v2/sdmproject?displayname=Conibiosoma%20elongatum&projectionscenariocode=worldclim-curr
http://client.lifemapper.org/api/v2/occurrence?displayname=Conibiosoma%20elongatum
"""
