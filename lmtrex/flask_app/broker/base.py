from flask import Flask, Blueprint, request, json
from werkzeug.exceptions import HTTPException

import lmtrex.tools.s2n.utils as lmutil
from lmtrex.common.lmconstants import (APIService, BrokerParameters, ServiceProvider)
from lmtrex.common.s2n_type import S2nEndpoint, S2nKey, S2nOutput
from lmtrex.tools.provider.gbif import GbifAPI
from lmtrex.tools.provider.itis import ItisAPI

app = Flask(__name__)

# .............................................................................
class _S2nService:
    """Base S-to-the-N service, handles parameter names and acceptable values"""
    # overridden by subclasses
    SERVICE_TYPE = None

    # ...............................................
    @classmethod
    def _get_s2n_provider_response_elt(cls, query_term=None):
        provider_element = {}
        s2ncode = ServiceProvider.Broker[S2nKey.PARAM]
        provider_element[S2nKey.PROVIDER_CODE] = s2ncode
        provider_element[S2nKey.PROVIDER_LABEL] = ServiceProvider.Broker[S2nKey.NAME]
        icon_url = lmutil.get_icon_url(s2ncode)
        if icon_url:
            provider_element[S2nKey.PROVIDER_ICON_URL] = icon_url
        # Status will be 200 if anyone ever sees this
        provider_element[S2nKey.PROVIDER_STATUS_CODE] = 200
        # Handle local debugging
        try:
            # TODO: get from headers
            base_url = 'https://broker.spcoco.org'
            # base_url = cherrypy.request.headers['Origin']
        except:
            base_url = 'https://localhost'
        # Optional URL queries
        standardized_url = '{}{}/{}'.format(
            base_url,
            S2nEndpoint.Root,
            cls.SERVICE_TYPE['endpoint']
        )
        if query_term:
            standardized_url = '{}?{}'.format(standardized_url, query_term)
        provider_element[S2nKey.PROVIDER_QUERY_URL] = [standardized_url]
        return provider_element

    # ...............................................
    @classmethod
    def _order_providers(cls, provnames):
        provnames = list(provnames)
        provnames.sort()
        if ServiceProvider.Specify[S2nKey.PARAM] in provnames:
            provnames.remove(ServiceProvider.Specify[S2nKey.PARAM])
            provnames.insert(0, ServiceProvider.Specify[S2nKey.PARAM])
        return provnames
    
    # ...............................................
    @classmethod
    def get_providers(cls, filter_params=None):
        """ Return a list of strings indicating all providers valid for this service. 
        
        Note:
            This returns a list of provider values.  If Specify Resolver is present, it will 
            be first, all following providers are in alphabetical order.  The values are used 
            as URL query parameters.  
        Note: 
            The order of these providers determines the order of records returned for 
            multi-provider responses. 
        """
        provnames = set()
        # Ignore as-yet undefined filter_params
        for p in ServiceProvider.all():
            if cls.SERVICE_TYPE['endpoint'] in p[S2nKey.SERVICES]:
                provnames.add(p[S2nKey.PARAM])        
        provnames = cls._order_providers(provnames)
        return provnames

    # ...............................................
    @classmethod
    def _get_valid_providers(cls, filter_params=None):
        """ Return a list of strings indicating all providers valid for this service. 

        Note:
            This returns a list of provider values.  If Specify Resolver is present, it will 
            be first, all following providers are in alphabetical order.  The values are used 
            as URL query parameters.  
        Note: 
            The order of these providers determines the order of records returned for 
            multi-provider responses. 
        """
        provnames = set()
        # Ignore as-yet undefined filter_params
        for p in ServiceProvider.all():
            if cls.SERVICE_TYPE['endpoint'] in p[S2nKey.SERVICES]:
                provnames.add(p[S2nKey.PARAM])
        provnames = cls._order_providers(provnames)
        return provnames
    
    # .............................................................................
    @classmethod
    def _get_valid_requested_params(cls, user_params_string, valid_params):
        """
        Return valid requested and invalid options for parameters that accept multiple values,
        including provider and scenariocode parameters.
        
        Note: 
            provider: 
                For the badge service, exactly one provider is required.  For all other services, 
                multiple providers are accepted, and None indicates to query all valid providers.
            scenariocode: 
                For the map service, multiple scenariocode are accepted, and None indicates to 
                return map layers computed with all valid scenariocodes.
        """
        valid_requested_params = invalid_params = []
        
        if user_params_string:
            tmplst = user_params_string.split(',')
            user_params = set([tp.lower().strip() for tp in tmplst])
            
            valid_requested_params = set()
            invalid_params = set()
            # valid_requested_providers, invalid_providers = cls.get_multivalue_options(user_provs, valid_providers)
            for param in user_params:
                if param in valid_params:
                    valid_requested_params.add(param)
                else:
                    invalid_params.add(param)
                    
            invalid_params = list(invalid_params)
            if valid_requested_params:
                valid_requested_params = list(valid_requested_params)
            else:
                valid_requested_params = []
                        
        return valid_requested_params, invalid_params

    # .............................................................................
    @classmethod
    def get_failure(cls, service=None, query_term=None, errors={}):
        """Output format for all (soon) S^n services
        
        Args:
            service: type of S^n services
            query_term: query term provided by the user, ex: name or id
            provider: original data provider metadata
            errors: list of info messages, warnings and errors (dictionaries)
            
        Return:
            lmtrex.services.api.v1.S2nOutput object
        """
        if not service: 
            service = cls.SERVICE_TYPE['endpoint']
        prov_meta = cls._get_s2n_provider_response_elt(query_term=query_term)
        all_output = S2nOutput(
            0, service, provider=prov_meta, errors=errors)
        return all_output

    # .............................................................................
    @classmethod
    def endpoint(cls):
        endpoint =  '{}/{}'.format(S2nEndpoint.Root, cls.SERVICE_TYPE['endpoint'])
        return endpoint

    # ...............................................
    @classmethod
    def get_endpoint(cls, **kwargs):
        try:
            valid_providers = cls._get_valid_providers()
            output = cls._show_online(valid_providers)
        except Exception as e:
            raise
        return output.response

    # ...............................................
    @classmethod
    def _show_online(cls, providers):
        svc = cls.SERVICE_TYPE['endpoint']
        info = {
            'info': 'S^n {} service is online.'.format(svc)}

        param_lst = []
        for p in cls.SERVICE_TYPE['params']:
            pinfo = BrokerParameters[p].copy()
            pinfo['type'] = str(type(pinfo['type']))
            if providers is not None and p == 'provider':
                pinfo['options'] = list(providers)
            param_lst.append({p: pinfo})
        info['parameters'] = param_lst
        
        prov_meta = cls._get_s2n_provider_response_elt()
        
        output = S2nOutput(0, svc, provider=prov_meta, errors=info)
        return output

    # ...............................................
    @classmethod
    def parse_name_with_gbif(cls, namestr):
        output = GbifAPI.parse_name(namestr)
        try:
            rec = output['record']
        except:
            # Default to original namestring if parsing fails
            pass
        else:
            success = rec['parsed']
            namestr = rec['canonicalName']
            
            if success:
                if namestr.startswith('? '):
                    namestr = rec['scientificName']
        return namestr

    # ...............................................
    def match_name_with_itis(self, namestr):
        output = ItisAPI.match_name(namestr, status='valid')
        try:
            namestr = output['records'][0]['nameWOInd']
        except:
            # Default to original namestring if match fails
            pass
        return namestr

    # ...............................................
    @classmethod
    def _fix_type_new(cls, key, provided_val):
        """Correct parameter by 
            * casting to correct type 
            * if there are limited options, make sure it valid.
           If the parameter is invalid (type or value), return the default.
        """
        valid_options = None
        if provided_val is None:
            return None
        # all strings are lower case
        try:
            provided_val = provided_val.lower()
        except:
            pass
        
        default_val = BrokerParameters[key]['default']
        type_val = BrokerParameters[key]['type']
        # If restricted options, check
        try:
            options = BrokerParameters[key]['options']
        except:
            options = None
        else:
            # Invalid option returns default value
            if provided_val in options:
                usr_val = provided_val
            else:
                valid_options = options
                usr_val = default_val
            
        # Cast values to correct type. Failed conversions return default value
        if isinstance(type_val, str) and not options:
            usr_val = str(provided_val)

        elif isinstance(type_val, float):
            try:
                usr_val = float(provided_val)
            except:
                usr_val = default_val
                
        # Boolean also tests as int, so try boolean first
        elif isinstance(type_val, bool):                
            if provided_val in (0, '0', 'n', 'no', 'f', 'false'):
                usr_val = False
            elif provided_val in (1, '1', 'y', 'yes', 't', 'true'):
                usr_val = True
            else:
                valid_options = (True, False)
                usr_val = default_val                    

        elif isinstance(type_val, int):
            try:
                usr_val = int(provided_val)
            except:
                usr_val = default_val
                
        else:
            usr_val = provided_val
                
        return usr_val, valid_options
    
    # ...............................................
    @classmethod
    def _process_params(cls, user_kwargs={}):
        """
        Modify all user provided key/value pairs to change keys to lower case, 
        and change values to the expected type (string, int, float, boolean).
        
        Args:
            user_kwargs: dictionary of keywords and values sent by the user for 
                the current service.
                
        Note:
            A list of valid values for a keyword can include None as a default 
                if user-provided value is invalid
                
        Todo:
            Do we need not_in_valid_options for error message?
        """
        good_params = {}
        errinfo = {}
        
        # Correct all parameter keys/values present
        for key in cls.SERVICE_TYPE['params']:
            val = user_kwargs[key]
            # Done in calling function
            if key == 'provider':
                pass
                    
            # Do not edit namestr, maintain capitalization
            elif key == 'namestr':
                good_params['namestr'] = val
                
            # Require one valid icon_status
            elif key == 'icon_status':
                valid_stat = BrokerParameters[key]['options']
                if val is None:
                    errinfo = lmutil.add_errinfo(
                        errinfo, 'error', 
                        'Parameter {} containing one of {} options is required'.format(
                            key, valid_stat))
                elif val not in valid_stat:
                    errinfo = lmutil.add_errinfo(
                        errinfo, 'error',
                        'Value {} for parameter {} not in valid options {}'.format(
                             val, key, valid_stat))
                else:
                    good_params[key] = val
                    
            elif val is not None:
                # Allows None or comma-delimited list
                if key == 'scenariocode':
                    valid_scens = BrokerParameters[key]['options']
                    valid_requested_scens, invalid_scens = cls._get_valid_requested_params(val, valid_scens)
                    good_params[key] = valid_requested_scens
                    # But include message for invalid options
                    for badscen in invalid_scens:
                        # Add warning, not fatal, some valid providers may requested
                        errinfo = lmutil.add_errinfo(
                            errinfo, 'warning', 
                            'Ignoring invalid value {} for parameter scenariocode (valid options: {})'.format(
                                badscen, valid_scens))
                # All other parameters have single value
                else:
                    usr_val, valid_options = cls._fix_type_new(key, val)
                    if valid_options is not None and val not in valid_options:
                        errinfo = lmutil.add_errinfo(
                            errinfo, 'error',
                            'Value {} for parameter {} is not in valid options {}'.format(
                                val, key, BrokerParameters[key]['options']))
                        good_params[key] = None
                    else:
                        good_params[key] = usr_val
                
        # Fill in defaults for missing parameters
        for key in cls.SERVICE_TYPE['params']:
            param_meta = BrokerParameters[key]
            try:
                val = good_params[key]
            except:
                good_params[key] = param_meta['default']
            
        return good_params, errinfo

    # ...............................................
    @classmethod
    def _get_providers_from_string(cls, usr_req_providers, filter_params=None):
        providers = []
        errinfo = {}
        
        valid_providers = cls._get_valid_providers(filter_params=filter_params)
        # Allows None or comma-delimited list
        valid_requested_providers, invalid_providers = cls._get_valid_requested_params(
            usr_req_providers, valid_providers)

        # # TODO: Delete? Not used elsewhere
        # prov_vals['valid_providers'] = valid_providers
        # prov_vals['invalid_providers'] = invalid_providers

        if cls.SERVICE_TYPE != APIService.Badge:
            if valid_requested_providers:
                providers = valid_requested_providers
            else:
                providers = valid_providers
        else:
            if valid_requested_providers:
                providers = valid_requested_providers[0]
            else:
                providers = None
                errinfo = lmutil.add_errinfo(
                    errinfo, 'error',
                    'Parameter provider containing exactly one of {} options is required'.format(
                        valid_providers))
                
        if invalid_providers:
            for ip in invalid_providers:
                errinfo = lmutil.add_errinfo(
                    errinfo, 'warning',
                    'Value {} for parameter provider not in valid options {}'.format(
                        ip, valid_providers))
        
        return providers, errinfo

    # ...............................................
    @classmethod
    def _standardize_params(
            cls, provider=None, namestr=None, is_accepted=False, gbif_parse=False,  
            gbif_count=False, itis_match=False, kingdom=None, 
            occid=None, gbif_dataset_key=None, count_only=False, url=None,
            scenariocode=None, bbox=None, color=None, exceptions=None, height=None, 
            layers=None, request=None, frmat=None, srs=None, transparent=None, 
            width=None, do_match=True, icon_status=None, filter_params=None):
        """
        Return:
            a dictionary containing keys and properly formated values for the
                user specified parameters.
        Note: 
            filter_params is present to distinguish between providers for occ service by 
            occurrence_id or by dataset_id.
        """
        user_kwargs = {
            'provider': provider,
            'namestr': namestr,
            'is_accepted': is_accepted, 
            'gbif_parse': gbif_parse, 
            'gbif_count': gbif_count, 
            'itis_match': itis_match, 
            'kingdom': kingdom, 
            'occid': occid, 
            'gbif_dataset_key': gbif_dataset_key, 
            'count_only': count_only, 
            'url': url,
            'scenariocode': scenariocode,
            'bbox': bbox, 
            'color': color, 
            'exceptions': exceptions, 
            'height': height, 
            'layers': layers, 
            'request': request, 
            'format': frmat, 
            'srs': srs, 
            'transparent': transparent, 
            'width': width, 
            'icon_status': icon_status}
        
        providers, prov_errinfo = cls._get_providers_from_string(provider, filter_params=filter_params)
        usr_params, errinfo = cls._process_params(user_kwargs)
        # consolidate parameters and errors
        usr_params['provider'] = providers
        errinfo = lmutil.combine_errinfo(errinfo, prov_errinfo)

        # Remove gbif_parse and itis_match flags
        gbif_parse = itis_match = False
        try:
            gbif_parse = usr_params.pop('gbif_parse')
        except:
            pass
        try:
            itis_match = usr_params.pop('itis_match')
        except:
            pass
        # Replace namestr with GBIF-parsed namestr
        if namestr and (gbif_parse or itis_match):
            usr_params['namestr'] = cls.parse_name_with_gbif(namestr)
            
        return usr_params, errinfo

    # ..........................
    @staticmethod
    def OPTIONS():
        """Common options request for all services (needed for CORS)"""
        return

# .............................................................................
if __name__ == '__main__':
    kwarg_defaults = {
        'count_only': False,
        'width': 600,
        'height': 300,
        'type': [],
        }