"""Module containing functions for API Queries"""
from http import HTTPStatus
import requests
import typing
import urllib

from lmtrex.common.lmconstants import (URL_ESCAPES, ENCODING)
from lmtrex.common.s2n_type import S2nKey, S2nOutput
from lmtrex.tools.fileop.logtools import (log_warn)
from lmtrex.tools.misc.lm_xml import fromstring, deserialize
from lmtrex.tools.s2n.utils import add_errinfo, combine_errinfo, get_traceback

import lmtrex.tools.s2n.utils as lmutil
# .............................................................................
class APIQuery:
    """Class to query APIs and return results.

    Note:
        CSV files are created with tab delimiter
    """
    # Not implemented in base class
    PROVIDER = None

    def __init__(self, base_url, q_key='q', q_filters=None,
                 other_filters=None, filter_string=None, headers=None, 
                 logger=None):
        """
        @summary Constructor for the APIQuery class
        """
        self.logger = logger
        self._q_key = q_key
        self.headers = {} if headers is None else headers
        # No added filters are on url (unless initialized with filters in url)
        self.base_url = base_url
        self._q_filters = {} if q_filters is None else q_filters
        self._other_filters = {} if other_filters is None else other_filters
        self.filter_string = self._assemble_filter_string(
            filter_string=filter_string)
        self.output = None
        self.error = None
        self.debug = False

    # ...............................................
    @classmethod
    def _standardize_record(cls, rec):
        """
        Standardize record to common schema. 
        
        Note: 
            implemented in subclasses
        """
        raise Exception('Not implemented in base class')

    # ...............................................
    @classmethod
    def _standardize_output(
            cls, output, count_key, records_key, record_format, service, query_status=None, 
            query_urls=[], count_only=False, errinfo={}):
        stdrecs = []
        total = 0
        # Count
        try:
            total = output[count_key]
        except:
            msg = cls._get_error_message(msg='Missing `{}` element'.format(count_key))
            try:
                errinfo['warning'].append(msg)
            except:
                errinfo['warning'] = [msg]
        # Records
        if not count_only:
            try:
                recs = output[records_key]
            except:
                msg = cls._get_error_message(msg='Output missing `{}` element'.format(records_key))
                errinfo = add_errinfo(errinfo, 'warning', msg)
            else:
                for r in recs:
                    try:
                        stdrecs.append(cls._standardize_record(r))
                    except Exception as e:
                        msg = cls._get_error_message(err=e)
                        errinfo = add_errinfo(errinfo, 'error', msg)
                            
        prov_meta = cls._get_provider_response_elt(query_status=query_status, query_urls=query_urls)
        std_output = S2nOutput(
            total, service, provider=prov_meta, record_format=record_format, 
            records=stdrecs, errors=errinfo)

        return std_output

    # .....................................
    @classmethod
    def _get_error_message(cls, msg=None, err=None):
        text = cls.__name__
        if msg is not None:
            text = '{}; {}'.format(text, msg)
        if err is not None:
            text = '{}; (exception: {})'.format(text, err)
        return text

    # ...............................................
    @classmethod
    def _get_code2description_dict(cls, code_lst, code_map):
        # May contain 'issues'
        code_dict = {}
        if code_lst:
            for tmp in code_lst:
                code = tmp.strip()
                try:
                    code_dict[code] = code_map[code]
                except:
                    code_dict[code] = code
        return code_dict

    # ...............................................
    @classmethod
    def _get_provider_response_elt(cls, query_status=None, query_urls=[]):
        provider_element = {}
        provcode = cls.PROVIDER[S2nKey.PARAM]
        provider_element[S2nKey.PROVIDER_CODE] = provcode
        provider_element[S2nKey.PROVIDER_LABEL] = cls.PROVIDER[S2nKey.NAME]
        icon_url = lmutil.get_icon_url(provcode)
        if icon_url:
            provider_element[S2nKey.PROVIDER_ICON_URL] = icon_url
        # Optional http status_code
        try:
            stat = int(query_status)
        except: 
            try:
                stat = max(query_status)
            except:
                stat = None
        if stat:
            provider_element[S2nKey.PROVIDER_STATUS_CODE] = stat
        # Optional URL queries
        if query_urls:
            provider_element[S2nKey.PROVIDER_QUERY_URL] = query_urls
        return provider_element

    
    # .....................................
    @classmethod
    def init_from_url(cls, url, headers=None, logger=None):
        """Initialize APIQuery from a url

        Args:
            url (str): The url to use as the base
            headers (dict): Headers to use for query
        """
        if headers is None:
            headers = {}
        base, filters = url.split('?')
        qry = APIQuery(
            base, filter_string=filters, headers=headers, logger=logger)
        return qry

    # .........................................
    @property
    def url(self):
        """Retrieve a url for the query"""
        # All filters added to url
        if self.filter_string and len(self.filter_string) > 1:
            return '{}?{}'.format(self.base_url, self.filter_string)

        return self.base_url

    # ...............................................
    def add_filters(self, q_filters=None, other_filters=None):
        """Add or replace filters.

        Note:
            This does not remove existing filters unless they are replaced
        """
        self.output = None
        q_filters = {} if q_filters is None else q_filters
        other_filters = {} if other_filters is None else other_filters

        for k, val in q_filters.items():
            self._q_filters[k] = val
        for k, val in other_filters.items():
            self._other_filters[k] = val
        self.filter_string = self._assemble_filter_string()

    # ...............................................
    def clear_all(self, q_filters=True, other_filters=True):
        """Clear existing q_filters, other_filters, and output
        """
        self.output = None
        if q_filters:
            self._q_filters = {}
        if other_filters:
            self._other_filters = {}
        self.filter_string = self._assemble_filter_string()

    # ...............................................
    def clear_other_filters(self):
        """Clear existing otherFilters and output
        """
        self.clear_all(other_filters=True, q_filters=False)

    # ...............................................
    def clear_q_filters(self):
        """Clear existing qFilters and output
        """
        self.clear_all(other_filters=False, q_filters=True)

    # ...............................................
    def _burrow(self, key_list):
        this_dict = self.output
        if isinstance(this_dict, dict):
            for key in key_list:
                try:
                    this_dict = this_dict[key]
                except KeyError:
                    raise Exception('Missing key {} in output'.format(key))
        else:
            raise Exception('Invalid output type ({})'.format(type(this_dict)))
        return this_dict


    # ...............................................
    def _assemble_filter_string(self, filter_string=None):
        # Assemble key/value pairs
        if filter_string is None:
            all_filters = self._other_filters.copy()
            if self._q_filters:
                q_val = self._assemble_q_val(self._q_filters)
                all_filters[self._q_key] = q_val
            for k, val in all_filters.items():
                if isinstance(val, bool):
                    val = str(val).lower()
#                 elif isinstance(val, str):
#                     for oldstr, newstr in URL_ESCAPES:
#                         val = val.replace(oldstr, newstr)
                # works for GBIF, iDigBio, ITIS web services (no manual escaping)
                all_filters[k] = str(val).encode(ENCODING)
            filter_string = urllib.parse.urlencode(all_filters)
        # Escape filter string
        else:
            for oldstr, newstr in URL_ESCAPES:
                filter_string = filter_string.replace(oldstr, newstr)

        return filter_string

    # ...............................................
    @classmethod
    def _interpret_q_clause(cls, key, val, logger=None):
        clause = None
        if isinstance(val, (float, int, str)):
            clause = '{}:{}'.format(key, str(val))
        # Tuple for negated or range value
        elif isinstance(val, tuple):
            # negated filter
            if isinstance(val[0], bool) and val[0] is False:
                clause = 'NOT ' + key + ':' + str(val[1])
            # range filter (better be numbers)
            elif isinstance(
                    val[0], (float, int)) and isinstance(val[1], (float, int)):
                clause = '{}:[{} TO {}]'.format(key, str(val[0]), str(val[1]))
            else:
                log_warn('Unexpected value type {}'.format(val), logger=logger)
        else:
            log_warn('Unexpected value type {}'.format(val), logger=logger)
        return clause

    # ...............................................
    def _assemble_q_item(self, key, val):
        itm_clauses = []
        # List for multiple values of same key
        if isinstance(val, list):
            for list_val in val:
                itm_clauses.append(self._interpret_q_clause(key, list_val))
        else:
            itm_clauses.append(self._interpret_q_clause(key, val))
        return itm_clauses

    # ...............................................
    def _assemble_q_val(self, q_dict):
        clauses = []
        q_val = ''
        # interpret dictionary
        for key, val in q_dict.items():
            clauses.extend(self._assemble_q_item(key, val))
        # convert to string
        first_clause = ''
        for cls in clauses:
            if not first_clause and not cls.startswith('NOT'):
                first_clause = cls
            elif cls.startswith('NOT'):
                q_val = ' '.join((q_val, cls))
            else:
                q_val = ' AND '.join((q_val, cls))
        q_val = first_clause + q_val
        return q_val

    # ...............................................
    @classmethod
    def get_api_failure(
            cls, service, provider_response_status, errinfo={}):
        """Output format for all (soon) API queries
        
        Args:
            provider_response_status: HTTPStatus of provider query
            errors: list of info messages, warnings, errors (dictionaries)
            service: type of S^n services
            
        Return:
            lmtrex.services.api.v1.S2nOutput object
        """
        prov_meta = cls._get_provider_response_elt(query_status=provider_response_status)
        return S2nOutput(
            0, service, provider=prov_meta, errors=errinfo)

    # ...............................................
    def query_by_get(self, output_type='json', verify=True):
        """
        Queries the API and sets 'output' attribute to a JSON or ElementTree 
        object and S2nKey.ERRORS attribute to a string if appropriate.
        
        Note:
            Sets a single error message, not a list, to error attribute
        """
        self.output = {}
        self.error = None
        self.status_code = None
        self.reason = None
        errmsg = None
        try:
            if verify:
                response = requests.get(self.url, headers=self.headers)
            else:
                response = requests.get(self.url, headers=self.headers, verify=False)
        except Exception as e:
            errmsg = self._get_error_message(err=e)
        else:
            # Save server status
            try:
                self.status_code = response.status_code
                self.reason = response.reason
            except Exception:
                self.status_code = HTTPStatus.INTERNAL_SERVER_ERROR
                self.reason = 'Unknown API status_code/reason'
            # Parse response
            if response.status_code == HTTPStatus.OK:
                if output_type == 'json':
                    try:
                        self.output = response.json()
                    except Exception as e:
                        output = response.content
                        if output.find(b'<html') != -1:
                            self.status_code = HTTPStatus.INTERNAL_SERVER_ERROR
                            errmsg = self._get_error_message(
                                msg='Provider error', 
                                err='Invalid JSON response ({})'.format(output))
                        else:
                            try:
                                self.output = deserialize(fromstring(output))
                            except:
                                self.status_code = HTTPStatus.INTERNAL_SERVER_ERROR
                                errmsg = self._get_error_message(
                                    msg='Provider error', err='Unrecognized output {}'.format(
                                        output))
                elif output_type == 'xml':
                    try:
                        output = fromstring(response.text)
                        self.output = output
                    except Exception as e:
                        self.output = response.text
                else:
                    self.status_code = HTTPStatus.INTERNAL_SERVER_ERROR
                    errmsg = self._get_error_message(
                        msg='Unrecognized output type {}'.format(output_type))
            else:
                errmsg = self._get_error_message(
                    msg='URL {}, code = {}, reason = {}'.format(
                        self.base_url, self.status_code, self.reason))

        if errmsg:
            self.error = errmsg

    # ...........    ....................................
    def query_by_post(self, output_type='json', file=None):
        """Perform a POST request."""
        self.output = None
        self.error = None
        errmsg = None
        # Post a file
        if file is not None:
            # TODO: send as bytes here?
            files = {'files': open(file, 'rb')}
            try:
                response = requests.post(self.base_url, files=files)
            except Exception as e:
                try:
                    ret_code = response.status_code
                    reason = response.reason
                except Exception:
                    ret_code = HTTPStatus.INTERNAL_SERVER_ERROR
                    reason = 'Unknown Error'
                errmsg = self._get_error_message(
                    msg='file {}, code = {}, reason = {}'.format(
                        file, ret_code, reason),
                    err=e)
        # Post parameters
        else:
            all_params = self._other_filters.copy()
            if self._q_filters:
                all_params[self._q_key] = self._q_filters
            query_as_string = urllib.parse.urlencode(all_params)
            url = self.base_url + '/?' + query_as_string
            try:
                response = requests.post(url, headers=self.headers)
            except Exception as e:
                try:
                    ret_code = response.status_code
                    reason = response.reason
                except Exception:
                    ret_code = HTTPStatus.INTERNAL_SERVER_ERROR
                    reason = 'Unknown Error'
                errmsg = self._get_error_message(
                    msg='code = {}, reason = {}'.format(ret_code, reason), 
                    err=e)
        # Save server status
        try:
            self.status_code = response.status_code
            self.reason = response.reason
        except Exception:
            self.status_code = HTTPStatus.INTERNAL_SERVER_ERROR
            self.reason = 'Unknown API status_code/reason'
        # Parse response
        if response.ok:
            try:
                if output_type == 'json':
                    try:
                        self.output = response.json()
                    except Exception as e:
                        output = response.content
                        self.output = deserialize(fromstring(output))
                elif output_type == 'xml':
                    output = response.text
                    self.output = deserialize(fromstring(output))
                else:
                    errmsg = 'Unrecognized output type {}'.format(output_type)
            except Exception as e:
                errmsg = self._get_error_message(
                    msg='Unrecognized output, URL {}, content={}'.format(
                        self.base_url, response.content),
                    err=e)
        else:
            errmsg = self._get_error_message(
                msg='URL {}, code = {}, reason = {}'.format(
                    self.base_url, self.status_code, self.reason))
            
        if errmsg is not None:
            self.error = errmsg
