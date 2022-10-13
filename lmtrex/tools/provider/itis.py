from collections import OrderedDict
from http import HTTPStatus
import urllib

from lmtrex.common.lmconstants import (
    ITIS, ServiceProvider, URL_ESCAPES, TST_VALUES)
from lmtrex.common.s2n_type import S2nEndpoint, S2nOutput, S2nSchema
from lmtrex.tools.fileop.logtools import log_info
from lmtrex.tools.provider.api import APIQuery
from lmtrex.tools.s2n.utils import get_traceback, add_errinfo


# .............................................................................
class ItisAPI(APIQuery):
    """Class to pull data from the ITIS Solr or Web service, documentation at:
        https://www.itis.gov/solr_documentation.html and 
        https://www.itis.gov/web_service.html
    """
    PROVIDER = ServiceProvider.ITISSolr
    NAME_MAP = S2nSchema.get_itis_name_map()
    
    # ...............................................
    def __init__(
            self, base_url, service=None, q_filters={}, other_filters={}, 
            logger=None):
        """Constructor for ItisAPI class.
        
        Args:
            base_url: Base URL for the ITIS Solr or Web service
            service: Indicator for which ITIS Web service to address
            q_filters:
            other_filters:
            
        Note:
            ITIS Solr service does not have nested services
        """
        if base_url == ITIS.SOLR_URL:
            other_filters['wt'] = 'json'
        if service is not None:
            base_url = '{}/{}'.format(base_url, service)
        APIQuery.__init__(
            self, base_url, q_filters=q_filters, other_filters=other_filters,
            logger=logger)

    # ...............................................
    def _assemble_filter_string(self, filter_string=None):
        # Assemble key/value pairs
        if filter_string is None:
            all_filters = self._other_filters.copy()
            if self._q_filters:
                q_val = self._assemble_q_val(self._q_filters)
                all_filters[self._q_key] = q_val

            if self.base_url == ITIS.SOLR_URL:
                kvpairs = []
                for k, val in all_filters.items():
                    if isinstance(val, bool):
                        val = str(val).lower()
                    # manual escaping for ITIS Solr
                    elif isinstance(val, str):
                        for oldstr, newstr in URL_ESCAPES:
                            val = val.replace(oldstr, newstr)
                    kvpairs.append('{}={}'.format(k, val))
                filter_string = '&'.join(kvpairs)
            else:
                for k, val in all_filters.items():
                    if isinstance(val, bool):
                        val = str(val).lower()
                # urlencode for ITIS web services
                filter_string = urllib.parse.urlencode(all_filters)
            
        # Escape filter string
        else:
            for oldstr, newstr in URL_ESCAPES:
                filter_string = filter_string.replace(oldstr, newstr)
        return filter_string  

    # ...............................................
    def _processRecordInfo(self, rec, header, reformat_keys=[]):
        row = []
        if rec is not None:
            for key in header:
                try:
                    val = rec[key]
                    
                    if type(val) is list:
                        if len(val) > 0:
                            val = val[0]
                        else:
                            val = ''
                            
                    if key in reformat_keys:
                        val = self._saveNLDelCR(val)
                        
                    elif key == 'citation':
                        if type(val) is dict:
                            try:
                                val = val['text']
                            except:
                                pass
                        
                    elif key in ('created', 'modified'):
                        val = self._clipDate(val)
                            
                except KeyError:
                    val = ''
                row.append(val)
        return row

# ...............................................
    @classmethod
    def _get_fld_value(cls, doc, fldname):
        try:
            val = doc[fldname]
        except:
            val = None
        return val

    # ...............................................
    @classmethod
    def _get_rank_from_path(cls, tax_path, rank_key):
        for rank, tsn, name in tax_path:
            if rank == rank_key:
                return (int(tsn), name)
        return (None, None)

    # ...............................................
    def _return_hierarchy(self):
        """
        Todo:
            Look at formatted strings, I don't know if this is working
        """
        tax_path = []
        for tax in self.output.iter(
                # '{{}}{}'.format(ITIS.DATA_NAMESPACE, ITIS.HIERARCHY_TAG)):
                '{}{}'.format(ITIS.DATA_NAMESPACE, ITIS.HIERARCHY_TAG)):
            rank = tax.find(
                # '{{}}{}'.format(ITIS.DATA_NAMESPACE, ITIS.RANK_TAG)).text
                '{}{}'.format(ITIS.DATA_NAMESPACE, ITIS.RANK_TAG)).text
            name = tax.find(
                # '{{}}{}'.format(ITIS.DATA_NAMESPACE, ITIS.TAXON_TAG)).text
                '{}{}'.format(ITIS.DATA_NAMESPACE, ITIS.TAXON_TAG)).text
            tsn = tax.find(
                # '{{}}{}'.format(ITIS.DATA_NAMESPACE, ITIS.TAXONOMY_KEY)).text
                '{}{}'.format(ITIS.DATA_NAMESPACE, ITIS.TSN_KEY)).text
            tax_path.append((rank, tsn, name))
        return tax_path

# # ...............................................
#     @classmethod
#     def _get_itis_solr_recs(cls, itis_output):
#         std_output = {}
#         errmsgs = []
#         try:
#             data = itis_output['response']
#         except:
#             errmsgs.append(cls._get_error_message(
#                 msg='Missing `response` element'))
#         else:
#             try:
#                 std_output[S2nKey.COUNT] = data['numFound']
#             except:
#                 errmsgs.append(cls._get_error_message(
#                     msg='Missing `count` element'))
#             try:
#                 std_output[S2nKey.RECORDS] = data['docs']
#             except:
#                 errmsgs.append(cls._get_error_message(
#                     msg='Missing `docs` element'))
#         if errmsgs:
#             std_output[S2nKey.ERRORS] = errmsgs
#         return std_output
            
    # ...............................................
    @classmethod
    def _parse_hierarchy_to_dicts(cls, val):
        hierarchy_lst = []
        if val:
            for hier_str in val:
                temp_hierarchy = {}
                hier_lst = hier_str.split('$')
                for elt in hier_lst:
                    parts = elt.split(':')
                    key = parts[0]
                    try:
                        name = parts[1]
                    except:
                        pass
                    else:
                        try:
                            rnk = key.lower()
                        except:
                            pass
                        else:
                            temp_hierarchy[rnk] = name
                # Reorder and filter to desired ranks
                hierarchy = OrderedDict()
                for rnk in S2nSchema.RANKS:
                    try:
                        hierarchy[rnk] = temp_hierarchy[rnk]
                    except:
                        hierarchy[rnk] = None
                hierarchy_lst.append(hierarchy)
        return hierarchy_lst

    # ...............................................
    @classmethod
    def _parse_synonyms_to_lists(cls, val):
        synonym_lst = []
        if val:
            for syn in val:
                syn_group = []
                lst = syn.split('$')
                for name in lst:
                    if name and name.find(':') < 0:
                        syn_group.append(name)
                synonym_lst.append(syn_group)
        return synonym_lst

    # ...............................................
    @classmethod
    def _standardize_record(cls, rec, is_accepted=False):
        newrec = {}
        view_std_fld = S2nSchema.get_view_url_fld()
        data_std_fld = S2nSchema.get_data_url_fld()
        hierarchy_prov_fld = 'hierarchySoFarWRanks'
        synonym_prov_fld = 'synonyms'
        good_statii = ('accepted', 'valid')
        
        status = rec['usage'].lower()
        if (not is_accepted or (is_accepted and status in good_statii)):
            for stdfld, provfld in cls.NAME_MAP.items():
                try:
                    val = rec[provfld]
                except:
                    val = None

                if provfld == ITIS.TSN_KEY:
                    newrec[stdfld] =  val
                    newrec[view_std_fld] = ITIS.get_taxon_view(val)
                    newrec[data_std_fld] = ITIS.get_taxon_data(val)

                elif provfld == hierarchy_prov_fld:
                    hierarchy_lst = cls._parse_hierarchy_to_dicts(val)                   
                    newrec[stdfld] =  hierarchy_lst
                
                elif provfld == synonym_prov_fld:
                    synonym_lst = cls._parse_synonyms_to_lists(val)
                    newrec[stdfld] =  synonym_lst                    
                
                else:
                    newrec[stdfld] =  val
        return newrec
    
    # ...............................................
    @classmethod
    def _standardize_output(
            cls, output, count_key, records_key, service,
            query_status=None, query_urls=[], is_accepted=False, errinfo={}):
        total = 0
        stdrecs = []

        try:
            total = output[count_key]
        except Exception as e:
            errinfo = add_errinfo(errinfo, 'error', cls._get_error_message(err=e))
        try:
            docs = output[records_key]
        except Exception as e:
            errinfo = add_errinfo(errinfo, 'error', cls._get_error_message(err=e))
        else:
            for doc in docs:
                newrec = cls._standardize_record(doc, is_accepted=is_accepted)
                if newrec:
                    stdrecs.append(newrec)
        prov_meta = cls._get_provider_response_elt(query_status=query_status, query_urls=query_urls)
        std_output = S2nOutput(
            total, service, provider=prov_meta, records=stdrecs, errors=errinfo)
        
        return std_output
    
# ...............................................
    @classmethod
    def match_name(cls, sciname, is_accepted=False, kingdom=None, logger=None):
        """Return an ITIS record for a scientific name using the 
        ITIS Solr service.
        
        Args:
            sciname: a scientific name designating a taxon
            status: optional designation for taxon status, 
                kingdom Plantae are valid/invalid, others are accepted 
            kingdom: optional designation for kingdom
            logger: optional logger for info and error messages.  If None, 
                prints to stdout    

        Return: 
            a dictionary containing one or more keys: 
                count, records, error, warning
            
        Example URL: 
            http://services.itis.gov/?q=nameWOInd:Spinus\%20tristis&wt=json
        """
        errinfo = {}
        q_filters = {ITIS.NAME_KEY: sciname}
        if kingdom is not None:
            q_filters['kingdom'] = kingdom
        api = ItisAPI(ITIS.SOLR_URL, q_filters=q_filters, logger=logger)

        try:
            api.query()
        except Exception as e:
            tb = get_traceback()
            std_output = cls.get_api_failure(
                S2nEndpoint.Name, HTTPStatus.INTERNAL_SERVER_ERROR,
                errors=[{'error': cls._get_error_message(err=tb)}])
        else:
            try:
                output = api.output['response']
            except:
                if api.error is not None:
                    errinfo['error'] = [cls._get_error_message(err=api.error)]
                    std_output = cls.get_api_failure(
                        S2nEndpoint.Name, HTTPStatus.INTERNAL_SERVER_ERROR, errinfo=errinfo)
                else:
                    errinfo['error'] = [cls._get_error_message(msg='Missing `response` element')]
                    std_output = cls.get_api_failure(
                        S2nEndpoint.Name, HTTPStatus.INTERNAL_SERVER_ERROR, errinfo=errinfo)
            else:
                errinfo = add_errinfo(errinfo, 'error', api.error)
                # Standardize output from provider response
                std_output = cls._standardize_output(
                    output, ITIS.COUNT_KEY, ITIS.RECORDS_KEY, S2nEndpoint.Name, 
                    query_status=api.status_code, query_urls=[api.url], is_accepted=is_accepted, 
                    errinfo=errinfo)
        return std_output

# ...............................................
    @classmethod
    def get_name_by_tsn(cls, tsn, logger=None):
        """Return a name and kingdom for an ITIS TSN using the ITIS Solr service.
        
        Args:
            tsn: a unique integer identifier for a taxonomic record in ITIS
            
        Note: not used or tested yet
        
        Ex: https://services.itis.gov/?q=tsn:566578&wt=json
        """
        output = {}
        errinfo = {}
        apiq = ItisAPI(
            ITIS.SOLR_URL, q_filters={ITIS.TSN_KEY: tsn}, logger=logger)
        try:
            apiq.query()
        except Exception as e:
            tb = get_traceback()
            errinfo = add_errinfo(errinfo, 'error', cls._get_error_message(err=tb))
            std_output = cls.get_api_failure(
                S2nEndpoint.Name, HTTPStatus.INTERNAL_SERVER_ERROR,
                errinfo=errinfo)
        else:
            errinfo = add_errinfo(errinfo, 'error', apiq.error)
            # Standardize output from provider response
            std_output = cls._standardize_output(
                output, ITIS.COUNT_KEY, ITIS.RECORDS_KEY, S2nEndpoint.Name, 
                apiq.status_code, query_urls=[apiq.url], is_accepted=True, errinfo=errinfo)

        return std_output


    # ...............................................
    def query(self):
        """Queries the API and sets 'output' attribute to a JSON object"""
        APIQuery.query_by_get(self, output_type='json')

# # ...............................................
#     @classmethod
#     def get_vernacular_by_tsn(cls, tsn, logger=None):
#         """Return vernacular names for an ITIS TSN.
#         
#         Args:
#             tsn: an ITIS code designating a taxonomic name
#         """
#         common_names = []
#         if tsn is not None:
#             url = '{}/{}?{}={}'.format(
#                 ITIS.WEBSVC_URL, ITIS.VERNACULAR_QUERY, ITIS.TSN_KEY, str(tsn))
#             root = self._getDataFromUrl(url, resp_type='xml')
#         
#             retElt = root.find('{}return'.format(ITIS.NAMESPACE))
#             if retElt is not None:
#                 cnEltLst = retElt.findall('{}commonNames'.format(ITIS.DATA_NAMESPACE))
#                 for cnElt in cnEltLst:
#                     nelt = cnElt.find('{}commonName'.format(ITIS.DATA_NAMESPACE))
#                     if nelt is not None and nelt.text is not None:
#                         common_names.append(nelt.text)
#         return common_names
# 
#     # ...............................................
#     @classmethod
#     def get_tsn_hierarchy(cls, tsn, logger=None):
#         """Retrieve taxon hierarchy"""
#         url = '{}/{}'.format(ITIS.WEBSVC_URL, ITIS.TAXONOMY_HIERARCHY_QUERY)
#         apiq = APIQuery(
#             url, other_filters={ITIS.TSN_KEY: tsn}, 
#             headers={'Content-Type': 'text/xml'}, logger=logger)
#         apiq.query_by_get(output_type='xml')
#         tax_path = apiq._return_hierarchy()
#         hierarchy = {}
#         for rank in (
#                 ITIS.KINGDOM_KEY, ITIS.PHYLUM_DIVISION_KEY, ITIS.CLASS_KEY,
#                 ITIS.ORDER_KEY, ITIS.FAMILY_KEY, ITIS.GENUS_KEY,
#                 ITIS.SPECIES_KEY):
#             hierarchy[rank] = apiq._get_rank_from_path(tax_path, rank)
#         return hierarchy

# # ...............................................
#     @classmethod
#     def match_name_nonsolr(cls, sciname, count_only=False, outformat='json', logger=None):
#         """Return matching names for scienfific name using the ITIS Web service.
#         
#         Args:
#             sciname: a scientific name
#             
#         Ex: https://services.itis.gov/?q=tsn:566578&wt=json
#         """
#         output = {}
#         errmsgs = []
#         if outformat == 'json':
#             url = ITIS.JSONSVC_URL
#         else:
#             url = ITIS.WEBSVC_URL
#             outformat = 'xml'
#         apiq = ItisAPI(
#             url, service=ITIS.ITISTERMS_FROM_SCINAME_QUERY, 
#             other_filters={ITIS.SEARCH_KEY: sciname}, logger=logger)
#         apiq.query_by_get(output_type=outformat)
#         
#         recs = []
#         if outformat == 'json':    
#             outjson = apiq.output
#             try:
#                 recs = outjson['itisTerms']
#             except:
#                 errmsgs.append(cls._get_error_message(
#                     msg='Missing `itisTerms` element'))
#         else:
#             root = apiq.output    
#             retElt = root.find('{}return'.format(ITIS.NAMESPACE))
#             if retElt is not None:
#                 termEltLst = retElt.findall('{}itisTerms'.format(ITIS.DATA_NAMESPACE))
#                 for tElt in termEltLst:
#                     rec = {}
#                     elts = tElt.getchildren()
#                     for e in elts:
#                         rec[e.tag] = e.text
#                     if rec:
#                         recs.append(rec)
#                         
#         output[S2nKey.COUNT] = len(recs)
#         if not count_only:
#             output[S2nKey.RECORDS] = recs
#             output[S2nKey.RECORD_FORMAT] = 'tbd'
#         output[S2nKey.ERRORS] = errmsgs
#         return output

# .............................................................................
if __name__ == '__main__':
    # test
    from lmtrex.tools.provider.gbif import GbifAPI
    
    names = TST_VALUES.NAMES[:5]
    canonicals = GbifAPI.parse_names(names=names)
    
    for name in names:
        itis_names = ItisAPI.match_name(name)
        log_info ('Matched {} with {} ITIS names using Solr'.format(
            name, len(itis_names)))
        for n in itis_names:
            log_info('{}: {}, {}, {}'.format(
                n['nameWOInd'], n['kingdom'], n['usage'], n['rank']))
        log_info ('')
 
"""
https://services.itis.gov/?wt=json&q=nameWInd:Plagioecia\%20patina
"""