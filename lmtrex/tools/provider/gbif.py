from collections import OrderedDict
from http import HTTPStatus
import os
import requests
import urllib

from lmtrex.common.issue_definitions import ISSUE_DEFINITIONS
from lmtrex.common.lmconstants import (
    APIService, GBIF, ServiceProvider, URL_ESCAPES, ENCODING)
from lmtrex.common.s2n_type import S2nEndpoint, S2nKey, S2nOutput, S2nSchema
from lmtrex.tools.fileop.logtools import (log_info, log_error)


from lmtrex.tools.provider.api import APIQuery
from lmtrex.tools.s2n.utils  import get_traceback, add_errinfo

# .............................................................................
class GbifAPI(APIQuery):
    """Class to query GBIF APIs and return results"""
    PROVIDER = ServiceProvider.GBIF
    OCCURRENCE_MAP = S2nSchema.get_gbif_occurrence_map()
    NAME_MAP = S2nSchema.get_gbif_name_map()
    
    # ...............................................
    def __init__(self, service=GBIF.SPECIES_SERVICE, key=None,
                 other_filters=None, logger=None):
        """
        Constructor for GbifAPI class
        
        Args:
            service: GBIF service to query
            key: unique identifier for an object of this service
            other_filters: optional filters
            logger: optional logger for info and error messages.  If None, 
                prints to stdout
        """
        url = '/'.join((GBIF.REST_URL, service))
        if key is not None:
            url = '/'.join((url, str(key)))
        APIQuery.__init__(self, url, other_filters=other_filters, logger=logger)

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
    def _get_output_val(cls, out_dict, name):
        try:
            tmp = out_dict[name]
            val = str(tmp).encode(ENCODING)
        except Exception:
            return None
        return val

    # # ...............................................
    # @classmethod
    # def get_taxonomy(cls, taxon_key, logger=None):
    #     """Return GBIF backbone taxonomy for this GBIF Taxon ID
    #     """
    #     std_output = {S2nKey.COUNT: 0}
    #     errmsgs = []
    #     std_recs = []
    #     rec = {}
    #     tax_api = GbifAPI(
    #         service=GBIF.SPECIES_SERVICE, key=taxon_key, logger=logger)
    #     try:
    #         tax_api.query()
    #     except Exception as e:
    #         traceback = lmutil.get_traceback()
    #         errmsgs.append({'error': traceback})
    #     else:
    #         output = tax_api.output
    #         elements_of_interest = [
    #             'scientificName', 'kingdom', 'phylum', 'class', 'order', 
    #             'family', 'genus', 'species', 'rank', 'genusKey', 'speciesKey', 
    #             'taxonomicStatus', 'canonicalName', 'scientificName', 'kingdom', 
    #             'phylum', 'class', 'order', 'family', 'genus', 'species', 
    #             'rank', 'genusKey', 'speciesKey', 'taxonomicStatus', 
    #             'canonicalName', 'acceptedKey', 'accepted', 'nubKey']
    #         for fld in elements_of_interest:
    #             rec[fld] = tax_api._get_output_val(output, fld)
    #         std_recs.append(rec)
    #
    #     std_output[S2nKey.RECORDS] = std_recs
    #     std_output[S2nKey.ERRORS] = errmsgs
    #     return std_output

    # ...............................................
    @classmethod
    def get_occurrences_by_occid(cls, occid, count_only=False, logger=None):
        """Return GBIF occurrences for this occurrenceId.  This should retrieve 
        a single record if the occurrenceId is unique.
        
        Args:
            occid: occurrenceID for query
            count_only: boolean flag signaling to return records or only count
            logger: optional logger for info and error messages.  If None, 
                prints to stdout    

        Return: 
            a dictionary containing one or more keys: 
                count, records, error, warning
                
        Todo: enable paging
        """
        errinfo = {}
        api = GbifAPI(
            service=GBIF.OCCURRENCE_SERVICE, key=GBIF.SEARCH_COMMAND,
            other_filters={'occurrenceID': occid}, logger=logger)
        try:
            api.query()
        except Exception as e:
            tb = get_traceback()
            errinfo['error'] = [cls._get_error_message(err=tb)]
            std_output = cls.get_api_failure(
                S2nEndpoint.Occurrence, HTTPStatus.INTERNAL_SERVER_ERROR, errinfo=errinfo)
        else:
            if api.error:
                errinfo['error'] =  [api.error]
                
            # Standardize output from provider response
            std_output = cls._standardize_occurrence_output(
                api.output, api.status_code, query_urls=[api.url], 
                count_only=count_only, errinfo=errinfo)
        
        return std_output

    # ...............................................
    @classmethod
    def _get_fld_vals(cls, big_rec):
        rec = {}
        for fld_name in GbifAPI.NameMatchFieldnames:
            try:
                rec[fld_name] = big_rec[fld_name]
            except KeyError:
                pass
        return rec

    # ...............................................
    @classmethod
    def _standardize_occurrence_record(cls, rec):
        newrec = {}
        parse_prov_fields = ['associatedSequences', 'associatedReferences']
        to_str_prov_fields = ['year', 'month', 'day', 'decimalLongitude', 'decimalLatitude']
        view_std_fld = S2nSchema.get_view_url_fld()
        data_std_fld = S2nSchema.get_data_url_fld()
        issue_prov_fld = 'issues'        
        
        for stdfld, provfld in cls.OCCURRENCE_MAP.items():
            try:
                val = rec[provfld]
            except:
                val = None

            # Save ID field, plus use to construct URLs
            if provfld == GBIF.OCC_ID_FIELD:
                newrec[stdfld] =  val
                newrec[view_std_fld] = GBIF.get_occurrence_view(val)
                newrec[data_std_fld] = GBIF.get_occurrence_data(val)
                
            # expand fields to dictionary, with code and definition
            elif provfld == issue_prov_fld:
                newrec[stdfld] = cls._get_code2description_dict(
                    val, ISSUE_DEFINITIONS[ServiceProvider.GBIF[S2nKey.PARAM]])
                
            # Modify/parse into list
            elif val and provfld in parse_prov_fields:
                lst = val.split('|')
                elts = [l.strip() for l in lst]
                newrec[stdfld] = elts
                
            # Modify int date elements to string (to match iDigBio)
            elif val and provfld in to_str_prov_fields:
                newrec[stdfld] = str(val)
                
            # all others
            else:
                newrec[stdfld] =  val
        return newrec
    
    # ...............................................
    @classmethod
    def _standardize_name_record(cls, rec):
        newrec = {}
        view_std_fld = S2nSchema.get_view_url_fld()
        data_std_fld = S2nSchema.get_data_url_fld()
        hierarchy_fld = 'hierarchy'
        
        for stdfld, provfld in cls.NAME_MAP.items():
            try:
                val = rec[provfld]
            except:
                val = None
            # Also use ID field to construct URLs
            if provfld == GBIF.SPECIES_ID_FIELD:
                newrec[stdfld] =  val
                newrec[view_std_fld] = GBIF.get_species_view(val)
                newrec[data_std_fld] = GBIF.get_species_data(val)
                
            # Assemble from other fields
            elif provfld == hierarchy_fld:
                hierarchy = OrderedDict()
                for rnk in S2nSchema.RANKS:
                    try:
                        val = rec[rnk]
                    except:
                        pass
                    else:
                        hierarchy[rnk] = val
                newrec[stdfld] = [hierarchy]
                
            # all others
            else:
                newrec[stdfld] = val
        return newrec
    
    # ...............................................
    @classmethod
    def _test_record(cls, status, rec):
        is_good = False
        # No filter by status, take original
        if status is None:
            is_good = True
        else:
            outstatus = None
            try:
                outstatus = rec['status'].lower()
            except AttributeError:
                print(cls._get_error_message(msg='No status in record'))
            else:
                if outstatus == status:
                    is_good = True
        return is_good
        
    # ...............................................
    @classmethod
    def _standardize_match_output(
            cls, output, record_status, query_status, query_urls=[], errinfo={}):
        stdrecs = []
        try:
            alternatives = output.pop('alternatives')
        except:
            alternatives = []
            
        is_match = True
        try:
            if output['matchType'].lower() == 'none':
                is_match = False
        except AttributeError:
            msg = cls._get_error_message(msg='No matchType')
            errinfo['error'].append(msg)
        else:
            goodrecs = []
            # take primary output if matched
            if is_match:
                if cls._test_record(record_status, output):
                    goodrecs.append(output)
            for alt in alternatives:
                if cls._test_record(record_status, alt):
                    goodrecs.append(alt)
            # Standardize name output
            for r in goodrecs:
                stdrecs.append(cls._standardize_name_record(r))
        total = len(stdrecs)
        prov_meta = cls._get_provider_response_elt(query_status=query_status, query_urls=query_urls)
        # TODO: standardize_record and provide schema link
        std_output = S2nOutput(
            total, S2nEndpoint.Name, provider=prov_meta, records=stdrecs, errors=errinfo)
        return std_output
        
    # ...............................................
    @classmethod
    def _standardize_record(cls, rec, record_format):
        # todo: standardize gbif output to DWC, DSO, etc
        if record_format == GBIF.RECORD_FORMAT_OCCURRENCE:
            stdrec = cls._standardize_occurrence_record(rec)
        else:
            stdrec = cls._standardize_name_record(rec)
        return stdrec
    
    # ...............................................
    @classmethod
    def _standardize_occurrence_output(
            cls, output, query_status, query_urls=[], count_only=False, errinfo={}):
        # GBIF.COUNT_KEY, GBIF.RECORDS_KEY, GBIF.RECORD_FORMAT_OCCURRENCE, 
        stdrecs = []
        total = 0
        # Count
        try:
            total = output[GBIF.COUNT_KEY]
        except:
            msg = cls._get_error_message(msg='Missing `{}` element'.format(GBIF.COUNT_KEY))
            errinfo['error'].append(msg)
        # Records
        if not count_only:
            try:
                recs = output[GBIF.RECORDS_KEY]
            except:
                msg = cls._get_error_message(msg='Missing `{}` element'.format(GBIF.RECORDS_KEY))
                errinfo['error'].append(msg)
            else:
                stdrecs = []
                for r in recs:
                    try:
                        stdrecs.append(
                            cls._standardize_record(r, GBIF.RECORD_FORMAT_OCCURRENCE))
                    except Exception as e:
                        msg = cls._get_error_message(err=e)
                        errinfo['error'].append(msg)
        prov_meta = cls._get_provider_response_elt(query_status=query_status, query_urls=query_urls)
        std_output = S2nOutput(
            total, S2nEndpoint.Occurrence, provider=prov_meta, records=stdrecs, errors=errinfo)

        return std_output
    
    # ...............................................
    @classmethod
    def get_occurrences_by_dataset(
            cls, gbif_dataset_key, count_only, logger=None):
        """
        Count and optionally return (a limited number of) records with the given 
        gbif_dataset_key.  This currently only returns the first page (0-limit) of records.
        
        Args:
            gbif_dataset_key: unique identifier for the dataset, assigned by GBIF
                and retained by Specify
            count_only: boolean flag signaling to return records or only count
            logger: optional logger for info and error messages.  If None, 
                prints to stdout    

        Return: 
            a dictionary containing one or more keys: 
                count, records, error, warning
        
        Todo: 
            handle large queries asynchronously
        """
        errinfo = {}
        if count_only is True:
            limit = 1
        else:
            limit = GBIF.LIMIT   
        api = GbifAPI(
            service=GBIF.OCCURRENCE_SERVICE, key=GBIF.SEARCH_COMMAND,
            other_filters={
                GBIF.REQUEST_DATASET_KEY: gbif_dataset_key, 'offset': 0, 
                'limit': limit}, logger=logger)
        try:
            api.query()
        except Exception as e:
            tb = get_traceback()
            std_out = cls.get_api_failure(
                S2nEndpoint.Occurrence, HTTPStatus.INTERNAL_SERVER_ERROR,
                errinfo={'error': [cls._get_error_message(err=tb)]})
        else:
            # Standardize output from provider response
            if api.error:
                errinfo['error'] =  [api.error]
                
            std_out = cls._standardize_occurrence_output(
                api.output, api.status_code, query_urls=[api.url], count_only=count_only, errinfo=errinfo)
            
        return std_out


    # ...............................................
    @classmethod
    def match_name(cls, namestr, is_accepted=False, logger=None):
        """Return closest accepted species in GBIF backbone taxonomy,
        
        Args:
            namestr: A scientific namestring possibly including author, year, 
                rank marker or other name information.
            is_accepted: match the ACCEPTED TaxonomicStatus in the GBIF record
                
        Returns:
            Either a dictionary containing a matching record with status 
                'accepted' or 'synonym' without 'alternatives'.  
            Or, if there is no matching record, return the first/best 
                'alternative' record with status 'accepted' or 'synonym'.

        Note:
            This function uses the name search API, 
        Note:
            GBIF TaxonomicStatus enum at:
            https://gbif.github.io/gbif-api/apidocs/org/gbif/api/vocabulary/TaxonomicStatus.html

        """
        status = None
        errinfo = {}
        if is_accepted:
            status = 'accepted'
        name_clean = namestr.strip()
        other_filters = {'name': name_clean, 'verbose': 'true'}
#         if rank:
#             other_filters['rank'] = rank
#         if kingdom:
#             other_filters['kingdom'] = kingdom
        api = GbifAPI(
            service=GBIF.SPECIES_SERVICE, key='match',
            other_filters=other_filters, logger=logger)
        
        try:
            api.query()
        except Exception as e:
            tb = get_traceback()
            errinfo['error'] =  [cls._get_error_message(err=tb)]
            std_output = cls.get_api_failure(
                S2nEndpoint.Name, HTTPStatus.INTERNAL_SERVER_ERROR, errinfo=errinfo)
        else:
            if api.error:
                errinfo['error'] =  [api.error]
            # Standardize output from provider response
            std_output = cls._standardize_match_output(
                api.output, status, api.status_code, query_urls=[api.url], errinfo=errinfo)
            
        return std_output

    # ...............................................
    @classmethod
    def count_occurrences_for_taxon(cls, taxon_key, logger=None):
        """Return a count of occurrence records in GBIF with the indicated taxon.
                
        Args:
            taxon_key: A GBIF unique identifier indicating a taxon object.
               out 
        Returns:
            A record as a dictionary containing the record count of occurrences
            with this accepted taxon, and a URL to retrieve these records.            
        """
        simple_output = {}
        errinfo = {}
        total = 0
        # Query GBIF
        api = GbifAPI(
            service=GBIF.OCCURRENCE_SERVICE, key=GBIF.SEARCH_COMMAND,
            other_filters={'taxonKey': taxon_key}, logger=logger)
        
        try:
            api.query_by_get()
        except Exception as e:
            msg = cls._get_error_message(err=e)
            errinfo = add_errinfo(errinfo, 'error', msg)
        else:
            try:
                total = api.output['count']
            except Exception as e:
                msg = cls._get_error_message(msg='Missing `count` element')
                errinfo = add_errinfo(errinfo, 'error', msg)
            else:
                if total < 1:
                    msg = cls._get_error_message(msg='No match')
                    errinfo = add_errinfo(errinfo, 'info', msg)
                    simple_output[S2nKey.OCCURRENCE_URL] = None
                else:
                    simple_output[S2nKey.OCCURRENCE_URL] = api.url
        prov_meta = cls._get_provider_response_elt(query_status=api.status_code, query_urls=[api.url])
        std_output = S2nOutput(
            total, S2nEndpoint.Occurrence, provider=prov_meta, errors=errinfo)
        return std_output

    # ......................................
    @classmethod
    def _post_json_to_parser(cls, url, data, logger=None):
        response = output = None
        try:
            response = requests.post(url, json=data)
        except Exception as e:
            if response is not None:
                ret_code = response.status_code
            else:
                log_error('Failed on URL {} ({})'.format(url, str(e)), 
                          logger=logger)
        else:
            if response.ok:
                try:
                    output = response.json()
                except Exception as e:
                    try:
                        output = response.content
                    except Exception:
                        output = response.text
                    else:
                        log_error(
                            'Failed to interpret output of URL {} ({})'.format(
                                url, str(e)), logger=logger)
            else:
                try:
                    ret_code = response.status_code
                    reason = response.reason
                except AttributeError:
                    log_error(
                        'Failed to find failure reason for URL {} ({})'.format(
                            url, str(e)), logger=logger)
                else:
                    log_error(
                        'Failed on URL {} ({}: {})'.format(url, ret_code, reason), 
                        logger=logger)
        return output
    
    
# ...............................................
    @classmethod
    def _trim_parsed_output(cls, output, logger=None):
        recs = []
        for rec in output:
            # Only return parsed records
            try:
                success = rec['parsed']
            except:
                log_error('Missing `parsed` field in record', logger=logger)
            else:
                if success:
                    recs.append(rec)
        return recs

# ...............................................
    @classmethod
    def parse_name(cls, namestr, logger=None):
        """
        Send a scientific name to the GBIF Parser returning a canonical name.
        
        Args:
            namestr: A scientific namestring possibly including author, year, 
                rank marker or other name information.
                
        Returns:
            A dictionary containing a single record for a parsed scientific name
            and any optional error messages.
            
        sent (bad) http://api.gbif.org/v1/parser/name?name=Acer%5C%2520caesium%5C%2520Wall.%5C%2520ex%5C%2520Brandis
        send good http://api.gbif.org/v1/parser/name?name=Acer%20heldreichii%20Orph.%20ex%20Boiss.
        """
        output = {}
        # Query GBIF
        name_api = GbifAPI(
            service=GBIF.PARSER_SERVICE, 
            other_filters={GBIF.REQUEST_NAME_QUERY_KEY: namestr},
            logger=logger)
        name_api.query_by_get()
        # Parse results (should be only one)
        if name_api.output is not None:
            recs = name_api._trim_parsed_output(name_api.output)
            try:
                output['record'] = recs[0]
            except:
                msg = 'Failed to return results from {}, ({})'.format(
                    name_api.url, cls.__class__.__name__)
                log_error(msg, logger=logger)
                output[S2nKey.ERRORS] = msg
        return output, name_api.url

    # ...............................................
    @classmethod
    def parse_names(cls, names=[], filename=None, logger=None):
        """
        Send a list or file (or both) of scientific names to the GBIF Parser,
        returning a dictionary of results.  Each scientific name can possibly 
        include author, year, rank marker or other name information.
        
        Args:
            names: a list of names to be parsed
            filename: a file of names to be parsed
            
        Returns:
            A list of resolved records, each is a dictionary with keys of 
            GBIF fieldnames and values with field values. 
        """
        if filename and os.path.exists(filename):
            with open(filename, 'r', encoding=ENCODING) as in_file:
                for line in in_file:
                    names.append(line.strip())

        url = '{}/{}'.format(GBIF.REST_URL, GBIF.PARSER_SERVICE)
        try:
            output = GbifAPI._post_json_to_parser(url, names, logger=logger)
        except Exception as e:
            log_error(
                'Failed to get response from GBIF for data {}, {}'.format(
                    filename, e), logger=logger)
            raise e

        if output:
            recs = GbifAPI._trim_parsed_output(output, logger=logger)
            if filename is not None:
                log_info(
                    'Wrote {} parsed records from GBIF to file {}'.format(
                        len(recs), filename), logger=logger)
            else:
                log_info(
                    'Found {} parsed records from GBIF for {} names'.format(
                        len(recs), len(names)), logger=logger)

        return recs

    # ...............................................
    @classmethod
    def get_publishing_org(cls, pub_org_key, logger=None):
        """Return title from one organization record with this key

        Args:
            pub_org_key: GBIF identifier for this publishing organization
        """
        org_api = GbifAPI(
            service=GBIF.ORGANIZATION_SERVICE, key=pub_org_key, logger=logger)
        try:
            org_api.query()
            pub_org_name = org_api._get_output_val(org_api.output, 'title')
        except Exception as e:
            log_error(str(e), logger=logger)
            raise
        return pub_org_name

    # ...............................................
    def query(self):
        """ Queries the API and sets 'output' attribute to a ElementTree object
        """
        APIQuery.query_by_get(self, output_type='json')





# .............................................................................
if __name__ == '__main__':
    # test
    pass