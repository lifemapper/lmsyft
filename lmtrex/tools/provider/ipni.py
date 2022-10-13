from collections import OrderedDict
from http import HTTPStatus
import pykew.ipni as ipni
from pykew.ipni_terms import Name
import urllib

from lmtrex.common.lmconstants import (
    ENCODING, ServiceProvider, URL_ESCAPES, WORMS)
from lmtrex.common.s2n_type import S2nEndpoint, S2nOutput, S2nSchema

from lmtrex.tools.provider.api import APIQuery
from lmtrex.tools.s2n.utils  import get_traceback

# .............................................................................
class IpniAPI(APIQuery):
    """Class to query WoRMS API for a name match
    
    Todo:
        Extend for other services
    """
    PROVIDER = ServiceProvider.IPNI
    # NAME_MAP = S2nSchema.get_ipni_name_map()
    
    # ...............................................
    def __init__(self, name, other_filters={}, logger=None):
        """
        Constructor for WormsAPI class
        
        Args:
            other_filters: optional filters
            logger: optional logger for info and error messages.  If None, 
                prints to stdout
        """
        url = '{}/{}'.format(WORMS.REST_URL, WORMS.NAME_MATCH_SERVICE)
        other_filters[WORMS.MATCH_PARAM] = name
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
    
    # ...............................................
    @classmethod
    def _standardize_record(cls, rec, is_accepted=False):
        """
        dict_items([('name', 'Poa annua'), 
        ('authors', 'L.'), 
        ('publishingAuthor', 'L.'), 
        ('authorTeam', [{'name': 'L.', 'id': '12653-1', 'order': 0, 'type': 'aut', 'summary': None, 'url': '/a/12653-1'}]), 
        ('rank', 'spec.'), 
        ('url', '/n/320035-2'), 
        ('family', 'Poaceae'), ('genus', 'Poa'), ('species', 'annua'),
        ('citationType', 'tax. nov.'), ('hybrid', False), ('hybridGenus', False), 
        ('inPowo', True), 
        ('linkedPublication', {'abbreviation': 'Sp. Pl. [Linnaeus]', 'date': '1 May 1753', 'fqId': 'urn:lsid:ipni.org:publications:1071-2', 'id': '1071-2', 'lcNumber': 'QK91.S6', 'recordType': 'publication', 'remarks': 'For 2 volumes, the date of publication established as 1 May, 1753', 'suppressed': False, 'title': 'Species Plantarum', 'tl2Author': 'Linnaeus, Carl', 'tl2Number': '4.769', 'url': '/p/1071-2', 'version': '1.4', 'hasBhlLinks': True, 'hasBhlTitleLink': True, 'hasBhlPageLink': True, 'bhlPageLink': 'http://www.biodiversitylibrary.org/openurl?ctx_ver=Z39.88-2004&rft_id=http://www.biodiversitylibrary.org/page/33355180&rft_val_fmt=info:ofi/fmt:kev:mtx:book&url_ver=z39.88-2004', 'bhlTitleLink': 'http://www.biodiversitylibrary.org/openurl?ctx_ver=Z39.88-2004&rft_id=http://www.biodiversitylibrary.org/bibliography/669&rft_val_fmt=info:ofi/fmt:kev:mtx:book&url_ver=z39.88-2004'}), ('publication', 'Sp. Pl.'), ('publicationYear', 1753), ('publicationYearNote', '1 May 1753'), ('referenceCollation', '1: 68'), ('publicationId', '1071-2'), ('recordType', 'citation'), ('reference', 'Sp. Pl. 1: 68. 1753 [1 May 1753] '), ('remarks', '[Gandhi 30 Jun 2000]'), ('suppressed', False), ('topCopy', True), ('typeLocations', 'lectotype LINN (NO. 87.17, RIGHT-HAND PLANT) (NO. 87.17, RIGHT-HAND PLANT)'), ('version', '1.5'), ('id', '320035-2'), ('fqId', 'urn:lsid:ipni.org:names:320035-2'), ('hasNomenclaturalNotes', False), ('hasTypeData', True), ('hasOriginalData', False), ('hasLinks', False), ('bhlLink', 'http://www.biodiversitylibrary.org/openurl?ctx_ver=Z39.88-2004&rft.date=1753&rft.spage=68&rft.volume=1&rft_id=http://www.biodiversitylibrary.org/bibliography/669&rft_val_fmt=info:ofi/fmt:kev:mtx:book&url_ver=z39.88-2004')])

        """
        newrec = {}
        data_std_fld = S2nSchema.get_data_url_fld()
        hierarchy_fld = 'hierarchy'
        
        try:
            canonical_str = rec['name']
        except:
            canonical_str = ''

        try:
            auth_str = ' {}'.format(rec['authors'])
        except:
            auth_str = ''
        
        sciname_str = '{}{}'.format(canonical_str, auth_str)

        hierarchy = OrderedDict()
        for rnk in S2nSchema.RANKS:
            try:
                val = rec[rnk]
            except:
                pass
            else:
                hierarchy[rnk] = val
            
        for stdfld, provfld in cls.NAME_MAP.items():
            try:
                val = rec[provfld]
            except:
                val = None
                
            # Use ID field to construct data_url
            if provfld == hierarchy_fld:
                newrec[stdfld] = [hierarchy]
                
            elif provfld == WORMS.ID_FLDNAME:
                newrec[stdfld] = val
                newrec[data_std_fld] = WORMS.get_species_data(val)
                
            # all others, including view_url
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
    def _standardize_output(
            cls, output, service, query_status=None, query_urls=[], is_accepted=False, errinfo={}):
        total = 0
        stdrecs = []
        # output is an iterator over dictionaries
        resp = output._response
        if resp['totalResults'] > 0:
            for rec in output:
                total +=1
                newrec = cls._standardize_record(rec, is_accepted=is_accepted)
                if newrec:
                    stdrecs.append(newrec)
        prov_meta = cls._get_provider_response_elt(query_status=query_status, query_urls=query_urls)
        std_output = S2nOutput(
            total, service, provider=prov_meta, records=stdrecs, errors=errinfo)
        
        return std_output
    
    # ...............................................
    @classmethod
    def _parse_name(cls, namestr):
        genus = species = infrasp = auth = ''
        name_clean = namestr.strip()
        parts = name_clean.split(' ')
        try:
            genus = parts.pop(0)
        except:
            pass
        else:
            try:
                species = parts.pop(0)
            except:
                pass
            else:
                try:
                    tmp = parts[0]
                except:
                    pass
                else:
                    if not (tmp[0].isupper() or tmp[0] in ['('] or tmp[-1] in ['.', ',']):
                        infrasp = parts.pop(0)
                    auth = ' '.join(parts)
        return genus, species, infrasp, auth
                        
        
    # ...............................................
    @classmethod
    def match_name(cls, namestr, is_accepted=False, logger=None):
        """Return closest accepted species in IPNI taxonomy,
        
        Args:
            namestr: A scientific namestring possibly including author, year, 
                rank marker or other name information.
            is_accepted: match the ACCEPTED TaxonomicStatus 
                
        Returns:
            Either a dictionary containing a matching record with status 
                'accepted' or 'synonym' without 'alternatives'.  
            Or, if there is no matching record, return the first/best 
                'alternative' record with status 'accepted' or 'synonym'.
        """
        status = None
        errinfo = {}
        if is_accepted:
            status = 'accepted'
        genus, species, _, _ = cls._parse_name(namestr)
        
        query = { Name.genus: genus, Name.species: species }
        try:
            output = ipni.search(query)
        
        except Exception as e:
            tb = get_traceback()
            errinfo['error'] =  [cls._get_error_message(err=tb)]
            std_output = cls.get_api_failure(
                S2nEndpoint.Name, HTTPStatus.INTERNAL_SERVER_ERROR, errinfo=errinfo)
        else:
            qry = ''.join([
                'pykew.ipni.search(', '{', 
                ' Name.genus: {}, Name.species: {} '.format(genus, species), 
                '}', ')'])
            # Standardize output from provider response
            std_output = cls._standardize_output(
                output, S2nEndpoint.Name, query_status=200, query_urls=[qry], 
                is_accepted=is_accepted, errinfo=errinfo)
            
        return std_output



    # ...............................................
    def query(self):
        """ Queries the API and sets 'output' attribute to a ElementTree object
        """
        APIQuery.query_by_get(self, output_type='json')





# .............................................................................
if __name__ == '__main__':
    # test
    pass


"""
import pykew.ipni as ipni
import pykew.powo as powo

from pykew.ipni_terms import Name
from pykew.powo_timperms import Filters

gn = 'Poa'
sp = 'annua'

can = '{} {}'.format(gn, sp)
filter = { Name.genus: 'Poa', Name.species: 'annua' }

res1 = ipni.search(can)
res2 = ipni.search(filter)


"""