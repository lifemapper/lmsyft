"""Module containing functions for GBIF API Queries."""
from collections import OrderedDict
from logging import ERROR
import os
import requests
import urllib

from flask_app.broker.constants import GBIF, ISSUE_DEFINITIONS
from flask_app.common.s2n_type import (
    APIEndpoint, BrokerOutput, BrokerSchema, S2nKey, ServiceProvider)
from flask_app.common.constants import URL_ESCAPES

from spnet.common.constants import ENCODING
from spnet.common.log import logit
from spnet.common.util import add_errinfo
from sppy.provider.api import APIQuery


# .............................................................................
class GbifAPI(APIQuery):
    """Class to query GBIF APIs and return results."""
    PROVIDER = ServiceProvider.GBIF
    OCCURRENCE_MAP = BrokerSchema.get_gbif_occurrence_map()
    NAME_MAP = BrokerSchema.get_gbif_name_map()

    # ...............................................
    def __init__(
            self, service=GBIF.SPECIES_SERVICE, key=None, other_filters=None,
            logger=None):
        """Constructor.

        Args:
            service: GBIF service to query
            key: unique identifier for an object of this service
            other_filters: optional filters
            logger: object for logging messages and errors.
        """
        url = "/".join((GBIF.REST_URL, service))
        if key is not None:
            url = "/".join((url, str(key)))
        APIQuery.__init__(self, url, other_filters=other_filters, logger=logger)

    # ...............................................
    def _assemble_filter_string(self, filter_string=None):
        # Assemble key/value pairs
        if filter_string is None:
            all_filters = self._other_filters.copy()
            if self._q_filters:
                q_val = self._assemble_q_val(self._q_filters)
                all_filters[self.Q_KEY] = q_val
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
            val = out_dict[name]
        except Exception:
            return None
        if type(val) is bytes:
            val = str(val).encode(ENCODING)
        return val

    # ...............................................
    @classmethod
    def _get_nested_output_val(cls, output, key_list):
        while key_list:
            key = key_list[0]
            key_list = key_list[1:]
            try:
                output = output[key]
                if not key_list:
                    return str(output).encode(ENCODING)
            except Exception:
                return None

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
    #     except Exception:
    #         traceback = lmutil.get_traceback()
    #         errmsgs.append({"error": traceback})
    #     else:
    #         output = tax_api.output
    #         elements_of_interest = [
    #             "scientificName", "kingdom", "phylum", "class", "order",
    #             "family", "genus", "species", "rank", "genusKey", "speciesKey",
    #             "taxonomicStatus", "canonicalName", "scientificName", "kingdom",
    #             "phylum", "class", "order", "family", "genus", "species",
    #             "rank", "genusKey", "speciesKey", "taxonomicStatus",
    #             "canonicalName", "acceptedKey", "accepted", "nubKey"]
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
        """Return GBIF occurrences for this occurrenceId.

        Args:
            occid: occurrenceID for query
            count_only: boolean flag signaling to return records or only count
            logger: object for logging messages and errors.

        Returns:
            a dictionary containing one or more keys:
                count, records, error, warning

        Note:
            This should retrieve a single record if the occurrenceId is unique.

        Todo: enable paging
        """
        errinfo = {}
        api = GbifAPI(
            service=GBIF.OCCURRENCE_SERVICE, key=GBIF.SEARCH_COMMAND,
            other_filters={"occurrenceID": occid}, logger=logger)
        try:
            api.query()
        except Exception:
            std_output = cls._get_query_fail_output(
                [api.url], APIEndpoint.Occurrence)
        else:
            if api.error:
                add_errinfo(errinfo, "error", api.error)
            prov_meta = cls._get_provider_response_elt(
                query_status=api.status_code, query_urls=[api.url])

            # Standardize output from provider response
            std_output = cls._standardize_occurrence_output(
                api.output, prov_meta, count_only=count_only, errinfo=errinfo)

        return std_output

    # ...............................................
    @classmethod
    def _get_fld_vals(cls, big_rec):
        rec = {}
        for gbif_name, spnet_name in GbifAPI.NAME_MAP.items():
            try:
                rec[spnet_name] = big_rec[gbif_name]
            except KeyError:
                pass
        return rec

    # ...............................................
    @classmethod
    def _standardize_occurrence_record(cls, rec):
        newrec = {}
        parse_prov_fields = ["associatedSequences", "associatedReferences"]
        to_str_prov_fields = [
            "year", "month", "day", "decimalLongitude", "decimalLatitude"]
        view_std_fld = BrokerSchema.get_view_url_fld()
        data_std_fld = BrokerSchema.get_data_url_fld()
        issue_prov_fld = "issues"

        for stdfld, provfld in cls.OCCURRENCE_MAP.items():
            try:
                val = rec[provfld]
            except KeyError:
                val = None

            # Save ID field, plus use to construct URLs
            if provfld == GBIF.OCC_ID_FIELD:
                newrec[stdfld] = val
                newrec[view_std_fld] = GBIF.get_occurrence_view(val)
                newrec[data_std_fld] = GBIF.get_occurrence_data(val)

            # expand fields to dictionary, with code and definition
            elif provfld == issue_prov_fld:
                newrec[stdfld] = cls._get_code2description_dict(
                    val, ISSUE_DEFINITIONS[ServiceProvider.GBIF[S2nKey.PARAM]])

            # Modify/parse into list
            elif val and provfld in parse_prov_fields:
                lst = val.split("|")
                elts = [item.strip() for item in lst]
                newrec[stdfld] = elts

            # Modify int date elements to string (to match iDigBio)
            elif val and provfld in to_str_prov_fields:
                newrec[stdfld] = str(val)

            # all others
            else:
                newrec[stdfld] = val
        return newrec

    # ...............................................
    @classmethod
    def _standardize_name_record(cls, rec):
        newrec = {}
        view_std_fld = BrokerSchema.get_view_url_fld()
        data_std_fld = BrokerSchema.get_data_url_fld()
        hierarchy_fld = "hierarchy"

        for stdfld, provfld in cls.NAME_MAP.items():
            try:
                val = rec[provfld]
            except KeyError:
                val = None
            # Also use ID field to construct URLs
            if provfld == GBIF.SPECIES_ID_FIELD:
                newrec[stdfld] = val
                newrec[view_std_fld] = GBIF.get_species_view(val)
                newrec[data_std_fld] = GBIF.get_species_data(val)

            # Assemble from other fields
            elif provfld == hierarchy_fld:
                hierarchy = OrderedDict()
                for rnk in BrokerSchema.RANKS:
                    try:
                        val = rec[rnk]
                    except KeyError:
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
                outstatus = rec["status"].lower()
            except AttributeError:
                print(cls._get_error_message(msg="No status in record"))
            else:
                if outstatus == status:
                    is_good = True
        return is_good

    # ...............................................
    @classmethod
    def _standardize_match_output(
            cls, output, record_status, provider_meta, query_urls=None, errinfo=None):
        stdrecs = []
        try:
            alternatives = output.pop("alternatives")
        except KeyError:
            alternatives = []

        is_match = True
        try:
            if output["matchType"].lower() == "none":
                is_match = False
        except KeyError:
            msg = cls._get_error_message(msg=f"No matchType element in {output.keys()}")
            errinfo = add_errinfo(errinfo, "error", msg)
        except AttributeError:
            msg = cls._get_error_message(msg="No matchType")
            errinfo = add_errinfo(errinfo, "error", msg)
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
        # TODO: standardize_record and provide schema link
        std_output = BrokerOutput(
            total, APIEndpoint.Name, provider=provider_meta, records=stdrecs,
            errors=errinfo)
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
            cls, output, provider_meta, count_only=False, errinfo=None):
        # GBIF.COUNT_KEY, GBIF.RECORDS_KEY, GBIF.RECORD_FORMAT_OCCURRENCE,
        stdrecs = []
        total = 0
        # Count
        try:
            total = output[GBIF.COUNT_KEY]
        except KeyError:
            msg = cls._get_error_message(msg=f"Missing `{GBIF.COUNT_KEY}` element")
            errinfo = add_errinfo(errinfo, "error", msg)
        # Records
        if not count_only:
            try:
                recs = output[GBIF.RECORDS_KEY]
            except KeyError:
                msg = cls._get_error_message(
                    msg=f"Missing `{GBIF.RECORDS_KEY}` element")
                errinfo = add_errinfo(errinfo, "error", msg)
            else:
                stdrecs = []
                for r in recs:
                    try:
                        stdrecs.append(
                            cls._standardize_record(r, GBIF.RECORD_FORMAT_OCCURRENCE))
                    except Exception as e:
                        msg = cls._get_error_message(err=e)
                        errinfo = add_errinfo(errinfo, "error", msg)
        std_output = BrokerOutput(
            total, APIEndpoint.Occurrence, provider=provider_meta, records=stdrecs,
            errors=errinfo)

        return std_output

    # ...............................................
    @classmethod
    def get_occurrences_by_dataset(
            cls, gbif_dataset_key, count_only, logger=None):
        """Count and optionally return records with the given gbif_dataset_key.

        Args:
            gbif_dataset_key: unique identifier for the dataset, assigned by GBIF
                and retained by Specify
            count_only: boolean flag signaling to return records or only count
            logger: object for logging messages and errors.

        Returns:
            a dictionary containing one or more keys:
                count, records, error, warning

        Note:
            This currently only returns the first page (0-limit) of records.

        Todo:
            * handle large queries asynchronously
            * page results
        """
        errinfo = {}
        if count_only is True:
            limit = 1
        else:
            limit = GBIF.LIMIT
        api = GbifAPI(
            service=GBIF.OCCURRENCE_SERVICE, key=GBIF.SEARCH_COMMAND,
            other_filters={
                GBIF.REQUEST_DATASET_KEY: gbif_dataset_key, "offset": 0,
                "limit": limit}, logger=logger)
        try:
            api.query()
        except Exception:
            std_output = cls._get_query_fail_output(
                [api.url], APIEndpoint.Occurrence)
        else:
            if api.error:
                add_errinfo(errinfo, "error", api.error)
            prov_meta = cls._get_provider_response_elt(
                query_status=api.status_code, query_urls=[api.url])
            std_output = cls._standardize_occurrence_output(
                api.output, prov_meta, count_only=count_only, errinfo=errinfo)

        return std_output

    # ...............................................
    @classmethod
    def match_name(cls, namestr, is_accepted=False, logger=None):
        """Return closest accepted species in GBIF backbone taxonomy.

        Args:
            namestr: A scientific namestring possibly including author, year,
                rank marker or other name information.
            is_accepted: match the ACCEPTED TaxonomicStatus in the GBIF record
            logger: object for logging messages and errors.

        Returns:
            Either a dictionary containing a matching record with status
                "accepted" or "synonym" without "alternatives".
            Or, if there is no matching record, return the first/best
                "alternative" record with status "accepted" or "synonym".

        Note:
            * This function uses the name search API
            * GBIF TaxonomicStatus enum at:
            https://gbif.github.io/gbif-api/apidocs/org/gbif/api/vocabulary/TaxonomicStatus.html
        """
        status = None
        errinfo = {}
        if is_accepted:
            status = "accepted"
        name_clean = namestr.strip()
        other_filters = {"name": name_clean, "verbose": "true"}
#         if rank:
#             other_filters["rank"] = rank
#         if kingdom:
#             other_filters["kingdom"] = kingdom
        api = GbifAPI(
            service=GBIF.SPECIES_SERVICE, key="match",
            other_filters=other_filters, logger=logger)

        try:
            api.query()
        except Exception:
            std_output = cls._get_query_fail_output([api.url], APIEndpoint.Name)
        else:
            if api.error:
                add_errinfo(errinfo, "error", api.error)
            prov_meta = cls._get_provider_response_elt(
                query_status=api.status_code, query_urls=[api.url])
            # Standardize output from provider response
            std_output = cls._standardize_match_output(
                api.output, status, prov_meta, errinfo=errinfo)

        return std_output

    # ...............................................
    @classmethod
    def count_occurrences_for_taxon(cls, taxon_key, logger=None):
        """Return a count of occurrence records in GBIF with the indicated taxon.

        Args:
            taxon_key: A GBIF unique identifier indicating a taxon object.
            logger: object for logging messages and errors.

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
            other_filters={"taxonKey": taxon_key}, logger=logger)

        try:
            api.query_by_get()
        except Exception:
            std_output = cls._get_query_fail_output([api.url], APIEndpoint.Occurrence)
            # query_status = HTTPStatus.INTERNAL_SERVER_ERROR
            # errinfo["error"] = [cls._get_error_message(err=get_traceback())]
        else:
            try:
                total = api.output["count"]
            except Exception:
                errinfo = add_errinfo(
                    errinfo, "error", cls._get_error_message(
                        msg="Missing `count` element"))
            else:
                if total < 1:
                    errinfo = add_errinfo(
                        errinfo, "info", cls._get_error_message(msg="No match"))
                    simple_output[S2nKey.OCCURRENCE_URL] = None
                else:
                    simple_output[S2nKey.OCCURRENCE_URL] = api.url
            prov_meta = cls._get_provider_response_elt(
                query_status=api.status_code, query_urls=[api.url])
            std_output = BrokerOutput(
                total, APIEndpoint.Occurrence, provider=prov_meta, errors=errinfo)
        return std_output

    # ......................................
    @classmethod
    def _post_json_to_parser(cls, url, data, logger=None):
        response = output = None
        try:
            response = requests.post(url, json=data)
        except Exception as e:
            logit(
                logger, f"Failed on URL {url} ({e})", refname=cls.__name__,
                log_level=ERROR)
        else:
            if response.ok:
                try:
                    output = response.json()
                except Exception:
                    try:
                        output = response.content
                    except Exception:
                        output = response.text
                    else:
                        logit(
                            logger, f"Failed to interpret output of {url}",
                            refname=cls.__name__, log_level=ERROR)
            else:
                try:
                    ret_code = response.status_code
                    reason = response.reason
                except AttributeError:
                    logit(
                        logger, f"Failed to find failure reason for URL {url}",
                        refname=cls.__name__, log_level=ERROR)
                else:
                    logit(
                        logger, f"Failed on URL {url} ({ret_code}: {reason})",
                        refname=cls.__name__, log_level=ERROR)
        return output

# ...............................................
    @classmethod
    def _trim_parsed_output(cls, output, logger=None):
        recs = []
        for rec in output:
            # Only return parsed records
            try:
                success = rec["parsed"]
            except KeyError:
                logit(
                    logger, "Missing `parsed` field in record", refname=cls.__name__,
                    log_level=ERROR)
            else:
                if success:
                    recs.append(rec)
        return recs

# ...............................................
    @classmethod
    def parse_name(cls, namestr, logger=None):
        """Send a scientific name to the GBIF Parser returning a canonical name.

        Args:
            namestr: A scientific namestring possibly including author, year,
                rank marker or other name information.
            logger: object for logging messages and errors.

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
            if recs:
                try:
                    output["record"] = recs[0]
                except KeyError:
                    msg = f"Failed to return results from {name_api.url}"
                    logit(logger, msg, refname=cls.__name__, log_level=ERROR)
                    output[S2nKey.ERRORS] = msg
        return output, name_api.url

    # ...............................................
    @classmethod
    def parse_names(cls, names=None, filename=None, logger=None):
        """Parse a list or file of scientific names with the GBIF Parser.

        Args:
            names: a list of names to be parsed
            filename: a file of names to be parsed
            logger: object for logging messages and errors.

        Returns:
            A list of resolved records, each is a dictionary with keys of
            GBIF fieldnames and values with field values.

        Raises:
            Exception: on query failure.
            Exception: on no names or file.
        """
        if filename and os.path.exists(filename):
            with open(filename, "r", encoding=ENCODING) as in_file:
                for line in in_file:
                    names.append(line.strip())

        if not names:
            raise Exception("Must provide a list or filename of scientific names.")

        url = f"{GBIF.REST_URL}/{GBIF.PARSER_SERVICE}"
        try:
            output = GbifAPI._post_json_to_parser(url, names, logger=logger)
        except Exception as e:
            logit(
                logger, f"Failed to get response from GBIF for data {filename}, {e}",
                refname=cls.__name__, log_level=ERROR)
            raise

        if output:
            recs = GbifAPI._trim_parsed_output(output, logger=logger)
            if filename is not None:
                logit(
                    logger,
                    f"Wrote {len(recs)} parsed records from GBIF to file {filename}",
                    refname=cls.__name__)
            else:
                logit(
                    logger,
                    f"Found {len(recs)} records from GBIF for {len(names)} names",
                    refname=cls.__name__)

        return recs

    # ...............................................
    @classmethod
    def get_publishing_org(cls, pub_org_key, logger=None):
        """Return title from one organization record with this key.

        Args:
            pub_org_key: GBIF identifier for this publishing organization
            logger: object for logging messages and errors.

        Returns:
            pub_org_name: the name of the organization.

        Raises:
            Exception: on query failure.
        """
        org_api = GbifAPI(
            service=GBIF.ORGANIZATION_SERVICE, key=pub_org_key, logger=logger)
        try:
            org_api.query()
            pub_org_name = org_api._get_output_val(org_api.output, "title")
        except Exception as e:
            logit(logger, str(e), refname=cls.__name__)
            raise
        return pub_org_name

    # ...............................................
    @classmethod
    def get_dataset(cls, dataset_key, logger=None):
        """Return title from one dataset record with this key.

        Args:
            dataset_key: GBIF identifier for this dataset
            logger: object for logging messages and errors.

        Returns:
            dataset_name: the name of the dataset.
            citation: the preferred citation for the dataset.

        Raises:
            Exception: on query failure.
        """
        ds_api = GbifAPI(
            service=GBIF.DATASET_SERVICE, key=dataset_key, logger=logger)
        try:
            ds_api.query()
            dataset_name = ds_api._get_output_val(ds_api.output, "title")
        except Exception as e:
            logit(logger, str(e), refname=cls.__name__)
            raise
        try:
            citation = ds_api._get_nested_output_val(
                ds_api.output, ["citation", "text"])
        except Exception:
            citation = None
        return dataset_name, citation

    # ...............................................
    def query(self):
        """Query the API and set "output" attribute to a ElementTree object."""
        APIQuery.query_by_get(self, output_type="json", verify=False)


# .............................................................................
if __name__ == "__main__":
    # test
    pass

"""
from sppy.tools.provider.gbif import GbifAPI

dataset_key = 'e9d1c589-5df6-4bd8-aead-c09e2d8630e4'
ds_api = GbifAPI(service='dataset', key=dataset_key)
try:
    ds_api.query()
    dataset_name = ds_api._get_output_val(ds_api.output, "title")
except Exception as e:
    logit(logger, str(e), refname=cls.__name__)
    raise
try:
    citation = ds_api._get_nested_output_val(
        ds_api.output, ["citation", "text"])
except Exception as e:
    logit(logger, str(e), refname=cls.__name__)
    raise
return dataset_name, citation

"""
