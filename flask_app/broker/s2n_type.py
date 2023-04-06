"""Class for the output formats and keys used by Specify Network Name API service."""
from collections import OrderedDict
import typing

RecordsList = typing.List[typing.Dict]


# .............................................................................
class S2nEndpoint:
    """URL elements for a valid Specify Network API request."""
    Root = "/api/v1"
    Address = "address"
    Badge = "badge"
    Heartbeat = "heartbeat"
    Map = "map"
    Name = "name"
    Occurrence = "occ"
    Resolve = "resolve"
    SpecimenExtension = "occext"
    Frontend = "frontend"
    Stats = "stats"

    @classmethod
    def get_endpoints(cls):
        """Get the endpoints for all Specify Network API services.

        Returns:
            list of all S2nEndpoints
        """
        return [
            cls.Address, cls.Badge, cls.Name, cls.Occurrence, cls.Resolve,
            cls.Frontend
        ]


# .............................................................................
class S2nKey:
    """Keywords in a valid Specify Network API response."""
    # standard service output keys
    COUNT = "count"
    RECORD_FORMAT = "record_format"
    RECORDS = "records"
    ERRORS = "errors"
    # output one service at a time
    SERVICE = "service"
    # provider is a dictionary with keys code, label, query_url
    PROVIDER = "provider"
    PROVIDER_CODE = "code"
    PROVIDER_LABEL = "label"
    PROVIDER_STATUS_CODE = "status_code"
    PROVIDER_ICON_URL = "icon_url"
    PROVIDER_QUERY_URL = "query_url"
    # other S2N constant keys
    NAME = "name"
    # input request multiple services
    SERVICES = "services"
    PARAM = "param"
    OCCURRENCE_COUNT = "gbif_occurrence_count"
    OCCURRENCE_URL = "gbif_occurrence_url"

    # ...............................................
    @classmethod
    def response_keys(cls):
        """Top level keywords in valid Specify Network API response.

        Returns:
            list of all top level keywords in a flask_app.broker.s2n_type.S2nOutput
        """
        return {
            cls.COUNT, cls.RECORD_FORMAT, cls.RECORDS, cls.ERRORS,
            cls.SERVICE, cls.PROVIDER
        }

    # ...............................................
    @classmethod
    def response_provider_keys(cls):
        """Keywords in the provider element of a Specify Network API response.

        Returns:
            list of all keywords in provider element of a
                flask_app.broker.s2n_type.S2nOutput
        """
        return {
            cls.PROVIDER_CODE, cls.PROVIDER_LABEL, cls.PROVIDER_STATUS_CODE,
            cls.PROVIDER_QUERY_URL
        }


# .............................................................................
class COMMUNITY_SCHEMA:
    """Codes and URLs for community schemas used by the Specify Network."""
    DWC = {"code": "dwc", "url": "http://rs.tdwg.org/dwc/terms"}
    GBIF = {
        "code": "gbif",
        "url":
            "https://gbif.github.io/dwc-api/apidocs/org/gbif/dwc/terms/GbifTerm.html"
    }
    DCT = {"code": "dcterms", "url": "http://purl.org/dc/terms"}
    IDB = {"code": "idigbio", "url": ""}
    MS = {"code": "mopho", "url": "https://www.morphosource.org/About/API"}
    S2N = {"code": "s2n", "url": ""}


# .............................................................................
class S2nSchema:
    """Record schema for each of the services provided by the Specify Network."""
    NAME = OrderedDict({
        # Link to provider record webpage
        "view_url": COMMUNITY_SCHEMA.S2N,
        # API link to provider record data
        "api_url": COMMUNITY_SCHEMA.S2N,
        # S2n standardization of common elements
        "status": COMMUNITY_SCHEMA.S2N,
        "scientific_name": COMMUNITY_SCHEMA.S2N,
        "canonical_name": COMMUNITY_SCHEMA.S2N,
        "common_names": COMMUNITY_SCHEMA.S2N,
        "kingdom": COMMUNITY_SCHEMA.S2N,
        "rank": COMMUNITY_SCHEMA.S2N,
        # list of strings
        "synonyms": COMMUNITY_SCHEMA.S2N,
        # list of (one) dictionary containing rank: name
        "hierarchy": COMMUNITY_SCHEMA.S2N,

        # Occurrence data for this name
        S2nKey.OCCURRENCE_COUNT: COMMUNITY_SCHEMA.S2N,
        S2nKey.OCCURRENCE_URL: COMMUNITY_SCHEMA.S2N,

        # GBIF-specific fields
        "gbif_confidence": COMMUNITY_SCHEMA.S2N,
        "gbif_taxon_key": COMMUNITY_SCHEMA.S2N,

        # ITIS-specific fields
        "itis_tsn": COMMUNITY_SCHEMA.S2N,
        "itis_credibility": COMMUNITY_SCHEMA.S2N,

        # WoRMS-specific fields
        "worms_valid_AphiaID":  COMMUNITY_SCHEMA.S2N,
        "worms_lsid":  COMMUNITY_SCHEMA.S2N,
        "worms_isMarine":  COMMUNITY_SCHEMA.S2N,
        "worms_isBrackish":  COMMUNITY_SCHEMA.S2N,
        "worms_isFreshwater":  COMMUNITY_SCHEMA.S2N,
        "worms_isTerrestrial":  COMMUNITY_SCHEMA.S2N,
        "worms_isExtinct":  COMMUNITY_SCHEMA.S2N,
        "worms_match_type":  COMMUNITY_SCHEMA.S2N,
        })
    # MAP = OrderedDict({
    #     # Provider"s URLs to this record in dictionary
    #     # "provider_links": COMMUNITY_SCHEMA.S2N,
    #     "view_url": COMMUNITY_SCHEMA.S2N,
    #     "api_url": COMMUNITY_SCHEMA.S2N,
    #
    #     "endpoint": COMMUNITY_SCHEMA.S2N,
    #     "data_link": COMMUNITY_SCHEMA.S2N,
    #     "layer_type": COMMUNITY_SCHEMA.S2N,
    #     "layer_name": COMMUNITY_SCHEMA.S2N,
    #     # integer
    #     "point_count": COMMUNITY_SCHEMA.S2N,
    #     # list of 4 float values: minX, minY, maxX, maxY
    #     "point_bbox": COMMUNITY_SCHEMA.S2N,
    #     "species_name": COMMUNITY_SCHEMA.S2N,
    #     # dictionary with queryparameter/value
    #     "vendor_specific_parameters": COMMUNITY_SCHEMA.S2N,
    #     })
    OCCURRENCE = OrderedDict({
        # Provider"s URLs to this record in dictionary
        # "provider_links": COMMUNITY_SCHEMA.S2N,
        "view_url": COMMUNITY_SCHEMA.S2N,
        "api_url": COMMUNITY_SCHEMA.S2N,

        "scientificName": COMMUNITY_SCHEMA.DWC,
        "taxonRank": COMMUNITY_SCHEMA.DWC,
        "kingdom": COMMUNITY_SCHEMA.DWC,
        "phylum": COMMUNITY_SCHEMA.DWC,
        "class": COMMUNITY_SCHEMA.DWC,
        "order": COMMUNITY_SCHEMA.DWC,
        "family": COMMUNITY_SCHEMA.DWC,
        "genus": COMMUNITY_SCHEMA.DWC,
        "specificEpithet": COMMUNITY_SCHEMA.DWC,
        "scientificNameAuthorship": COMMUNITY_SCHEMA.DWC,

        "catalogNumber": COMMUNITY_SCHEMA.DWC,
        "collectionCode": COMMUNITY_SCHEMA.DWC,
        "institutionCode": COMMUNITY_SCHEMA.DWC,
        "otherCatalogNumbers": COMMUNITY_SCHEMA.DWC,        # list of strings
        "datasetName": COMMUNITY_SCHEMA.DWC,

        "year": COMMUNITY_SCHEMA.DWC,
        "month": COMMUNITY_SCHEMA.DWC,
        "day": COMMUNITY_SCHEMA.DWC,

        "recordedBy": COMMUNITY_SCHEMA.DWC,
        "fieldNumber": COMMUNITY_SCHEMA.DWC,

        "locality": COMMUNITY_SCHEMA.DWC,
        "county": COMMUNITY_SCHEMA.DWC,
        "stateProvince": COMMUNITY_SCHEMA.DWC,
        "country": COMMUNITY_SCHEMA.DWC,
        "countryCode": COMMUNITY_SCHEMA.DWC,
        "decimalLongitude": COMMUNITY_SCHEMA.DWC,           # string
        "decimalLatitude": COMMUNITY_SCHEMA.DWC,            # string
        "geodeticDatum": COMMUNITY_SCHEMA.DWC,

        "basisOfRecord": COMMUNITY_SCHEMA.DWC,
        "preparations": COMMUNITY_SCHEMA.DWC,

        "associatedReferences": COMMUNITY_SCHEMA.DWC,       # list of strings
        "associatedSequences": COMMUNITY_SCHEMA.DWC,        # list of strings

        # S2n resolution of non-standard contents
        # dictionary of codes: descriptions
        "issues": COMMUNITY_SCHEMA.S2N,

        "accessRights": COMMUNITY_SCHEMA.DCT,
        "language": COMMUNITY_SCHEMA.DCT,
        "license": COMMUNITY_SCHEMA.DCT,
        "modified": COMMUNITY_SCHEMA.DCT,
        "type": COMMUNITY_SCHEMA.DCT,

        # GBIF-specific field
        "gbifID": COMMUNITY_SCHEMA.GBIF,
        "publishingOrgKey": COMMUNITY_SCHEMA.GBIF,
        "datasetKey": COMMUNITY_SCHEMA.GBIF,
        "acceptedScientificName": COMMUNITY_SCHEMA.GBIF,

        # iDigBio-specific field
        "uuid": COMMUNITY_SCHEMA.IDB,

        # MorphoSource-specific field
        "specimen.specimen_id": COMMUNITY_SCHEMA.MS,

        # Specify7-specific field
        "specify_identifier": COMMUNITY_SCHEMA.S2N,
    })

    RANKS = ("kingdom", "phylum", "class", "order", "family", "genus", "species")

    # ...............................................
    @classmethod
    def get_view_url_fld(cls):
        """Get the URL for the provider webpage response.

        Returns:
            webpage URL
        """
        return "s2n:view_url"

    # ...............................................
    @classmethod
    def get_data_url_fld(cls):
        """Get the URL for the provider API response.

        Returns:
            API url
        """
        return "s2n:api_url"

    # ...............................................
    @classmethod
    def get_s2n_fields(cls, svc):
        """Get the record fieldnames for a Specify Network API response.

        Args:
            svc: S2nEndpoint of interest

        Returns:
            list of fieldnames in the records for a Specify Network service.

        Raises:
            Exception: on invalid Service requested.
        """
        if svc == S2nEndpoint.Name:
            schema = S2nSchema.NAME
        elif svc == S2nEndpoint.Occurrence:
            schema = S2nSchema.OCCURRENCE
        else:
            raise Exception(f"Service {svc} does not exist")
        ordered_flds = []
        for fname, ns in schema.items():
            ordered_flds.append(f"{ns['code']}:{fname}")
        return ordered_flds

    # ...............................................
    @classmethod
    def get_s2n_collection_fields(cls, svc):
        """Get fieldnames for list and dictionary elements in Specify Network response.

        Args:
            svc: S2nEndpoint of interest

        Returns:
            list_fields: list of fieldnames for response elements containing a list
                in a Specify Network response.
            dict_fields: list of fieldnames for response elements containing a dict
                in a Specify Network response.

        Raises:
            Exception: on invalid Service requested.
        """
        if svc == S2nEndpoint.Name:
            schema = cls.NAME
            list_fields = ["hierarchy", "synonyms"]
        elif svc == S2nEndpoint.Occurrence:
            schema = cls.OCCURRENCE
            list_fields = ["associatedSequences", "associatedReferences"]
            dict_fields = ["issues"]
        else:
            raise Exception(f"Service {svc} does not exist")

        # Standardize names
        list_fields = [f"{schema[fname]['code']}:{fname}" for fname in list_fields]
        dict_fields = [f"{schema[fname]['code']}:{fname}" for fname in dict_fields]

        return list_fields, dict_fields

    # ...............................................
    @classmethod
    def get_gbif_taxonkey_fld(cls):
        """Get the field in a Specify Network response containing the GBIF taxonKey.

        Returns:
            the fieldname in the S2nSchema containing the GBIF taxonKey.
        """
        return f"{COMMUNITY_SCHEMA.S2N['code']}:gbif_taxon_key"

    # ...............................................
    @classmethod
    def get_gbif_occcount_fld(cls):
        """Get the field in a Specify Network response containing the GBIF record count.

        Returns:
            the fieldname in the S2nSchema containing the count of records in GBIF.
        """
        return f"{COMMUNITY_SCHEMA.S2N['code']}:{S2nKey.OCCURRENCE_COUNT}"

    # ...............................................
    @classmethod
    def get_gbif_occurl_fld(cls):
        """Get the field in a Specify Network response containing the GBIF API query.

        Returns:
            the fieldname in the S2nSchema containing the GBIF API query.
        """
        return f"{COMMUNITY_SCHEMA.S2N['code']}:{S2nKey.OCCURRENCE_URL}"

    # ...............................................
    @classmethod
    def get_gbif_occurrence_map(cls):
        """Map the GBIF to the Specify Network occurrence response fields.

        Returns:
            dictionary of the GBIF fieldname (with namespace) to the simple S2n name.
        """
        stdfld_provfld = OrderedDict()
        for fn, comschem in S2nSchema.OCCURRENCE.items():
            std_name = f"{comschem['code']}:{fn}"
            stdfld_provfld[std_name] = fn
        return stdfld_provfld

    # ...............................................
    @classmethod
    def get_idb_occurrence_map(cls):
        """Map iDigBio response fields to the Specify Network response fields.

        Returns:
            dictionary of the iDigBio fieldname (with namespace) to the simple S2n name.
        """
        stdfld_provfld = OrderedDict()
        for fn, comschem in S2nSchema.OCCURRENCE.items():
            stdname = f"{comschem['code']}:{fn}"
            if fn == "uuid":
                stdfld_provfld[stdname] = fn
            else:
                stdfld_provfld[stdname] = stdname
        return stdfld_provfld

    # @classmethod
    # def get_specify_occurrence_map(cls):
    #     """Map broker response fields to Specify response fields.
    #
    #     Returns:
    #         dictionary of the Specify fieldname (with namespace) to the simple S2n name.
    #     """
    #     # sname_stdname = {}
    #     stdfld_provfld = OrderedDict()
    #     for fn, comschem in S2nSchema.OCCURRENCE.items():
    #         spfldname = f"{comschem['url']}/{fn}"
    #         stdname = f"{comschem['code']}:{fn}"
    #         stdfld_provfld[stdname] = spfldname
    #     return stdfld_provfld
    #
    # @classmethod
    # def get_specifycache_occurrence_map(cls):
    #     """Map broker response fields to Specify Cache response fields."""
    #     stdfld_provfld = OrderedDict()
    #     old_id = "identifier"
    #     new_id = "specify_identifier"
    #
    #     for fn, comschem in S2nSchema.OCCURRENCE.items():
    #         # if fn in names_in_spcache:
    #         stdname = f"{comschem['code']}:{fn}"
    #         if fn == new_id:
    #             stdfld_provfld[stdname] = old_id
    #         else:
    #             stdfld_provfld[stdname] = fn
    #
    #     return stdfld_provfld

    # ...............................................
    @classmethod
    def get_mopho_occurrence_map(cls):
        """Map the MorphoSource to the Specify Network response fields.

        Returns:
            dictionary of MorphoSource fieldname (with namespace) to the simple S2n name.
        """
        stdfld_provfld = OrderedDict()
        for fn, comschem in S2nSchema.OCCURRENCE.items():
            std_name = f"{comschem['code']}:{fn}"
            if fn == "catalogNumber":
                stdfld_provfld[std_name] = "specimen.catalog_number"
            elif fn == "institutionCode":
                stdfld_provfld[std_name] = "specimen.institution_code"
            elif fn == "occurrenceID":
                stdfld_provfld[std_name] = "specimen.occurrence_id"
            elif fn == "uuid":
                stdfld_provfld[std_name] = "specimen.uuid"
            elif fn in ["specimen.specimen_id", "view_url", "api_url"]:
                stdfld_provfld[std_name] = fn
        return stdfld_provfld

    # ...............................................
    @classmethod
    def get_gbif_name_map(cls):
        """Map the GBIF name response fields  to the Specify Network response fields.

        Returns:
            dictionary of the GBIF fieldname (with namespace) to the simple S2n name.
        """
        stdfld_provfld = OrderedDict()
        for fn, comschem in S2nSchema.NAME.items():
            std_name = "{}:{}".format(comschem["code"], fn)
            if fn == "scientific_name":
                oldname = "scientificName"
            elif fn == "canonical_name":
                oldname = "canonicalName"
            elif fn == "gbif_confidence":
                oldname = "confidence"
            elif fn == "gbif_taxon_key":
                oldname = "usageKey"
            else:
                oldname = fn
            if oldname:
                stdfld_provfld[std_name] = oldname
        return stdfld_provfld

    # ...............................................
    @classmethod
    def get_itis_name_map(cls):
        """Map ITIS response fields to the Specify Network response fields.

        Returns:
            dictionary of the ITIS fieldname (with namespace) to the simple S2n name.
        """
        stdfld_provfld = OrderedDict()
        for fn, comschem in S2nSchema.NAME.items():
            std_name = f"{comschem['code']}:{fn}"
            if fn == "scientific_name":
                oldname = "nameWTaxonAuthor"
            elif fn == "canonical_name":
                oldname = "nameWOInd"
            elif fn == "hierarchy":
                oldname = "hierarchySoFarWRanks"
            elif fn == "status":
                oldname = "usage"
            elif fn == "itis_tsn":
                oldname = "tsn"
            elif fn == "itis_credibility":
                oldname = "credibilityRating"
            else:
                oldname = fn
            if oldname:
                stdfld_provfld[std_name] = oldname
        return stdfld_provfld

    # ...............................................
    @classmethod
    def get_worms_name_map(cls):
        """Map WoRMS response fields to the Specify Network response fields.

        Returns:
            dictionary of the WoRMS fieldname (with namespace) to the simple S2n name.
        """
        stdfld_provfld = OrderedDict()
        for fn, comschem in S2nSchema.NAME.items():
            std_name = f"{comschem['code']}:{fn}"
            if fn == "view_url":
                oldname = "url"
            elif fn == "scientific_name":
                oldname = "valid_authority"
            elif fn == "canonical_name":
                oldname = "valid_name"
            elif fn == "worms_valid_AphiaID":
                oldname = "valid_AphiaID"
            elif fn == "worms_lsid":
                oldname = "lsid"
            elif fn == "worms_isMarine":
                oldname = "isMarine"
            elif fn == "worms_isBrackish":
                oldname = "isBrackish"
            elif fn == "worms_isFreshwater":
                oldname = "isFreshwater"
            elif fn == "worms_isTerrestrial":
                oldname = "isTerrestrial"
            elif fn == "worms_isExtinct":
                oldname = "isExtinct"
            elif fn == "worms_match_type":
                oldname = "match_type"
            else:
                oldname = fn
            if oldname:
                stdfld_provfld[std_name] = oldname
        return stdfld_provfld


# # .............................................................................
# Change to TypedDict on update to Python 3.8+
# # This corresponds to the base_response in OpenAPI specification
# class S2nOutput(typing.NamedTuple):
#     count: int
#     query_term: str
#     service: str
#     record_format: str = ""
#     provider: dict = {}
#     records: typing.List[dict] = []
#     errors: typing.List[dict] = []
#
#
# # .............................................................................
# def print_s2n_output(out_obj, count_only=False):
#     missing = 0
#     print("*** S^n output ***")
#     elements = {
#         "count": out_obj.count, "provider": out_obj.provider,
#         "errors": out_obj.errors,
#         "query_term": out_obj.query_term, "records": out_obj.records }
#     for name, attelt in elements.items():
#         try:
#             if name == "records" and count_only is True:
#                 print("{}: {} returned records".format(name, len(attelt)))
#             else:
#                 print("{}: {}".format(name, attelt))
#         except:
#             missing += 1
#             print("Missing {} element".format(name))
#     print("Missing {} elements".format(missing))
#     print("")


# # .............................................................................
# class S2nOutput(dict):
#     "count": int
#     "query_term": str
#     "service": str
#     "record_format": str = ""
#     "provider": dict = {}
#     "records": typing.List[dict] = []
#     "errors": typing.List[str] = []
#
#     # ...............................................
#     def __init__(
#             self, count, query_term, service, provider, provider_query=[],
#             record_format="", records=[], errors=[]):
#         so = {
#             "count": count, "query_term": query_term, "service": service,
#             "provider": provider,
#             "record_format": record_format, "records": records, "errors": errors
#             }
#         return so

# # .............................................................................
# class S2n:
#     RECORD_FORMAT = "Lifemapper service broker schema TBD"
# provider element"s query_url replaces query_term


# .............................................................................
class S2nOutput(object):
    """Format for all Specify Network query responses."""
    count: int
    service: str
    provider: dict = {}
    record_format: str = ""
    records: typing.List[dict] = []
    errors: dict = {}

    # ...............................................
    def __init__(
            self, count, service, provider=None, record_format=None, records=None,
            errors=None
    ):
        """Constructor.

        Args:
            count: Number of records in the object.
            service: API Service this object is responding to.
            provider: Provider returning in this response.
            record_format: Format of the records in this response.
            records: Records in this response.
            errors: Errors encountered when generating this response.
        """
        if errors is None:
            errors = {}
        if provider is None:
            provider = {}
        if record_format is None:
            record_format = ""
        if records is None:
            records = []
        # Dictionary is json-serializable
        self._response = {
            S2nKey.COUNT: count,
            S2nKey.SERVICE: service,
            S2nKey.PROVIDER: provider,
            S2nKey.RECORD_FORMAT: record_format,
            S2nKey.RECORDS: records,
            S2nKey.ERRORS: errors
        }

    # ...............................................
    def set_value(self, prop, value):
        """Set the keyword and value for part of a S2nOutput query response.

        Args:
            prop: keyword in the response
            value: value for the response

        Raises:
            Exception: on an invalid prop/keyword.
        """
        if prop in S2nKey.response_keys():
            self._response[prop] = value

        elif prop in S2nKey.response_provider_keys():
            self._response[S2nKey.PROVIDER][prop] = value

        else:
            raise Exception(f"Unrecognized property {prop}")

    # ...............................................
    def append_value(self, prop, value):
        """Add a value for the keyword in part of a S2nOutput query response.

        Args:
            prop: keyword in the response
            value: value to be added to the response

        Raises:
            Exception: on an invalid prop/keyword.
        """
        # Do not add null value to list
        if value:
            if prop == S2nKey.RECORDS:
                # Append or set
                self._response[prop].append(value)
            elif prop == S2nKey.PROVIDER_QUERY_URL:
                # Append or set
                self._response[S2nKey.PROVIDER][S2nKey.PROVIDER_QUERY_URL].append(value)
            else:
                raise Exception(
                    f"Property {prop} is not a multi-value element, use `set_value`")

    # ...............................................
    def append_error(self, error_type, error_desc):
        """Add a value to the errors in a S2nOutput query response.

        Args:
            error_type: keyword in the error response
            error_desc: description to be added to the error response
        """
        try:
            self._response[S2nKey.ERRORS][error_type].append(error_desc)
        except KeyError:
            self._response[S2nKey.ERRORS][error_type] = [error_desc]

    # ...............................................
    @property
    def response(self):
        """Return the S2nOutput query response.

        Returns:
            the response object
        """
        return self._response

    # ...............................................
    @property
    def count(self):
        """Return the number of elements from a S2nOutput query response.

        Returns:
            The value of the record count.
        """
        return self._response[S2nKey.COUNT]

    # ...............................................
    @property
    def service(self):
        """Return the service element from a S2nOutput query response.

        Returns:
            The service element of the query response.
        """
        return self._response[S2nKey.SERVICE]

    # ...............................................
    @property
    def provider(self):
        """Return the provider element from a S2nOutput query response.

        Returns:
            The provider element of the query response.
        """
        return self._response[S2nKey.PROVIDER]

    # ...............................................
    @property
    def provider_code(self):
        """Return the provider code from a S2nOutput query response.

        Returns:
            The provider code of the query response.
        """
        return self._response[S2nKey.PROVIDER][S2nKey.PROVIDER_CODE]

    # ...............................................
    @property
    def provider_label(self):
        """Return the provider label from a S2nOutput query response.

        Returns:
            The provider label of the query response.
        """
        return self._response[S2nKey.PROVIDER][S2nKey.PROVIDER_LABEL]

    # ...............................................
    @property
    def provider_status_code(self):
        """Return the provider status code from a S2nOutput query response.

        Returns:
            The provider status code of the query response.
        """
        return self._response[S2nKey.PROVIDER][S2nKey.PROVIDER_STATUS_CODE]

    # ...............................................
    @property
    def provider_query(self):
        """Return the provider query URL from a S2nOutput query response.

        Returns:
            The provider query URL of the query response.
        """
        try:
            return self._response[S2nKey.PROVIDER][S2nKey.PROVIDER_QUERY_URL]
        except KeyError:
            return None

    # ...............................................
    @property
    def record_format(self):
        """Return the record format from a S2nOutput query response.

        Returns:
            The record format of the records in the query response.
        """
        return self._response[S2nKey.RECORD_FORMAT]

    # ...............................................
    @property
    def records(self):
        """Return the recprds from a S2nOutput query response.

        Returns:
            The records in the query response.
        """
        return self._response[S2nKey.RECORDS]

    # ...............................................
    @property
    def errors(self):
        """Return the errors from a S2nOutput query response.

        Returns:
            The errors of the query response.
        """
        return self._response[S2nKey.ERRORS]

    # ...............................................
    def format_records(self, ordered_fieldnames):
        """Order output fields of all records according to the provided schema.

        Args:
            ordered_fieldnames: list of fieldnames defined in
                flask_app.broker.s2n_type.S2nSchema
        """
        ordered_recs = []
        list_fields, dict_fields = S2nSchema.get_s2n_collection_fields(
            self._response[S2nKey.SERVICE])
        recs = self._response[S2nKey.RECORDS]
        for rec in recs:
            ordrec = OrderedDict({})
            for fn in ordered_fieldnames:
                try:
                    val = rec[fn]
                except KeyError:
                    val = None

                if val is not None:
                    ordrec[fn] = val
                else:
                    if fn in list_fields:
                        ordrec[fn] = []
                    elif fn in dict_fields:
                        ordrec[fn] = {}
                    else:
                        ordrec[fn] = None
            if ordrec:
                ordered_recs.append(ordrec)
        self._response[S2nKey.RECORDS] = ordered_recs


# .............................................................................
def _print_oneprov_output(oneprov, do_print_rec):
    print("* One provider S^n output *")
    for name, attelt in oneprov.items():
        try:
            if name == "records":
                print("   records")
                if do_print_rec is False:
                    print(f"      {name}: {len(attelt)} returned records")
                else:
                    for rec in attelt:
                        print("      record")
                        for k, v in rec.items():
                            print("         {}: {}".format(k, v))
            else:
                print("   {}: {}".format(name, attelt))
        except Exception:
            pass


# ....................................
def print_s2n_output(response_dict, do_print_rec=False):
    """Print a formatted string of the elements in an S2nOutput query response.

    Args:
        response_dict: flask_app.broker.s2n_type.S2nOutput object
        do_print_rec: True to print each record in the response.
    """
    print("*** Dictionary of S^n dictionaries ***")
    for name, attelt in response_dict.items():
        try:
            if name == "records":
                print(f"{name}: ")
                for respdict in attelt:
                    _print_oneprov_output(respdict, do_print_rec)
            else:
                print(f"{name}: {attelt}")
        except Exception:
            pass
    outelts = set(response_dict.keys())
    missing = S2nKey.response_keys().difference(outelts)
    extras = outelts.difference(S2nKey.response_keys())
    if missing:
        print(f"Missing elements: {missing}")
    if extras:
        print(f"Extra elements: {extras}")
    print("")
