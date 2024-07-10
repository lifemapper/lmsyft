"""Class for the output formats and keys used by Specify Network Name API service."""
from collections import OrderedDict
import typing
from flask_app.common.util import get_host_url

RecordsList = typing.List[typing.Dict]


# .............................................................................
class S2nKey:
    """Keywords in a valid Specify Network API response."""
    # standard service output keys
    COUNT = "count"
    DESCRIPTION = "description"
    RECORD_FORMAT = "record_format"
    RECORDS = "records"
    OUTPUT = "output"
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
    def broker_response_keys(cls):
        """Top level keywords in valid Specify Network API response.

        Returns:
            list of all top level keywords in a flask_app.broker.s2n_type.BrokerOutput
        """
        return {
            cls.COUNT, cls.RECORD_FORMAT, cls.RECORDS, cls.ERRORS,
            cls.SERVICE, cls.PROVIDER
        }

    # ...............................................
    @classmethod
    def broker_response_provider_keys(cls):
        """Keywords in the provider element of a Specify Network API response.

        Returns:
            list of all keywords in provider element of a
                flask_app.common.s2n_type.BrokerOutput
        """
        return {
            cls.PROVIDER_CODE, cls.PROVIDER_LABEL, cls.PROVIDER_STATUS_CODE,
            cls.PROVIDER_QUERY_URL
        }

    # ...............................................
    @classmethod
    def analyst_response_keys(cls):
        """Top level keywords in valid Specify Network Analyst API response.

        Returns:
            list of all top level keywords in a flask_app.analyst API response
        """
        return {cls.SERVICE, cls.DESCRIPTION, cls.RECORDS, cls.ERRORS}


# .............................................................................
class APIEndpoint:
    """URL elements for a valid Specify Network API request."""
    Root = "api/v1"
    Analyst = "analyst"
    Broker = "broker"
    # Broker services
    Badge = "badge"
    Name = "name"
    Occurrence = "occ"
    Frontend = "frontend"
    # Analyst services
    Compare = "compare"
    Summary = "summary"
    # Count = "count"
    Rank = "rank"

    @classmethod
    def Resources(cls):
        """Get the endpoints for each of the API tools, Analyst and Broker.

        Returns:
             a dictionary containing the endpoints for each of the API tools, Analyst
                and Broker.
        """
        return {
            cls.Analyst:
                [
                    cls.Compare,
                    cls.Summary,
                    cls.Rank
                ],
            cls.Broker:
                [
                    cls.Badge,
                    cls.Name,
                    cls.Occurrence,
                    cls.Frontend
                ]
        }

    @classmethod
    def BrokerServices(cls):
        """Get the endpoints for the Broker API tools.

        Returns:
             a dictionary containing the endpoints for the Broker API tools.
        """
        return cls.Resources()[cls.Broker]

    @classmethod
    def AnalystServices(cls):
        """Get the endpoints for the Analyst API tools.

        Returns:
             a dictionary containing the endpoints for the Analyst API tools.
        """
        return cls.Resources()[cls.Analyst]

    @classmethod
    def get_analyst_endpoints(cls):
        """Get the endpoints for all Specify Network Analyst API services.

        Returns:
            list of all Endpoints
        """
        return [f"{cls.Root}/{svc}" for svc in cls.AnalystServices()]

    @classmethod
    def get_broker_endpoints(cls):
        """Get the endpoints for all Specify Network API services.

        Returns:
            list of all BrokerEndpoints
        """
        return [f"{cls.Root}/{svc}" for svc in cls.BrokerServices()]


# .............................................................................
class APIService:
    """Endpoint, parameters, output format for all Specify Network Broker APIs."""
    BaseSpNetwork = {
        "name": "",
        "endpoint": "",
        "params": {},
        "description": "",
        S2nKey.RECORD_FORMAT: None
    }
    BrokerRoot = {
        "name": APIEndpoint.Broker,
        "endpoint": APIEndpoint.Root,
        "params": {},
        "description": "",
        S2nKey.RECORD_FORMAT: None
    }
    AnalystRoot = {
        "name": APIEndpoint.Analyst,
        "endpoint": APIEndpoint.Root,
        "params": {},
        "description": "",
        S2nKey.RECORD_FORMAT: None
    }
    # Analyst Summary stats
    Compare = {
        "name": APIEndpoint.Compare,
        "endpoint": f"{APIEndpoint.Root}/{APIEndpoint.Compare}",
        "params": {
            # TODO: extend dimensions to other measurements
            "summary_type": {
                "type": "",
                "description":
                    "Type or dimension of data to compare (i.e: species, dataset)",
                "options": ["dataset", "species"],
                "default": "dataset"
            },
            "summary_key": {
                "type": "",
                "description":
                    "Key of type of data to compare (i.e: species_key, dataset_key)",
                "default": None
            },
        },
        "description":
            "Compare the counts for one item in of all dimensions of the "
            "occurrence data against the counts of all other items.",
        S2nKey.RECORD_FORMAT: ""
    }
    # Analyst Summary stats
    Summary = {
        "name": APIEndpoint.Summary,
        "endpoint": f"{APIEndpoint.Root}/{APIEndpoint.Summary}",
        "params": {
            # TODO: extend dimensions to other measurements
            "summary_type": {
                "type": "",
                "description":
                    "Type or dimension of data to summarize (i.e: species, dataset)",
                "options": ["dataset", "species"],
                "default": "dataset"
            },
            "summary_key": {
                "type": "",
                "description":
                    "Key of type of data to summarize (i.e: species_key, dataset_key)",
                "default": None
            },
        },
        "description":
            "Summarize the counts for one or all items of all dimensions of the "
            "occurrence data.",
        S2nKey.RECORD_FORMAT: ""

    }
    # Rankings
    Rank = {
        "name": APIEndpoint.Rank,
        "endpoint": f"{APIEndpoint.Root}/{APIEndpoint.Rank}",
        "params": {
            "summary_type": {
                "type": "",
                "description":
                    "Type or dimension of data to summarize (i.e: species, dataset)",
                "options": ["dataset", "species"],
                "default": "dataset"
            },
            "summary_key": {
                "type": "",
                "description":
                    "Key of type of data to summarize (i.e: species_key, dataset_key)",
                "default": None
            },
            "rank_by": {
                "type": "",
                "description":
                    "Measurement or dimension of data to rank by (i.e: occurrence "
                    "counts or other dimension)",
                # TODO: extend dimensions to other measurements
                "options": ["occurrence", "dataset", "species"],
                # None will resolve to the other dimension while there are only 2
                "default": "occurrence",
            },
            "order": {
                "type": "",
                "options": ["ascending", "descending"],
                "default": "descending"
            },
            "limit": {"type": 2, "default": 10, "min": 1, "max": 500},
        },
        "description":
            "Return an ordered list of summaries of one type/dimension of data, ranked "
            "by occurrence counts or another dimension of the data for the top X "
            "(descending) or bottom X (ascending) datasets",
        S2nKey.RECORD_FORMAT: ""
    }
    # Broker endpoints
    # Icons for service providers
    Badge = {
        "name": APIEndpoint.Badge,
        "endpoint": f"{APIEndpoint.Root}/{APIEndpoint.Badge}",
        "params": {
            "provider": {
                "type": "",
                "default": None,
                "options": ["gbif", "idb", "itis", "mopho", "worms"]
            },
            "icon_status": {
                "type": "",
                "options": ["active", "inactive", "hover"],
                "default": None
            }
        },
        "description": "Return an icon for the given data provider service.",
        S2nKey.RECORD_FORMAT: "image/png"
    }
    # Taxonomic Resolution
    Name = {
        "name": APIEndpoint.Name,
        "endpoint": f"{APIEndpoint.Root}/{APIEndpoint.Name}",
        "params": {
            "provider": {
                "type": "",
                "default": None,
                "options": ["gbif", "itis", "worms"]
            },
            "namestr": {"type": "", "default": None},
            "is_accepted": {"type": False, "default": False},
            "gbif_parse": {"type": False, "default": False},
            "gbif_count": {"type": False, "default": False},
            "kingdom": {"type": "", "default": None}
        },
        "description":
            "Return `accepted` taxonomic names for a given string from taxonomic name "
            "services.",
        S2nKey.RECORD_FORMAT: ""
    }
    # Specimen occurrence records
    Occurrence = {
        "name": APIEndpoint.Occurrence,
        "endpoint": f"{APIEndpoint.Root}/{APIEndpoint.Occurrence}",
        "params": {
            "provider": {
                "type": "",
                "default": None,
                # Must match parameters in ServiceProvider["param"]
                "options": ["gbif", "idb", "mopho"]
            },
            "occid": {"type": "", "default": None},
            "gbif_dataset_key": {"type": "", "default": None},
            "count_only": {"type": False, "default": False},
        },
        "description":
            "Return occurrence records for a given identifier from occurrence data "
            "aggregators.",
        S2nKey.RECORD_FORMAT: ""
    }
    Frontend = {
        "name": APIEndpoint.Frontend,
        "endpoint": f"{APIEndpoint.Root}/{APIEndpoint.Frontend}",
        "params": {
            "occid" : {"type": "", "default": None},
            "namestr": {"type": "", "default": None}
        },
        "description":
            "Return a formatted webpage of metadata for a given occurrence identifier "
            "and its scientific name from occurrence aggregators and taxonomic name "
            "services.",
        S2nKey.RECORD_FORMAT: ""
    }

    @classmethod
    def _get_provider_param(cls):
        return {
            "type": "",
            "default": None,
            "options": [
                ServiceProvider.GBIF[S2nKey.PARAM],
                ServiceProvider.iDigBio[S2nKey.PARAM],
                ServiceProvider.ITISSolr[S2nKey.PARAM],
                ServiceProvider.MorphoSource[S2nKey.PARAM],
            ]
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
class BrokerSchema:
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
            svc: APIEndpoint of interest

        Returns:
            list of fieldnames in the records for a Specify Network service.

        Raises:
            Exception: on invalid Service requested.
        """
        if svc == APIEndpoint.Name:
            schema = BrokerSchema.NAME
        elif svc == APIEndpoint.Occurrence:
            schema = BrokerSchema.OCCURRENCE
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
            svc: BrokerEndpoint of interest

        Returns:
            list_fields: list of fieldnames for response elements containing a list
                in a Specify Network response.
            dict_fields: list of fieldnames for response elements containing a dict
                in a Specify Network response.

        Raises:
            Exception: on invalid Service requested.
        """
        if svc == APIEndpoint.Name:
            schema = cls.NAME
            list_fields = ["hierarchy", "synonyms"]
            dict_fields = []
        elif svc == APIEndpoint.Occurrence:
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
            the fieldname in the BrokerSchema containing the GBIF taxonKey.
        """
        return f"{COMMUNITY_SCHEMA.S2N['code']}:gbif_taxon_key"

    # ...............................................
    @classmethod
    def get_gbif_occcount_fld(cls):
        """Get the field in a Specify Network response containing the GBIF record count.

        Returns:
            the fieldname in the BrokerSchema containing the count of records in GBIF.
        """
        return f"{COMMUNITY_SCHEMA.S2N['code']}:{S2nKey.OCCURRENCE_COUNT}"

    # ...............................................
    @classmethod
    def get_gbif_occurl_fld(cls):
        """Get the field in a Specify Network response containing the GBIF API query.

        Returns:
            the fieldname in the BrokerSchema containing the GBIF API query.
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
        for fn, comschem in BrokerSchema.OCCURRENCE.items():
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
        for fn, comschem in BrokerSchema.OCCURRENCE.items():
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
    #     for fn, comschem in BrokerSchema.OCCURRENCE.items():
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
    #     for fn, comschem in BrokerSchema.OCCURRENCE.items():
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
        for fn, comschem in BrokerSchema.OCCURRENCE.items():
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
        for fn, comschem in BrokerSchema.NAME.items():
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
        for fn, comschem in BrokerSchema.NAME.items():
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
        for fn, comschem in BrokerSchema.NAME.items():
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

# .............................................................................
class BrokerOutput(object):
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
        if prop in S2nKey.broker_response_keys():
            self._response[prop] = value

        elif prop in S2nKey.broker_response_provider_keys():
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
    def combine_errors(self, errinfo):
        """Combine a dictionary of errors to the errors in a S2nOutput query response.

        Args:
            errinfo: dictionary of errors, with error level, and list of descriptions.
        """
        for err_type, err_desc in errinfo.items():
            try:
                self._response[S2nKey.ERRORS][err_type].append(err_desc)
            except KeyError:
                self._response[S2nKey.ERRORS][err_type] = [err_desc]

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
    def record_count(self):
        """Return the number of records in a S2nOutput query response.

        Returns:
            The value of the record count.
        """
        return len(self._response[S2nKey.RECORDS])

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
                flask_app.broker.s2n_type.BrokerSchema
        """
        ordered_recs = []
        list_fields, dict_fields = BrokerSchema.get_s2n_collection_fields(
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
    @classmethod
    def _print_sub_output(cls, oneelt, do_print_rec):
        print("* One record of Specify Network Outputs *")
        for name, attelt in oneelt.items():
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
    @classmethod
    def print_output(cls, response_dict, do_print_rec=False):
        """Print a formatted string of the elements in an S2nOutput query response.

        Args:
            response_dict: flask_app.broker.s2n_type.S2nOutput._response dictionary
            do_print_rec: True to print each record in the response.

        TODO: move to a class method
        """
        print("*** Broker output ***")
        for name, attelt in response_dict.items():
            try:
                if name == "records":
                    print("records: ")
                    for respdict in attelt:
                        cls._print_sub_output(respdict, do_print_rec)
                else:
                    print(f"{name}: {attelt}")
            except Exception:
                pass
        # outelts = set(response_dict.keys())
        # missing = S2nKey.broker_response_keys().difference(outelts)
        # extras = outelts.difference(S2nKey.broker_response_keys())
        # if missing:
        #     print(f"Missing elements: {missing}")
        # if extras:
        #     print(f"Extra elements: {extras}")
        print("")


# .............................................................................
class AnalystOutput:
    """Response type for a Specify Network Analyst query."""
    service: str
    description: str = ""
    # records: typing.List[dict] = []
    records: typing.List = []
    errors: dict = {}

    # ...............................................
    def __init__(self, service, description=None, output=None, errors=None):
        """Constructor.

        Args:
            service: API Service this object is responding to.
            description: Description of the computation in this response.
            output: Statistics (dict) in this response.
            errors: Errors encountered when generating this response.
        """
        if errors is None:
            errors = {}
        if description is None:
            description = ""
        if output is None:
            output = {}
        # Dictionary is json-serializable
        self._response = {
            S2nKey.SERVICE: service,
            S2nKey.DESCRIPTION: description,
            S2nKey.OUTPUT: output,
            S2nKey.ERRORS: errors
        }

    # ...............................................
    @property
    def response(self):
        """Return the S2nOutput query response.

        Returns:
            the response object
        """
        return self._response

    # ....................................
    @classmethod
    def print_output(cls, response_dict, do_print_rec=False):
        """Print a formatted string of the elements in an S2nOutput query response.

        Args:
            response_dict: flask_app.broker.s2n_type.S2nOutput._response dictionary
            do_print_rec: True to print each record in the response.
        """
        print("*** Analyst output ***")
        for name, attelt in response_dict.items():
            try:
                if name == "records" and do_print_rec:
                    print("records: ")
                    for rec in attelt:
                        print(rec)
                else:
                    print(f"{name}: {attelt}")
            except Exception:
                pass


# .............................................................................
class ServiceProvider:
    """Name and metadata for external Specify Network data providers."""
    Broker = {
        S2nKey.NAME: "Specify Network",
        S2nKey.PARAM: "specifynetwork",
        S2nKey.SERVICES: [APIEndpoint.Badge],
        # "icon": {"active": "{}/SpNetwork_active.png",
        #          "inactive": "{}/SpNetwork_inactive.png",
        #          "hover": "{}/SpNetwork_hover.png"}
    }
    GBIF = {
        S2nKey.NAME: "GBIF",
        S2nKey.PARAM: "gbif",
        S2nKey.SERVICES: [APIEndpoint.Occurrence, APIEndpoint.Name, APIEndpoint.Badge],
        "icon": {
            "active": "gbif_active-01.png",
            "inactive": "gbif_inactive-01.png",
            "hover": "gbif_hover-01-01.png"
        }
    }
    iDigBio = {
        S2nKey.NAME: "iDigBio",
        S2nKey.PARAM: "idb",
        S2nKey.SERVICES: [APIEndpoint.Occurrence, APIEndpoint.Badge],
        "icon": {
            "active": "idigbio_colors_active-01.png",
            "inactive": "idigbio_colors_inactive-01.png",
            "hover": "idigbio_colors_hover-01.png"
        }
    }
    ITISSolr = {
        S2nKey.NAME: "ITIS",
        S2nKey.PARAM: "itis",
        S2nKey.SERVICES: [APIEndpoint.Badge, APIEndpoint.Name],
        "icon": {
            "active": "itis_active.png",
            "inactive": "itis_inactive.png",
            "hover": "itis_hover.png"
        }
    }
    MorphoSource = {
        S2nKey.NAME: "MorphoSource",
        S2nKey.PARAM: "mopho",
        S2nKey.SERVICES: [
            APIEndpoint.Badge, APIEndpoint.Occurrence],
        "icon": {
            "active": "morpho_active-01.png",
            "inactive": "morpho_inactive-01.png",
            "hover": "morpho_hover-01.png"
        }
    }
    # TODO: need an WoRMS badge
    WoRMS = {
        S2nKey.NAME: "WoRMS",
        S2nKey.PARAM: "worms",
        S2nKey.SERVICES: [APIEndpoint.Badge, APIEndpoint.Name],
        "icon": {
            "active": "worms_active.png",
        }
    }

    # ....................
    @classmethod
    def get_values(cls, param_or_name):
        """Return the ServiceProvider object for standard provider long or short name.

        Args:
            param_or_name: full name or URL parameter for a ServiceProvider.

        Returns:
            ServiceProvider object.
        """
        if param_or_name in (
                cls.GBIF[S2nKey.NAME], cls.GBIF[S2nKey.PARAM]
        ):
            return cls.GBIF
        elif param_or_name in (
                cls.iDigBio[S2nKey.NAME],
                cls.iDigBio[S2nKey.PARAM]
        ):
            return cls.iDigBio
        elif param_or_name in (
                cls.ITISSolr[S2nKey.NAME],
                cls.ITISSolr[S2nKey.PARAM]
        ):
            return cls.ITISSolr
        elif param_or_name in (
                cls.MorphoSource[S2nKey.NAME],
                cls.MorphoSource[S2nKey.PARAM]
        ):
            return cls.MorphoSource
        elif param_or_name in (
                cls.WoRMS[S2nKey.NAME], cls.WoRMS[S2nKey.PARAM]
        ):
            return cls.WoRMS
        elif param_or_name in (
                cls.Broker[S2nKey.NAME],
                cls.Broker[S2nKey.PARAM]
        ):
            return cls.Broker
        else:
            return None

    # ....................
    @classmethod
    def is_valid_param(cls, param):
        """Return a flag indicating if the parameter key is valid for services.

        Args:
            param: keyword for URL request to a sp_network service

        Returns:
            boolean flag
        """
        params = [svc[S2nKey.PARAM] for svc in cls.all()]
        if param in params:
            return True
        return False

    # ....................
    @classmethod
    def is_valid_service(cls, param, svc):
        """Return a flag indicating if the parameter key is valid for given service.

        Args:
            param: keyword for URL request to a sp_network service
            svc: name of a sp_network service

        Returns:
            boolean flag
        """
        if param is not None:
            val_dict = cls.get_values(param)
            if svc in (val_dict["services"]):
                return True
        return False

    # ....................
    @classmethod
    def get_name_from_param(cls, param):
        """Return a full name of a service for the given service parameter value.

        Args:
            param: keyword for URL request to a sp_network service

        Returns:
            name for the service
        """
        name = None
        if param is not None:
            val_dict = cls.get_values(param)
            name = val_dict[S2nKey.NAME]
        return name

    # ....................
    @classmethod
    def all(cls):
        """Return all available ServiceProviders for the Specify network.

        Returns:
            list of ServiceProviders
        """
        return [
            cls.GBIF, cls.iDigBio, cls.ITISSolr, cls.MorphoSource, cls.WoRMS
        ]

    # ....................
    @classmethod
    def get_icon_url(cls, provider_code, icon_status=None):
        """Get a URL to the badge service with provider param and optionally icon_status.

        Args:
            provider_code: code for provider to get an icon for.
            icon_status: one of APIService.Badge["params"]["icon_status"]["options"]:
                active, inactive, hover

        Returns:
            URL of for the badge API
        """
        root_url = get_host_url()
        if cls.is_valid_service(provider_code, APIEndpoint.Badge):
            endpoint = APIService.Badge["endpoint"]
            url = f"{root_url}/{endpoint}/{provider_code}"
            if icon_status:
                url = f"{url}&icon_status={icon_status}"
        return url
