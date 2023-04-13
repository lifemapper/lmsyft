"""Constants for the Specify Network API services."""
from flask_app.broker.s2n_type import S2nEndpoint, S2nKey

# .............................................................................
CONFIG_DIR = "config"
TEST_SPECIFY7_SERVER = "http://preview.specifycloud.org"
TEST_SPECIFY7_RSS_URL = "{}/export/rss".format(TEST_SPECIFY7_SERVER)
JSON_HEADERS = {"Content-Type": "application/json"}

# For saving Specify7 server URL (used to download individual records)
SPECIFY7_SERVER_KEY = "specify7-server"
SPECIFY7_RECORD_ENDPOINT = "export/record"
SPECIFY_ARK_PREFIX = "http://spcoco.org/ark:/"

URL_ESCAPES = [[" ", r"\%20"], [",", r"\%2C"]]
ENCODING = "utf-8"

DATA_DUMP_DELIMITER = "\t"
GBIF_MISSING_KEY = "unmatched_gbif_ids"

# VALID broker parameter options, must be list
VALID_ICON_OPTIONS = ["active", "inactive", "hover"]

STATIC_DIR = "../../sppy/frontend/static"
ICON_DIR = "{}/icon".format(STATIC_DIR)

TEMPLATE_DIR = "../templates"
SCHEMA_DIR = "{}/schema".format(STATIC_DIR)
SCHEMA_FNAME = "open_api.yaml"

ICON_CONTENT = "image/png"
ICON_API = '/api/v1/badge'


# .............................................................................
class DWC:
    """Constants for the Darwin Core occurrence record standard."""
    QUALIFIER = "dwc:"
    URL = "http://rs.tdwg.org/dwc"
    SCHEMA = "http://rs.tdwg.org/dwc.json"
    RECORD_TITLE = "digital specimen object"


# .............................................................................
class DWCA:
    """Constants for the Darwin Core Archive file standard."""
    NS = "{http://rs.tdwg.org/dwc/text/}"
    META_FNAME = "meta.xml"
    DATASET_META_FNAME = "eml.xml"
    # Meta.xml element/attribute keys
    DELIMITER_KEY = "fieldsTerminatedBy"
    LINE_DELIMITER_KEY = "linesTerminatedBy"
    QUOTE_CHAR_KEY = "fieldsEnclosedBy"
    LOCATION_KEY = "location"
    UUID_KEY = "id"
    FLDMAP_KEY = "fieldname_index_map"
    FLDS_KEY = "fieldnames"
    CORE_FIELDS_OF_INTEREST = [
        "id",
        "institutionCode",
        "collectionCode",
        "datasetName",
        "basisOfRecord",
        "year",
        "month",
        "day"]
    # Human readable
    CORE_TYPE = "{}/terms/Occurrence".format(DWC.URL)


# .............................................................................
class TST_VALUES:
    """Test values for checking project responses."""
    SPECIFY_SOLR_COLLECTION = "spcoco"
    KU_IPT_RSS_URL = "http://ipt.nhm.ku.edu:8080/ipt/rss.do"
    ICH_RSS_URL = "https://ichthyology.specify.ku.edu/export/rss"

    SPECIFY_RSS = "https://ichthyology.specify.ku.edu/export/rss/"
    SPECIFY_URLS = [
        "http://ichthyology.specify.ku.edu/static/depository/export_feed/kui-dwca.zip",
        "http://ichthyology.specify.ku.edu/static/depository/export_feed/kuit-dwca.zip"
    ]
    GUIDS_WO_SPECIFY_ACCESS = [
        "ed8cfa5a-7b47-11e4-8ef3-782bcb9cd5b5",
        "f5725a56-7b47-11e4-8ef3-782bcb9cd5b5",
        "f69696a8-7b47-11e4-8ef3-782bcb9cd5b5",
        "5e7ec91c-4d20-42c4-ad98-8854800e82f7"]
    DS_GUIDS_WO_SPECIFY_ACCESS_RECS = ["e635240a-3cb1-4d26-ab87-57d8c7afdfdb"]
    BAD_GUIDS = [
        "KU :KUIZ:2200", "KU :KUIZ:1663", "KU :KUIZ:1569", "KU :KUIZ:2462",
        "KU :KUIZ:1743", "KU :KUIZ:3019", "KU :KUIZ:1816", "KU :KUIZ:2542",
        "KU :KUIZ:2396"]
    NAMES = [
        "Eucosma raracana",
        "Plagioecia patina",
        "Plagiloecia patina Lamarck, 1816",
        "Plagioecia patina (Lamarck, 1816)",
        "Plagiloecia patana Lemarck",
        "Phlox longifolia Nutt.",
        "Tulipa sylvestris L.",
        "Medinilla speciosa Blume",
        "Acer caesium Wall. ex Brandis",
        "Acer heldreichii Orph. ex Boiss.",
        "Acer pseudoplatanus L.",
        "Acer velutinum Boiss.",
        "Acer hyrcanum Fisch. & Meyer",
        "Acer monspessulanum L.",
        "Acer obtusifolium Sibthorp & Smith",
        "Acer opalus Miller",
        "Acer sempervirens L.",
        "Acer floridanum (Chapm.) Pax",
        "Acer grandidentatum Torr. & Gray",
        "Acer leucoderme Small",
        "Acer nigrum Michx.f.",
        "Acer skutchii Rehder",
        "Acer saccharum Marshall"]
    ITIS_TSNS = [526853, 183671, 182662, 566578]


# .............................................................................
class APIService:
    """Endpoint, parameters, output record format for all Specify Network APIs."""
    Root = {
        "endpoint": S2nEndpoint.Root,
        "params": [],
        S2nKey.RECORD_FORMAT: None
    }
    # Icons for service providers
    Badge = {
        "endpoint": S2nEndpoint.Badge,
        "params": ["provider", "icon_status"],
        S2nKey.RECORD_FORMAT: "image/png"
    }
    # Health for service providers
    Heartbeat = {
        "endpoint": S2nEndpoint.Heartbeat, "params": None,
        S2nKey.RECORD_FORMAT: ""
    }
    # Taxonomic Resolution
    Name = {
        "endpoint": S2nEndpoint.Name,
        "params": [
            "provider", "namestr", "is_accepted", "gbif_parse", "gbif_count", "kingdom"
        ],
        S2nKey.RECORD_FORMAT: ""
    }
    # Specimen occurrence records
    Occurrence = {
        "endpoint": S2nEndpoint.Occurrence,
        "params": ["provider", "occid", "gbif_dataset_key", "count_only"],
        S2nKey.RECORD_FORMAT: ""
    }
    # TODO: Consider an Extension service for Digital Object Architecture
    SpecimenExtension = {
        "endpoint": S2nEndpoint.SpecimenExtension, "params": None,
        S2nKey.RECORD_FORMAT: ""
    }
    Frontend = {
        "endpoint": S2nEndpoint.Frontend,
        "params": ["occid", "namestr"],
        S2nKey.RECORD_FORMAT: ""
    }
    Stats = {
        "endpoint": S2nEndpoint.Stats,
        "params": [],
        S2nKey.RECORD_FORMAT: ""
    }


# .............................................................................
class ServiceProvider:
    """Name and metadata for external Specify Network data providers."""
    Broker = {
        S2nKey.NAME: "Specify Network",
        S2nKey.PARAM: "specifynetwork",
        S2nKey.SERVICES: [S2nEndpoint.Badge],
        # "icon": {"active": "{}/SpNetwork_active.png",
        #          "inactive": "{}/SpNetwork_inactive.png",
        #          "hover": "{}/SpNetwork_hover.png"}
    }
    GBIF = {
        S2nKey.NAME: "GBIF",
        S2nKey.PARAM: "gbif",
        S2nKey.SERVICES: [S2nEndpoint.Occurrence, S2nEndpoint.Name, S2nEndpoint.Badge],
        "icon": {
            "active": "gbif_active-01.png",
            "inactive": "gbif_inactive-01.png",
            "hover": "gbif_hover-01-01.png"
        }
    }
    iDigBio = {
        S2nKey.NAME: "iDigBio",
        S2nKey.PARAM: "idb",
        S2nKey.SERVICES: [S2nEndpoint.Occurrence, S2nEndpoint.Badge],
        "icon": {
            "active": "idigbio_colors_active-01.png",
            "inactive": "idigbio_colors_inactive-01.png",
            "hover": "idigbio_colors_hover-01.png"
        }
    }
    # IPNI = {
    #     S2nKey.NAME: "IPNI",
    #     S2nKey.PARAM: "ipni",
    #     S2nKey.SERVICES: []
    # }
    ITISSolr = {
        S2nKey.NAME: "ITIS",
        S2nKey.PARAM: "itis",
        S2nKey.SERVICES: [S2nEndpoint.Badge, S2nEndpoint.Name],
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
            S2nEndpoint.Badge, S2nEndpoint.Occurrence, S2nEndpoint.SpecimenExtension],
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
        S2nKey.SERVICES: [S2nEndpoint.Badge, S2nEndpoint.Name],
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
                ServiceProvider.GBIF[S2nKey.NAME], ServiceProvider.GBIF[S2nKey.PARAM]
        ):
            return ServiceProvider.GBIF
        elif param_or_name in (
                ServiceProvider.iDigBio[S2nKey.NAME],
                ServiceProvider.iDigBio[S2nKey.PARAM]
        ):
            return ServiceProvider.iDigBio
        # elif param_or_name in (
        #         ServiceProvider.IPNI[S2nKey.NAME], ServiceProvider.IPNI[S2nKey.PARAM]
        # ):
        #     return ServiceProvider.IPNI
        elif param_or_name in (
                ServiceProvider.ITISSolr[S2nKey.NAME],
                ServiceProvider.ITISSolr[S2nKey.PARAM]
        ):
            return ServiceProvider.ITISSolr
        elif param_or_name in (
                ServiceProvider.MorphoSource[S2nKey.NAME],
                ServiceProvider.MorphoSource[S2nKey.PARAM]
        ):
            return ServiceProvider.MorphoSource
        elif param_or_name in (
                ServiceProvider.WoRMS[S2nKey.NAME], ServiceProvider.WoRMS[S2nKey.PARAM]
        ):
            return ServiceProvider.WoRMS
        elif param_or_name in (
                ServiceProvider.Broker[S2nKey.NAME],
                ServiceProvider.Broker[S2nKey.PARAM]
        ):
            return ServiceProvider.Broker
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
            val_dict = ServiceProvider.get_values(param)
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
            val_dict = ServiceProvider.get_values(param)
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
            ServiceProvider.GBIF, ServiceProvider.iDigBio,
            # ServiceProvider.IPNI,
            ServiceProvider.ITISSolr, ServiceProvider.MorphoSource,
            ServiceProvider.WoRMS, ServiceProvider.Broker]


# .............................................................................
BrokerParameters = {
    "provider": {
        "type": "", "default": None, "options": [
            ServiceProvider.GBIF[S2nKey.PARAM],
            ServiceProvider.iDigBio[S2nKey.PARAM],
            ServiceProvider.ITISSolr[S2nKey.PARAM],
            ServiceProvider.MorphoSource[S2nKey.PARAM],
            ]
        },
    "namestr": {"type": "", "default": None},
    "is_accepted": {"type": False, "default": False},
    "gbif_parse": {"type": False, "default": False},
    "gbif_count": {"type": False, "default": False},
    "itis_match": {"type": False, "default": False},
    "kingdom": {"type": "", "default": None},
    "occid": {"type": "", "default": None},
    "gbif_dataset_key": {"type": "", "default": None},
    "count_only": {"type": False, "default": False},
    "url": {"type": "", "default": None},
    "icon_status": {
        "type": "",
        "options": VALID_ICON_OPTIONS,
        "default": None}
    }


# ......................................................
class MorphoSource:
    """MorphoSource constants enumeration."""
    REST_URL = "https://ms1.morphosource.org/api/v1"
    VIEW_URL = "https://www.morphosource.org/concern/biological_specimens"
    NEW_VIEW_URL = "https://www.morphosource.org/catalog/objects"
    NEW_API_URL = "https://www.morphosource.org/catalog/objects.json"
    # FROZEN_URL = "https://ea-boyerlab-morphosource-01.oit.duke.edu/api/v1"
    DWC_ID_FIELD = "specimen.occurrence_id"
    LOCAL_ID_FIELD = "specimen.specimen_id"
    OCC_RESOURCE = "specimens"
    MEDIA_RESOURCE = "media"
    OTHER_RESOURCES = ["taxonomy", "projects", "facilities"]
    COMMAND = "find"
    OCCURRENCEID_KEY = "occurrence_id"
    TOTAL_KEY = "totalResults"
    RECORDS_KEY = "results"
    LIMIT = 1000
    RECORD_FORMAT = "https://www.morphosource.org/About/API"

    @classmethod
    def get_occurrence_view(cls, local_id):
        """Get the URL for viewing a MorphoSource record.

        Args:
             local_id: string for the MorpoSource identifier of the record

        Returns:
            url: for webpage displaying the record

        Note:
            Example:
            https://www.morphosource.org/concern/biological_specimens/000S27385
        """
        url = None
        if local_id:
            idtail = "S{}".format(local_id)
            leading_zero_count = (9 - len(idtail))
            prefix = "0" * leading_zero_count
            url = f"{MorphoSource.VIEW_URL}/{prefix}{idtail}"
        return url

    @classmethod
    def get_occurrence_data(cls, occurrence_id):
        """Get the URL for returning a MorphoSource record as parsable data.

        Args:
             occurrence_id: string for the MorpoSource identifier of the record

        Returns:
            url: for API call for the record
        """
        url = None
        if occurrence_id:
            url = "{}/find/specimens?start=0&limit=1000&q=occurrence_id%3A{}".format(
                MorphoSource.REST_URL, occurrence_id)
        return url


# ......................................................
class SPECIFY:
    """Specify constants enumeration."""
    DATA_DUMP_DELIMITER = "\t"
    RECORD_FORMAT = "http://rs.tdwg.org/dwc.json"
    RESOLVER_COLLECTION = "spcoco"


# ......................................................
class GBIF:
    """GBIF constants enumeration."""
    DATA_DUMP_DELIMITER = "\t"
    TAXON_KEY = "specieskey"
    TAXON_NAME = "sciname"
    PROVIDER = "puborgkey"
    OCC_ID_FIELD = "gbifID"
    SPECIES_ID_FIELD = "usageKey"
    WAIT_TIME = 180
    LIMIT = 300
    VIEW_URL = "https://www.gbif.org"
    REST_URL = "https://api.gbif.org/v1"
    QUALIFIER = "gbif:"

    SPECIES_SERVICE = "species"
    PARSER_SERVICE = "parser/name"
    OCCURRENCE_SERVICE = "occurrence"
    DATASET_SERVICE = "dataset"
    ORGANIZATION_SERVICE = "organization"

    COUNT_KEY = "count"
    RECORDS_KEY = "results"
    RECORD_FORMAT_NAME = "https://www.gbif.org/developer/species"
    RECORD_FORMAT_OCCURRENCE = "https://www.gbif.org/developer/occurrence"

    TAXONKEY_FIELD = "specieskey"
    TAXONNAME_FIELD = "sciname"
    PROVIDER_FIELD = "puborgkey"
    ID_FIELD = "gbifid"

    ACCEPTED_NAME_KEY = "accepted_name"
    SEARCH_NAME_KEY = "search_name"
    SPECIES_KEY_KEY = "speciesKey"
    SPECIES_NAME_KEY = "species"
    TAXON_ID_KEY = "taxon_id"

    REQUEST_SIMPLE_QUERY_KEY = "q"
    REQUEST_NAME_QUERY_KEY = "name"
    REQUEST_TAXON_KEY = "TAXON_KEY"
    REQUEST_RANK_KEY = "rank"
    REQUEST_DATASET_KEY = "dataset_key"

    DATASET_BACKBONE_VALUE = "GBIF Backbone Taxonomy"
    DATASET_BACKBONE_KEY = "d7dddbf4-2cf0-4f39-9b2a-bb099caae36c"

    SEARCH_COMMAND = "search"
    COUNT_COMMAND = "count"
    MATCH_COMMAND = "match"
    DOWNLOAD_COMMAND = "download"
    DOWNLOAD_REQUEST_COMMAND = "request"
    RESPONSE_NOMATCH_VALUE = "NONE"

    NameMatchFieldnames = [
        "scientificName", "kingdom", "phylum", "class", "order", "family",
        "genus", "species", "rank", "genusKey", "speciesKey", "usageKey",
        "canonicalName", "confidence"]

    # For writing files from GBIF DarwinCore download,
    # DWC translations in lmCompute/code/sdm/gbif/constants
    # We are adding the 2 fields: LM_WKT_FIELD and LINK_FIELD
    LINK_FIELD = "gbifurl"
    # Ends in / to allow appending unique id

    @classmethod
    def species_url(cls):
        """Get the base URL for viewing a GBIF taxonomic records.

        Returns:
            url: for webpage displaying a search interface to the records
        """
        return "{}/{}".format(GBIF.VIEW_URL, GBIF.SPECIES_SERVICE)

    @classmethod
    def get_occurrence_view(cls, key):
        """Get the URL for viewing a GBIF occurrence record.

        Args:
            key: gbifID for the occurrence record to view

        Returns:
            url: for webpage displaying a GBIF occurrence record.
        """
        url = None
        if key:
            url = "{}/{}/{}".format(GBIF.VIEW_URL, GBIF.OCCURRENCE_SERVICE, key)
        return url

    @classmethod
    def get_occurrence_data(cls, key):
        """Get the URL for returning a GBIF occurrence record.

        Args:
            key: gbifID for the occurrence record to retrieve.

        Returns:
            url: for API call to retrieve a GBIF occurrence record.
        """
        url = None
        if key:
            url = "{}/{}/{}".format(GBIF.REST_URL, GBIF.OCCURRENCE_SERVICE, key)
        return url

    @classmethod
    def get_species_view(cls, key):
        """Get the URL for viewing a GBIF taxon record.

        Args:
            key: taxonKey for the taxon record to view

        Returns:
            url: for webpage displaying a GBIF species record.
        """
        url = None
        if key:
            url = "{}/{}/{}".format(GBIF.VIEW_URL, GBIF.SPECIES_SERVICE, key)
        return url

    @classmethod
    def get_species_data(cls, key):
        """Get the URL for returning a GBIF taxonomic/species record.

        Args:
            key: taxonKey for the taxonomic record to retrieve.

        Returns:
            url: for API call to retrieve a GBIF species record.
        """
        url = None
        if key:
            url = "{}/{}/{}".format(GBIF.REST_URL, GBIF.SPECIES_SERVICE, key)
        return url


# .............................................................................
class WORMS:
    """World Register of Marine Species, WoRMS, constants enumeration.

    Notes:
        http://www.marinespecies.org/rest/AphiaRecordsByMatchNames
        ?scientificnames[]=Plagioecia%20patina&marine_only=false
    """
    REST_URL = "http://www.marinespecies.org/rest"
    NAME_MATCH_SERVICE = "AphiaRecordsByMatchNames"
    NAME_SERVICE = "AphiaNameByAphiaID"
    MATCH_PARAM = "scientificnames[]="
    ID_FLDNAME = "valid_AphiaID"

    @classmethod
    def get_species_data(cls, key):
        """Get the URL for returning a WORMS taxonomic/species record.

        Args:
            key: identifier for the taxonomic record to retrieve.

        Returns:
            url: for API call to retrieve a GBIF species record.
        """
        url = None
        if key:
            url = "{}/{}/{}".format(WORMS.REST_URL, WORMS.NAME_SERVICE, key)
        return url


#
# class IPNI:
#     """IPNI constants enumeration."""
#     base_url = "http://beta.ipni.org/api/1"


# .............................................................................
class ITIS:
    """ITIS constants enumeration.

    Notes:
        http://www.itis.gov/ITISWebService/services/ITISService/getAcceptedNamesFromTSN
        ?tsn=183671

    TODO: for JSON output use jsonservice instead of ITISService
    """
    DATA_NAMESPACE = "{http://data.itis_service.itis.usgs.gov/xsd}"
    NAMESPACE = "{http://itis_service.itis.usgs.gov}"
    VIEW_URL = "https://www.itis.gov/servlet/SingleRpt/SingleRpt"
    # ...........
    # Solr Services
    SOLR_URL = "https://services.itis.gov"
    TAXONOMY_HIERARCHY_QUERY = "getFullHierarchyFromTSN"
    VERNACULAR_QUERY = "getCommonNamesFromTSN"
    NAMES_FROM_TSN_QUERY = "getAcceptedNamesFromTSN"
    RECORD_FORMAT = "https://www.itis.gov/solr_documentation.html"
    COUNT_KEY = "numFound"
    RECORDS_KEY = "docs"
    # ...........
    # Web Services
    WEBSVC_URL = "http://www.itis.gov/ITISWebService/services/ITISService"
    JSONSVC_URL = "https://www.itis.gov/ITISWebService/jsonservice"
    # wildcard matching
    ITISTERMS_FROM_SCINAME_QUERY = "getITISTermsFromScientificName"
    SEARCH_KEY = "srchKey"
    # JSON return tags
    TSN_KEY = "tsn"
    NAME_KEY = "nameWInd"
    HIERARCHY_KEY = "hierarchySoFarWRanks"
    HIERARCHY_TAG = "hierarchyList"
    RANK_TAG = "rankName"
    TAXON_TAG = "taxonName"
    KINGDOM_KEY = "Kingdom"
    PHYLUM_DIVISION_KEY = "Division"
    CLASS_KEY = "Class"
    ORDER_KEY = "Order"
    FAMILY_KEY = "Family"
    GENUS_KEY = "Genus"
    SPECIES_KEY = "Species"
    URL_ESCAPES = [[" ", r"\%20"]]

    @classmethod
    def get_taxon_view(cls, tsn):
        """Get the URL for viewing a ITIS taxon record.

        Args:
            tsn: identifier for the taxon record to view

        Returns:
            url: for webpage displaying a ITIS taxon record.
        """
        return "{}?search_topic=TSN&search_value={}".format(ITIS.VIEW_URL, tsn)

    @classmethod
    def get_taxon_data(cls, tsn):
        """Get the URL for returning an ITIS taxon record.

        Args:
            tsn: identifier for the taxonomic record to retrieve.

        Returns:
            url: for API call to retrieve an ITIS taxon record.
        """
        return "{}?q=tsn:{}".format(ITIS.SOLR_URL, tsn)


# .............................................................................
# .                           iDigBio constants                               .
# .............................................................................
class Idigbio:
    """iDigBio constants enumeration."""
    NAMESPACE_URL = ""
    NAMESPACE_ABBR = "gbif"
    VIEW_URL = "https://www.idigbio.org/portal/records"
    REST_URL = "https://search.idigbio.org/v2/view/records"
    # LINK_PREFIX = "https://www.idigbio.org/portal/records/"
    SEARCH_PREFIX = "https://search.idigbio.org/v2"
    SEARCH_POSTFIX = "search"
    COUNT_POSTFIX = "summary/count"
    OCCURRENCE_POSTFIX = "records"
    PUBLISHERS_POSTFIX = "publishers"
    RECORDSETS_POSTFIX = "recordsets"
    SEARCH_LIMIT = 5000
    ID_FIELD = "uuid"
    OCCURRENCEID_FIELD = "occurrenceid"
    LINK_FIELD = "idigbiourl"
    GBIFID_FIELD = "taxonid"
    BINOMIAL_REGEX = "(^[^ ]*) ([^ ]*)$"
    RECORD_CONTENT_KEY = "data"
    RECORD_INDEX_KEY = "indexTerms"
    QUALIFIER = "idigbio:"
    QKEY = "rq"
    QFILTERS = {"basisofrecord": "preservedspecimen"}
    FILTERS = {"limit": 5000,
               "offset": 0,
               "no_attribution": False}
    COUNT_KEY = "itemCount"
    RECORDS_KEY = "items"
    RECORD_FORMAT = "https://github.com/idigbio/idigbio-search-api/wiki"

    @classmethod
    def get_occurrence_view(cls, uuid):
        """Get the URL for viewing an iDigBio occurrence record.

        Args:
            uuid: identifier for the occurrence record to view.

        Returns:
            url: for webpage for an iDigBio occurrence record.
        """
        url = None
        if uuid:
            url = "{}/{}".format(Idigbio.VIEW_URL, uuid)
        return url

    @classmethod
    def get_occurrence_data(cls, uuid):
        """Get the URL for returning an iDigBio occurrence record.

        Args:
            uuid: identifier for the occurrence record to retrieve.

        Returns:
            url: for API call to retrieve an iDigBio occurrence record.
        """
        url = None
        if uuid:
            url = "{}/{}".format(Idigbio.REST_URL, uuid)
        return url


ISSUE_DEFINITIONS = {
    "common": {
        "HAS_MULTIPLE_RECORDS":
            "Occurrence Tentacle server found duplicate instances of this record",
        },
    "gbif": {
        "AMBIGUOUS_COLLECTION":
            "The given collection matches with more than 1 GrSciColl collection.",
        "AMBIGUOUS_INSTITUTION":
            "The given institution matches with more than 1 GrSciColl institution.",
        "BASIS_OF_RECORD_INVALID":
            "The given basis of record is impossible to interpret or significantly "
            "different from the recommended vocabulary.",
        "COLLECTION_MATCH_FUZZY":
            "The given collection was fuzzily matched to a GrSciColl collection.",
        "COLLECTION_MATCH_NONE":
            "The given collection couldn't be matched with any GrSciColl collection.",
        "CONTINENT_COUNTRY_MISMATCH":
            "The interpreted continent and country do not match.",
        "CONTINENT_DERIVED_FROM_COORDINATES":
            "The interpreted continent is based on the coordinates, not the verbatim "
            "string information.",
        "CONTINENT_INVALID": "Uninterpretable continent values found.",
        "COORDINATE_ACCURACY_INVALID": "Deprecated. ",
        "COORDINATE_INVALID":
            "Coordinate value is given in some form but GBIF is unable to interpret "
            "it.",
        "COORDINATE_OUT_OF_RANGE":
            "Coordinate has a latitude and/or longitude value beyond the maximum (or "
            "minimum) decimal value.",
        "COORDINATE_PRECISION_INVALID":
            "Indicates an invalid or very unlikely coordinatePrecision",
        "COORDINATE_PRECISION_UNCERTAINTY_MISMATCH": "Deprecated. ",
        "COORDINATE_REPROJECTED":
            "The original coordinate was successfully reprojected from a different "
            "geodetic datum to WGS84.",
        "COORDINATE_REPROJECTION_FAILED":
            "The given decimal latitude and longitude could not be reprojected to "
            "WGS84 based on the provided datum.",
        "COORDINATE_REPROJECTION_SUSPICIOUS":
            "Indicates successful coordinate reprojection according to provided datum, "
            "but which results in a datum shift larger than 0.1 decimal degrees.",
        "COORDINATE_ROUNDED":
            "Original coordinate modified by rounding to 5 decimals.",
        "COORDINATE_UNCERTAINTY_METERS_INVALID":
            "Indicates an invalid or very unlikely dwc:uncertaintyInMeters.",
        "COUNTRY_COORDINATE_MISMATCH":
            "The interpreted occurrence coordinates fall outside of the indicated "
            "country.",
        "COUNTRY_DERIVED_FROM_COORDINATES":
            "The interpreted country is based on the coordinates, not the verbatim "
            "string information.",
        "COUNTRY_INVALID": "Uninterpretable country values found.",
        "COUNTRY_MISMATCH":
            "Interpreted country for dwc:country and dwc:countryCode contradict "
            "each other.",
        "DEPTH_MIN_MAX_SWAPPED": "Set if supplied minimum depth > maximum depth",
        "DEPTH_NON_NUMERIC": "Set if depth is a non-numeric value",
        "DEPTH_NOT_METRIC":
            "Set if supplied depth is not given in the metric system, for example "
            "using feet instead of meters",
        "DEPTH_UNLIKELY": "Set if depth is larger than 11,000m or negative.",
        "ELEVATION_MIN_MAX_SWAPPED":
            "Set if supplied minimum elevation > maximum elevation",
        "ELEVATION_NON_NUMERIC": "Set if elevation is a non-numeric value",
        "ELEVATION_NOT_METRIC":
            "Set if supplied elevation is not given in the metric system, for example "
            "using feet instead of meters",
        "ELEVATION_UNLIKELY":
            "Set if elevation is above the troposphere (17km) or below 11km "
            "(Mariana Trench).",
        "GEODETIC_DATUM_ASSUMED_WGS84":
            "Indicating that the interpreted coordinates assume they are based on "
            "WGS84 datum as the datum was either "
            "not indicated or interpretable.",
        "GEODETIC_DATUM_INVALID": "The geodetic datum given could not be interpreted.",
        "GEOREFERENCED_DATE_INVALID":
            "The date given for dwc:georeferencedDate is invalid and can't be "
            "interpreted at all.",
        "GEOREFERENCED_DATE_UNLIKELY":
            "The date given for dwc:georeferencedDate is in the future or before "
            "Linnean times (1700).",
        "IDENTIFIED_DATE_INVALID":
            "The date given for dwc:dateIdentified is invalid and can't be interpreted "
            "at all.",
        "IDENTIFIED_DATE_UNLIKELY":
            "The date given for dwc:dateIdentified is in the future or before Linnean "
            "times (1700).",
        "INDIVIDUAL_COUNT_CONFLICTS_WITH_OCCURRENCE_STATUS":
            "Example: individual count value > 0, but occurrence status is absent.",
        "INDIVIDUAL_COUNT_INVALID":
            "The individual count value is not a positive integer",
        "INSTITUTION_COLLECTION_MISMATCH":
            "The collection matched doesn't belong to the institution matched.",
        "INSTITUTION_MATCH_FUZZY":
            "The given institution was fuzzily matched to a GrSciColl institution.",
        "INSTITUTION_MATCH_NONE":
            "The given institution couldn't be matched with any GrSciColl institution.",
        "INTERPRETATION_ERROR":
            "An error occurred during interpretation, leaving the record "
            "interpretation incomplete.",
        "MODIFIED_DATE_INVALID":
            "A (partial) invalid date is given for dc:modified, such as a nonexistent "
            "date, zero month, etc.",
        "MODIFIED_DATE_UNLIKELY":
            "The date given for dc:modified is in the future or predates Unix "
            "time (1970).",
        "MULTIMEDIA_DATE_INVALID":
            "An invalid date is given for dc:created of a multimedia object.",
        "MULTIMEDIA_URI_INVALID": "An invalid URI is given for a multimedia object.",
        "OCCURRENCE_STATUS_INFERRED_FROM_BASIS_OF_RECORD":
            "Occurrence status was inferred from basis of records",
        "OCCURRENCE_STATUS_INFERRED_FROM_INDIVIDUAL_COUNT":
            "Occurrence status was inferred from the individual count value",
        "OCCURRENCE_STATUS_UNPARSABLE":
            "Occurrence status value can't be assigned to OccurrenceStatus",
        "POSSIBLY_ON_LOAN":
            "The given owner institution is different than the given institution.",
        "PRESUMED_NEGATED_LATITUDE": "Latitude appears to be negated, e.g.",
        "PRESUMED_NEGATED_LONGITUDE": "Longitude appears to be negated, e.g.",
        "PRESUMED_SWAPPED_COORDINATE": "Latitude and longitude appear to be swapped.",
        "RECORDED_DATE_INVALID":
            "A (partial) invalid date is given, such as a non existing date, zero "
            "month, etc.",
        "RECORDED_DATE_MISMATCH":
            "The recorded date specified as the eventDate string and the individual "
            "year, month, day are contradictory.",
        "RECORDED_DATE_UNLIKELY":
            "The recorded date is highly unlikely, falling either into the future or "
            "representing a very old date before 1600 thus predating modern taxonomy.",
        "REFERENCES_URI_INVALID": "An invalid URI is given for dc:references.",
        "TAXON_MATCH_FUZZY":
            "Matching to the taxonomic backbone can only be done using a fuzzy, non "
            "exact match.",
        "TAXON_MATCH_HIGHERRANK":
            "Matching to the taxonomic backbone can only be done on a higher rank and "
            "not the scientific name.",
        "TAXON_MATCH_NONE":
            "Matching to the taxonomic backbone cannot be done because there was no "
            "match at all, or several matches with too little information to keep them "
            "apart (potentially homonyms).",
        "TYPE_STATUS_INVALID":
            "The given type status is impossible to interpret or significantly "
            "different from the recommended vocabulary.",
        "ZERO_COORDINATE":
            "Coordinate is the exact 0°, 0° coordinate, often indicating a bad null "
            "coordinate.",
        },
    "idb": {
        "datecollected_bounds":
            "Date Collected out of bounds (Not between 1700-01-02 and the date of "
            "Indexing). Date Collected is generally composed from dwc:year, dwc:month, "
            "dwc:day or as specified in dwc:eventDate.",
        "dwc_acceptednameusageid_added":
            "Accepted Name Usage ID (dwc:acceptedNameUsageID) added where none was "
            "provided.",
        "dwc_basisofrecord_invalid":
            "Darwin Core Basis of Record (dwc:basisOfRecord) missing or not a value "
            "from controlled vocabulary.",
        "dwc_basisofrecord_paleo_conflict":
            "Darwin Core Basis of Record (dwc:basisOfRecord) is not FossilSpecimen but "
            "the record contains paleo context terms",
        "dwc_basisofrecord_removed":
            "Darin Core Basis of Record (dwc:basisOfRecord) removed because of invalid "
            "value.",
        "dwc_class_added":
            "Darwin Core Class (dwc:class) added where none was provided.",
        "dwc_class_replaced":
            "Darwin Core Class (dwc:class) replaced with a standardized value from "
            "GBIF Backbone Taxonomy.",
        "dwc_continent_added":
            "Darwin Core Continent (dwc:continent) added where none was provided.",
        "dwc_continent_replaced":
            "Darwin Core Continent (dwc:continent) replaced with a standardized value.",
        "dwc_country_added":
            "Darwin Core Country (dwc:country) added where none was provided.",
        "dwc_country_replaced":
            "Darwin Core Country (dwc:country) replaced with a standardized value from "
            "Getty Thesaurus of Geographic Names.",
        "dwc_datasetid_added": "Darwin Core Dataset ID (dwc:datasetID) added where "
                               "none was provided.",
        "dwc_datasetid_replaced":
            "Darwin Core Dataset ID (dwc:datasetID) replaced with value from ? TBD",
        "dwc_family_added":
            "Darwin Core Family (dwc:family) added where none was provided.",
        "dwc_family_replaced":
            "Darwin Core Family (dwc:family) replaced with a standardized value from "
            "GBIF Backbone Taxonomy.",
        "dwc_genus_added":
            "Darwin Core Genus (dwc:genus) added where none was provided.",
        "dwc_genus_replaced":
            "Darwin Core Genus (dwc:genus) replaced with a standardized value from "
            "GBIF Backbone Taxonomy.",
        "dwc_infraspecificepithet_added":
            "Darwin Core Infraspecific Epithet (dwc:infraspecificEpithet) added where "
            "none was provided.",
        "dwc_infraspecificepithet_replaced":
            "Darwin Core Infraspecific Epithet (dwc:infraspecificEpithet) replaced "
            "with a standardized value from GBIF Backbone Taxonomy.",
        "dwc_kingdom_added":
            "Darwin Core Kingdom (dwc:kingdom) added where none was provided.",
        "dwc_kingdom_replaced":
            "Darwin Core Kingdom (dwc:kingdom) replaced with a standardized value from "
            "GBIF Backbone Taxonomy.",
        "dwc_kingdom_suspect":
            "Darwin Core Kingdom (dwc:kingdom) not replaced with a standardized value "
            "from GBIF Backbone Taxonomy due to insufficient confidence level.",
        "dwc_multimedia_added": "TBD",
        "dwc_order_added":
            "Darwin Core Order (dwc:order) added where none was provided.",
        "dwc_order_replaced":
            "Darwin Core Order (dwc:order) replaced with a standardized value from "
            "GBIF Backbone Taxonomy.",
        "dwc_originalnameusageid_added":
            "Darwin Core Original Name Usage ID (dwc:originalNameUsageID) added where "
            "none was provided.",
        "dwc_parentnameusageid_added":
            "Darwin Core Parent Name Usage ID (dwc:parentNameUsageID) added where none "
            "was provided.",
        "dwc_phylum_added":
            "Darwin Core Phylum (dwc:phylum) added where none was provided.",
        "dwc_phylum_replaced":
            "Darwin Core Phylum (dwc:phylum) replaced with a standardized value from "
            "GBIF Backbone Taxonomy.",
        "dwc_scientificnameauthorship_added":
            "Darwin Core Scientific Name Authorship (dwc:scientificNameAuthorship) "
            "added where none was provided.",
        "*** dwc_scientificnameauthorship_replaced":
            "Darwin Core Scientific Name Authorship (dwc:scientificNameAuthorship) "
            "replaced with a standardized value from GBIF Backbone Taxonomy.",
        "dwc_specificepithet_added":
            "Darwin Core Specific Epithet (dwc:specificEpithet) added where none was "
            "provided.",
        "dwc_specificepithet_replaced":
            "Darwin Core Specific Epithet (dwc:specificEpithet) replaced with a "
            "standardized value from GBIF Backbone Taxonomy.",
        "dwc_stateprovince_replaced":
            "Darwin Core State or Province (dwc:stateProvince) replaced with a "
            "standardized value.",
        "dwc_taxonid_added":
            "Darwin Core Taxon ID (dwc:taxonID) added where none was provided.",
        "dwc_taxonid_replaced":
            "Darwin Core Taxon ID (dwc:taxonID) replaced with a standardized value "
            "from GBIF Backbone Taxonomy.",
        "dwc_taxonomicstatus_added":
            "Darwin Core Taxonomic Status (dwc:taxonomicStatus) added where none "
            "was provided.",
        "dwc_taxonomicstatus_replaced":
            "Darwin Core Taxonomic Status (dwc:taxonomicStatus) replaced with a "
            "standardized value from GBIF Backbone Taxonomy.",
        "dwc_taxonrank_added":
            "Darwin Core Taxon Rank (dwc:taxonRank) added where none was provided.",
        "dwc_taxonrank_invalid":
            "The supplied Darwin Core Taxon Rank (dwc:taxonRank) is not contained in "
            "controlled vocabulary (Taxonomic Rank GBIF Vocabulary).",
        "dwc_taxonrank_removed":
            "Darwin Core Taxon Rank (dwc:taxonRank) removed because it is not "
            "contained in controlled vocabulary (Taxonomic Rank GBIF Vocabulary).",
        "dwc_taxonrank_replaced":
            "Darwin Core Taxon Rank (dwc:taxonRank) replaced with a standardized value "
            "from GBIF Backbone Taxonomy.",
        "dwc_taxonremarks_added":
            "Darwin Core Taxon Remarks (dwc:taxonRemarks) added none was provided.",
        "dwc_taxonremarks_replaced":
            "Darwin Core Taxon Remarks (dwc:taxonRemarks) replaced with a standardized "
            "value from GBIF Backbone Taxonomy.",
        "gbif_canonicalname_added":
            "GBIF Canonical Name added from GBIF Backbone Taxonomy.",
        "gbif_genericname_added":
            "GBIF Generic Name added from GBIF Backbone Taxonomy.",
        "gbif_reference_added":
            "GBIF Reference added from GBIF Backbone Taxonomy",
        "gbif_taxon_corrected":
            "A match in GBIF Backbone Taxonomy was found. Inverse of "
            "taxon_match_failed flag.",
        "gbif_vernacularname_added": "GBIF Vernacular Name (common name) added.",
        "geopoint_0_coord": "Geographic Coordinate contains literal '0' values.",
        "geopoint_bounds":
            "Geographic Coordinate out of bounds (valid range is -90 to 90 lat, "
            "-180 to 180 long)",
        "geopoint_datum_error":
            "Geographic Coordinate Datum (dwc:geodeticDatum) is Unknown or coordinate "
            "cannot be converted to WGS84.",
        "geopoint_datum_missing":
            "Geographic Coordinate is missing Geodetic Datum (dwc:geodeticDatum) "
            "(Assumed to be WGS84).",
        "geopoint_low_precision": "Geographic Coordinate contains a Low Precision value.",
        "geopoint_pre_flip":
            "Geographic Coordinate latitude and longitude replaced with swapped "
            "values. Prior to examining other factors, the magnitude of latitude was "
            "determined to be greater than 180, and the longitude was less than 90.",
        "geopoint_similar_coord":
            "Geographic Coordinate latitude and longitude are similar "
            "(+/- lat == +/- lon) and likely have data entry issue.",
        "idigbio_isocountrycode_added": "iDigBio ISO 3166-1 alpha-3 Country Code added.",
        "rev_geocode_both_sign":
            "Geographic Coordinate Latitude and Longitude negated to place point in "
            "correct country.",
        "rev_geocode_corrected":
            "Geographic Coordinate placed within stated country by reverse geocoding "
            "process.",
        "rev_geocode_eez":
            "Geographic Coordinate is outside land boundaries of stated country but "
            "does fall inside the country's exclusive economic zone water boundary "
            "(approx. 200 miles from shore) based on reverse geocoding process.",
        "rev_geocode_eez_corrected":
            "The reverse geocoding process was able to find a coordinate operation "
            "that placed the point within the stated country's exclusive economic "
            "zone.",
        "rev_geocode_failure":
            "Geographic Coordinate could not be reverse geocoded to a particular "
            "country.",
        "rev_geocode_flip":
            "Geographic Coordinate Latitude and Longitude replaced with swapped values "
            "to place point in stated country by reverse geocoding process.",
        "rev_geocode_flip_both_sign":
            "Geographic Coordinate Latitude and Longitude replaced with both swapped "
            "and negated values to place point in stated country by reverse geocoding process.",
        "rev_geocode_flip_lat_sign":
            "Geographic Coordinate Latitude and Longitude replaced with swapped "
            "values, Latitude negated, to place point in stated country by reverse "
            "geocoding process.",
        "rev_geocode_flip_lon_sign":
            "Geographic Coordinate Latitude and Longitude replaced with swapped "
            "values, Longitude negated, to place it in stated country by reverse "
            "geocoding process.",
        "rev_geocode_lat_sign":
            "Geographic Coordinate Latitude negated to place point in stated country "
            "by reverse geocoding process.",
        "rev_geocode_lon_sign":
            "Geographic Coordinate had its Longitude negated to place it in stated "
            "country.",
        "rev_geocode_mismatch":
            "Geographic Coordinate did not reverse geocode to stated country.",
        "scientificname_added":
            "Scientific Name (dwc:scientificName) added where none was provided with "
            "the value constructed by concatenation of stated genus and species.",
        "taxon_match_failed":
            "Unable to match a taxon in GBIF Backbone Taxonomy. Inverse of "
            "gbif_taxon_corrected flag."
    }
}


"""
http://preview.specifycloud.org/static/depository/export_feed/kui-dwca.zip
http://preview.specifycloud.org/static/depository/export_feed/kuit-dwca.zip
"""
