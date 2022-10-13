# from collections import OrderedDict
from lmtrex.common.s2n_type import S2nEndpoint, S2nKey

# .............................................................................
# hierarchySoFarWRanks <class 'list'>: ['41107:$Kingdom:Plantae$Subkingdom:Viridiplantae$Infrakingdom:Streptophyta$Superdivision:Embryophyta$Division:Tracheophyta$Subdivision:Spermatophytina$Class:Magnoliopsida$Superorder:Lilianae$Order:Poales$Family:Poaceae$Genus:Poa$Species:Poa annua$']
# hierarchyTSN <class 'list'>: ['$202422$954898$846494$954900$846496$846504$18063$846542$846620$40351$41074$41107$']
CONFIG_DIR = 'config'
TEST_SPECIFY7_SERVER = 'http://preview.specifycloud.org'
TEST_SPECIFY7_RSS_URL = '{}/export/rss'.format(TEST_SPECIFY7_SERVER)

# Always point to production Syftorium
SYFT_BASE = 'https://syftorium.org'
    
ICON_API = '/api/v1/badge'
SPECIFY_CACHE_API = '{}/api/v1/sp_cache'.format(SYFT_BASE)

# For saving Specify7 server URL (used to download individual records)
SPECIFY7_SERVER_KEY = 'specify7-server'
SPECIFY7_RECORD_ENDPOINT = 'export/record'
SPECIFY_ARK_PREFIX = 'http://spcoco.org/ark:/'

DATA_DUMP_DELIMITER = '\t'
GBIF_MISSING_KEY = 'unmatched_gbif_ids'

# VALID broker parameter options, must be list
VALID_MAP_REQUESTS = ['getmap', 'getlegendgraphic']
VALID_ICON_OPTIONS = ['active', 'inactive', 'hover']

STATIC_DIR='../frontend/static'
ICON_DIR='{}/icon'.format(STATIC_DIR)

TEMPLATE_DIR = '../templates'
SCHEMA_DIR='{}/schema'.format(STATIC_DIR)
SCHEMA_FNAME = 'open_api.yaml'

ICON_CONTENT = 'image/png'
    
# .............................................................................
class DWC:
    QUALIFIER = 'dwc:'
    URL = 'http://rs.tdwg.org/dwc'
    SCHEMA = 'http://rs.tdwg.org/dwc.json'
    RECORD_TITLE = 'digital specimen object'

# .............................................................................
class DWCA:
    NS = '{http://rs.tdwg.org/dwc/text/}'
    META_FNAME = 'meta.xml'
    DATASET_META_FNAME = 'eml.xml'
    # Meta.xml element/attribute keys
    DELIMITER_KEY = 'fieldsTerminatedBy'
    LINE_DELIMITER_KEY = 'linesTerminatedBy'
    QUOTE_CHAR_KEY = 'fieldsEnclosedBy'
    LOCATION_KEY = 'location'
    UUID_KEY = 'id'
    FLDMAP_KEY = 'fieldname_index_map'
    FLDS_KEY = 'fieldnames'
    CORE_FIELDS_OF_INTEREST = [
        'id',
        'institutionCode',
        'collectionCode',
        'datasetName',
        'basisOfRecord',
        'year',
        'month',
        'day']
    # Human readable
    CORE_TYPE = '{}/terms/Occurrence'.format(DWC.URL)

JSON_HEADERS = {'Content-Type': 'application/json'}


# .............................................................................
class TST_VALUES:
    SPECIFY_SOLR_COLLECTION = 'spcoco'
    KU_IPT_RSS_URL = 'http://ipt.nhm.ku.edu:8080/ipt/rss.do'
    ICH_RSS_URL = 'https://ichthyology.specify.ku.edu/export/rss'

    SPECIFY_RSS = 'https://ichthyology.specify.ku.edu/export/rss/'
    SPECIFY_URLS = [
    'https://ichthyology.specify.ku.edu/static/depository/export_feed/kui-dwca.zip',
    'https://ichthyology.specify.ku.edu/static/depository/export_feed/kuit-dwca.zip'
    ]

    DS_GUIDS_W_SPECIFY_ACCESS_RECS = [
        # Fish Tissue
        '56caf05f-1364-4f24-85f6-0c82520c2792',
        # Fish
        '8f79c802-a58c-447f-99aa-1d6a0790825a']
    GUIDS_W_SPECIFY_ACCESS = [
        '2facc7a2-dd88-44af-b95a-733cc27527d4',
        '98fb49e0-591b-469e-99af-117b0bfdd7ee',
        '2c1becd5-e641-4e83-b3f5-76a55206539a', 
        'a413b456-0bff-47da-ab26-f074d9be5219',
        'dc92869c-1ed3-11e3-bfac-90b11c41863e',
        '21ac6644-5c55-44fd-b258-67eb66ea231d']
    GUIDS_WO_SPECIFY_ACCESS = [
        'ed8cfa5a-7b47-11e4-8ef3-782bcb9cd5b5',
        'f5725a56-7b47-11e4-8ef3-782bcb9cd5b5',
        'f69696a8-7b47-11e4-8ef3-782bcb9cd5b5',
        '5e7ec91c-4d20-42c4-ad98-8854800e82f7']
    DS_GUIDS_WO_SPECIFY_ACCESS_RECS = ['e635240a-3cb1-4d26-ab87-57d8c7afdfdb']
    BAD_GUIDS = [
        'KU :KUIZ:2200', 'KU :KUIZ:1663', 'KU :KUIZ:1569', 'KU :KUIZ:2462', 
        'KU :KUIZ:1743', 'KU :KUIZ:3019', 'KU :KUIZ:1816', 'KU :KUIZ:2542', 
        'KU :KUIZ:2396']
    NAMES = [
        'Eucosma raracana',
        'Plagioecia patina',
        'Plagiloecia patina Lamarck, 1816',
        'Plagioecia patina (Lamarck, 1816)',
        'Plagiloecia patana Lemarck',
        'Phlox longifolia Nutt.',
        'Tulipa sylvestris L.',
        'Medinilla speciosa Blume',
        'Acer caesium Wall. ex Brandis', 
        'Acer heldreichii Orph. ex Boiss.', 
        'Acer pseudoplatanus L.', 
        'Acer velutinum Boiss.', 
        'Acer hyrcanum Fisch. & Meyer', 
        'Acer monspessulanum L.', 
        'Acer obtusifolium Sibthorp & Smith', 
        'Acer opalus Miller', 
        'Acer sempervirens L.', 
        'Acer floridanum (Chapm.) Pax', 
        'Acer grandidentatum Torr. & Gray', 
        'Acer leucoderme Small', 
        'Acer nigrum Michx.f.', 
        'Acer skutchii Rehder', 
        'Acer saccharum Marshall']
    ITIS_TSNS = [526853, 183671, 182662, 566578]

# .............................................................................
class APIService:
    Root = {'endpoint': S2nEndpoint.Root, 'params': None,
        S2nKey.RECORD_FORMAT: None}
    # Direct access to syftorium upload
    Address = {'endpoint': S2nEndpoint.Address, 'params': None,
        S2nKey.RECORD_FORMAT: 'url string'}
    # Icons for service providers
    Badge = {
        'endpoint': S2nEndpoint.Badge, 
        'params': ['provider', 'icon_status'],
        S2nKey.RECORD_FORMAT: 'image/png'}
    # Health for service providers
    Heartbeat = {'endpoint': S2nEndpoint.Heartbeat, 'params': None,
        S2nKey.RECORD_FORMAT: ''}
    # Metadata for map services
    Map = {
        'endpoint': S2nEndpoint.Map, 
        'params': ['provider', 'namestr', 'gbif_parse', 'is_accepted', 'scenariocode', 'color'],
        S2nKey.RECORD_FORMAT: ''}
    # Taxonomic Resolution
    Name = {
        'endpoint': S2nEndpoint.Name, 
        'params': ['provider', 'namestr', 'is_accepted', 'gbif_parse', 'gbif_count', 'kingdom'],
        S2nKey.RECORD_FORMAT: ''}
    # Specimen occurrence records
    Occurrence = {'endpoint': S2nEndpoint.Occurrence, 'params': ['provider', 'occid', 'gbif_dataset_key', 'count_only'],
        S2nKey.RECORD_FORMAT: ''}
    # Specify guid resolver
    Resolve = {
        'endpoint': 'resolve', 
        'params': ['occid'],
        S2nKey.RECORD_FORMAT: ''}
    # TODO: Consider an Extension service for Digital Object Architecture
    SpecimenExtension = {'endpoint': S2nEndpoint.SpecimenExtension, 'params': None,
        S2nKey.RECORD_FORMAT: ''}
    Frontend = {
        'endpoint': S2nEndpoint.Frontend,
        'params': ['occid', 'namestr'],
        S2nKey.RECORD_FORMAT: ''}
    Stats = {
        'endpoint': S2nEndpoint.Stats,
        'params': [],
        S2nKey.RECORD_FORMAT: ''}


# .............................................................................
class ServiceProvider:
    Broker = {
        S2nKey.NAME: 'Specify Network', 
        S2nKey.PARAM: 'specifynetwork', 
        S2nKey.SERVICES: [S2nEndpoint.Badge],
        # 'icon': {'active': '{}/SpNetwork_active.png',
        #          'inactive': '{}/SpNetwork_inactive.png',
        #          'hover': '{}/SpNetwork_hover.png'}
        }
    GBIF = {
        S2nKey.NAME: 'GBIF', 
        S2nKey.PARAM: 'gbif', 
        S2nKey.SERVICES: [S2nEndpoint.Occurrence, S2nEndpoint.Name, S2nEndpoint.Badge],
        'icon': {'active': 'gbif_active-01.png',
                 'inactive': 'gbif_inactive-01.png',
                 'hover': 'gbif_hover-01-01.png'}
        }
    iDigBio = {
        S2nKey.NAME: 'iDigBio', 
        S2nKey.PARAM: 'idb', 
        S2nKey.SERVICES: [S2nEndpoint.Occurrence, S2nEndpoint.Badge],
        'icon': {'active': 'idigbio_colors_active-01.png',
                 'inactive': 'idigbio_colors_inactive-01.png',
                 'hover': 'idigbio_colors_hover-01.png'}
        }
    IPNI = {
        S2nKey.NAME: 'IPNI', 
        S2nKey.PARAM: 'ipni', 
        S2nKey.SERVICES: []
        }
    ITISSolr = {
        S2nKey.NAME: 'ITIS', 
        S2nKey.PARAM: 'itis', 
        S2nKey.SERVICES: [S2nEndpoint.Badge, S2nEndpoint.Name],
        'icon': {'active': 'itis_active.png',
                 'inactive': 'itis_inactive.png',
                 'hover': 'itis_hover.png'}
        }
    Lifemapper = {
        S2nKey.NAME: 'Lifemapper', 
        S2nKey.PARAM: 'lm', 
        S2nKey.SERVICES: [S2nEndpoint.Map, S2nEndpoint.Badge],
        'icon': {'active': 'lm_active.png',
                 'inactive': 'lm_inactive-01.png',
                 'hover': 'lm_hover-01.png'}
        }
    MorphoSource = {
        S2nKey.NAME: 'MorphoSource', 
        S2nKey.PARAM: 'mopho', 
        S2nKey.SERVICES: [
            S2nEndpoint.Badge, S2nEndpoint.Occurrence, S2nEndpoint.SpecimenExtension],
        'icon': {'active': 'morpho_active-01.png',
                 'inactive': 'morpho_inactive-01.png',
                 'hover': 'morpho_hover-01.png'}
        }
    Specify = {
        S2nKey.NAME: 'Specify', 
        S2nKey.PARAM: 'specify', 
        S2nKey.SERVICES: [
            S2nEndpoint.Badge, S2nEndpoint.Occurrence, S2nEndpoint.Resolve],
        'icon': {'active': 'specify_network_active.png',}}
    # TODO: need an WoRMS badge
    WoRMS = {
        S2nKey.NAME: 'WoRMS',
        S2nKey.PARAM: 'worms', 
        S2nKey.SERVICES: [S2nEndpoint.Badge, S2nEndpoint.Name],
        'icon': {
            'active': 'worms_active.png',
        }
    }
    
# ....................
    @classmethod
    def get_values(cls, param_or_name):
        if param_or_name in (
            ServiceProvider.GBIF[S2nKey.NAME], ServiceProvider.GBIF[S2nKey.PARAM]):
            return ServiceProvider.GBIF
        elif param_or_name in (
            ServiceProvider.iDigBio[S2nKey.NAME], ServiceProvider.iDigBio[S2nKey.PARAM]):
            return ServiceProvider.iDigBio
        elif param_or_name in (
            ServiceProvider.IPNI[S2nKey.NAME], ServiceProvider.IPNI[S2nKey.PARAM]):
            return ServiceProvider.IPNI
        elif param_or_name in (
            ServiceProvider.ITISSolr[S2nKey.NAME], ServiceProvider.ITISSolr[S2nKey.PARAM]):
            return ServiceProvider.ITISSolr
        elif param_or_name in (
            ServiceProvider.Lifemapper[S2nKey.NAME], ServiceProvider.Lifemapper[S2nKey.PARAM]):
            return ServiceProvider.Lifemapper
        elif param_or_name in (
            ServiceProvider.MorphoSource[S2nKey.NAME], ServiceProvider.MorphoSource[S2nKey.PARAM]):
            return ServiceProvider.MorphoSource
        elif param_or_name in (
            ServiceProvider.Specify[S2nKey.NAME], ServiceProvider.Specify[S2nKey.PARAM]):
            return ServiceProvider.Specify
        elif param_or_name in (
            ServiceProvider.WoRMS[S2nKey.NAME], ServiceProvider.WoRMS[S2nKey.PARAM]):
            return ServiceProvider.WoRMS
        elif param_or_name in (
            ServiceProvider.Broker[S2nKey.NAME], ServiceProvider.Broker[S2nKey.PARAM]):
            return ServiceProvider.Broker
        else:
            return None
# ....................
    @classmethod
    def is_valid_param(cls, param):
        params = [svc[S2nKey.PARAM] for svc in cls.all()]
        if param in params:
            return True
        return False
# ....................
    @classmethod
    def is_valid_service(cls, param, svc):
        if param is not None:
            val_dict = ServiceProvider.get_values(param)
            if svc in (val_dict['services']):
                return True
        return False

# ....................
    @classmethod
    def get_name_from_param(cls, param):
        name = None
        if param is not None:
            val_dict = ServiceProvider.get_values(param)
            name = val_dict[S2nKey.NAME]
        return name

# ....................
    @classmethod
    def all(cls):
        return [
            ServiceProvider.GBIF, ServiceProvider.iDigBio, ServiceProvider.IPNI, 
            ServiceProvider.ITISSolr, ServiceProvider.Lifemapper, ServiceProvider.MorphoSource, 
            ServiceProvider.Specify, ServiceProvider.WoRMS, ServiceProvider.Broker]

 # .............................................................................


# .............................................................................

URL_ESCAPES = [[" ", "\%20"], [",", "\%2C"]]
ENCODING = 'utf-8'


"""  
http://preview.specifycloud.org/static/depository/export_feed/kui-dwca.zip
http://preview.specifycloud.org/static/depository/export_feed/kuit-dwca.zip

curl '{}{}'.format(http://preview.specifycloud.org/export/record/
  | python -m json.tool

"""

# .............................................................................
# These fields must match the Solr core fields in spcoco/conf/schema.xml
SPCOCO_FIELDS = [
    # GUID and solr uniqueKey
    'id',
    # pull dataset/alternateIdentfier from DWCA eml.xml
    'dataset_guid',
    # ARK metadata
    # similar to DC Creator, Contributor, Publisher
    'who',
    # similar to DC Title
    'what',
    # similar to DC Date
    'when',
    # similar to DC Identifier, optional as this is the ARK
    'where',
    # Supplemental ARK metadata
    # redirection URL to specify7-server
    'url']

# ......................................................
class Lifemapper:
    URL = 'https://data.lifemapper.org/api/v2'
    OCC_RESOURCE = 'occurrence'
    PROJ_RESOURCE = 'sdmproject'
    MAP_RESOURCE = 'ogc' 
    OBSERVED_SCENARIO_CODE = 'worldclim-curr'
    PAST_SCENARIO_CODES = ['CMIP5-CCSM4-lgm-10min', 'CMIP5-CCSM4-mid-10min']
    FUTURE_SCENARIO_CODES = [
        'AR5-CCSM4-RCP8.5-2050-10min', 'AR5-CCSM4-RCP4.5-2050-10min', 
        'AR5-CCSM4-RCP4.5-2070-10min', 'AR5-CCSM4-RCP8.5-2070-10min'] 
    OTHER_RESOURCES = ['taxonomy', 'scenario', 'envlayer']
    NAME_KEY = 'displayname'
    ATOM_KEY = 'atom'
    MIN_STAT_KEY ='after_status'
    MAX_STAT_KEY = 'before_status'
    COMPLETE_STAT_VAL = 300
    SCENARIO_KEY = 'projectionscenariocode'
    PROJECTION_METADATA_KEYS = [
        'modelScenario', 'projectionScenario', 'algorithm', 'spatialRaster']
    COMMANDS = ['count']
    # VALID broker parameter options must be list
    VALID_MAPLAYER_TYPES = ['occ', 'prj', 'bmng']
    VALID_MAP_FORMAT = ['image/png', 'image/gif', 'image/jpeg', 'image/tiff', 'image/x-aaigrid']
    VALID_SRS = ['epsg:4326', 'epsg:3857', 'AUTO:42003']
    VALID_PALETTES = [
        'red', 'gray', 'green', 'blue', 'safe', 'pretty', 'yellow', 
        'fuschia', 'aqua', 'bluered', 'bluegreen', 'greenred']
    # TODO: replace with a schema definition
    RECORD_FORMAT_MAP = 'lifemapper_layer schema TBD'
    RECORD_FORMAT_OCC = 'lifemapper_occ schema TBD'
    
    @staticmethod
    def valid_scenario_codes():
        valid_scenario_codes = [Lifemapper.OBSERVED_SCENARIO_CODE]
        valid_scenario_codes.extend(Lifemapper.PAST_SCENARIO_CODES)
        valid_scenario_codes.extend(Lifemapper.FUTURE_SCENARIO_CODES)
        return valid_scenario_codes

BrokerParameters = {
    'provider': {
        'type': '', 'default': None, 'options': [
            ServiceProvider.GBIF[S2nKey.PARAM], ServiceProvider.iDigBio[S2nKey.PARAM],
            ServiceProvider.ITISSolr[S2nKey.PARAM], ServiceProvider.Lifemapper[S2nKey.PARAM], 
            ServiceProvider.MorphoSource[S2nKey.PARAM], ServiceProvider.Specify[S2nKey.PARAM]]
        },
    'namestr': {'type': '', 'default': None},
    'is_accepted': {'type': False, 'default': False},
    'gbif_parse': {'type': False, 'default': False},
    'gbif_count': {'type': False, 'default': False},
    'itis_match': {'type': False, 'default': False},
    'kingdom': {'type': '', 'default': None},
    'occid': {'type': '', 'default': None},
    'gbif_dataset_key': {'type': '', 'default': None},
    'count_only': {'type': False, 'default': False},
    'url': {'type': '', 'default': None},
    'scenariocode': {
        'type': '', 
        'options': Lifemapper.valid_scenario_codes(), 
        'default': None},
    'url': {'type': '', 'default': None},
    'bbox': {'type': '', 'default': '-180,-90,180,90'},
    'color': {
        'type': '', 
        'options': Lifemapper.VALID_PALETTES, 
        'default': Lifemapper.VALID_PALETTES[0]},
    'exceptions': {'type': '', 'default': None},
    'height': {'type': 300, 'default': 300},
    'width': {'type': 600, 'default': 600},
    'layers': {
        'type': '', 
        'options': Lifemapper.VALID_MAPLAYER_TYPES, 
        'default': Lifemapper.VALID_MAPLAYER_TYPES[0]},
    'request': {
        'type': '', 
        'options': VALID_MAP_REQUESTS, 
        'default': VALID_MAP_REQUESTS[0]},
    'format': {
        'type': '', 
        'options': Lifemapper.VALID_MAP_FORMAT,
        'default': Lifemapper.VALID_MAP_FORMAT[0]},
    'srs': {
        'type': '', 
        'options': Lifemapper.VALID_SRS,
        'default': Lifemapper.VALID_SRS[0]},
    'transparent': {'type': True, 'default': True},
    'icon_status': {
        'type': '', 
        'options': VALID_ICON_OPTIONS, 
        'default': None}
    }

# ......................................................
class MorphoSource:
    REST_URL = 'https://ms1.morphosource.org/api/v1'
    VIEW_URL = 'https://www.morphosource.org/concern/biological_specimens'
    NEW_VIEW_URL = 'https://www.morphosource.org/catalog/objects'
    NEW_API_URL = 'https://www.morphosource.org/catalog/objects.json'
    # FROZEN_URL = 'https://ea-boyerlab-morphosource-01.oit.duke.edu/api/v1'
    DWC_ID_FIELD = 'specimen.occurrence_id'
    LOCAL_ID_FIELD = 'specimen.specimen_id'
    OCC_RESOURCE = 'specimens'
    MEDIA_RESOURCE = 'media'
    OTHER_RESOURCES = ['taxonomy', 'projects', 'facilities']
    COMMAND = 'find'
    OCCURRENCEID_KEY = 'occurrence_id'
    TOTAL_KEY = 'totalResults'
    RECORDS_KEY = 'results'
    LIMIT = 1000
    RECORD_FORMAT = 'https://www.morphosource.org/About/API'

    @classmethod
    def get_occurrence_view(cls, local_id):
        """
        Example:
            https://www.morphosource.org/concern/biological_specimens/000S27385
        """
        url = None
        if local_id:
            idtail = 'S{}'.format(local_id)
            leading_zero_count = (9 - len(idtail))
            prefix = '0' * leading_zero_count
            url ='{}/{}{}'.format(MorphoSource.VIEW_URL, prefix, idtail)
        return url

    @classmethod
    def get_occurrence_data(cls, occurrence_id):
        url = None
        if occurrence_id:
            url = '{}/find/specimens?start=0&limit=1000&q=occurrence_id%3A{}'.format(
                MorphoSource.REST_URL, occurrence_id)
        return url
    
# ......................................................
class SPECIFY:
    """Specify constants enumeration
    """
    DATA_DUMP_DELIMITER = '\t'
    RECORD_FORMAT = 'http://rs.tdwg.org/dwc.json'
    RESOLVER_COLLECTION = 'spcoco'
    RESOLVER_LOCATION = SYFT_BASE
    
# ......................................................
class SYFTER:
    REST_URL = '{}/api/v1'.format(SYFT_BASE)
    RESOLVE_RESOURCE = 'resolve'
    
    
# ......................................................
class GBIF:
    """GBIF constants enumeration"""
    DATA_DUMP_DELIMITER = '\t'
    TAXON_KEY = 'specieskey'
    TAXON_NAME = 'sciname'
    PROVIDER = 'puborgkey'
    OCC_ID_FIELD = 'gbifID'
    SPECIES_ID_FIELD = 'usageKey'
    WAIT_TIME = 180
    LIMIT = 300
    VIEW_URL = 'https://www.gbif.org'
    REST_URL = 'https://api.gbif.org/v1'
    QUALIFIER = 'gbif:'

    SPECIES_SERVICE = 'species'
    PARSER_SERVICE = 'parser/name'
    OCCURRENCE_SERVICE = 'occurrence'
    DATASET_SERVICE = 'dataset'
    ORGANIZATION_SERVICE = 'organization'
    
    COUNT_KEY = 'count'
    RECORDS_KEY = 'results'
    RECORD_FORMAT_NAME = 'https://www.gbif.org/developer/species'
    RECORD_FORMAT_OCCURRENCE = 'https://www.gbif.org/developer/occurrence'

    TAXONKEY_FIELD = 'specieskey'
    TAXONNAME_FIELD = 'sciname'
    PROVIDER_FIELD = 'puborgkey'
    ID_FIELD = 'gbifid'

    ACCEPTED_NAME_KEY = 'accepted_name'
    SEARCH_NAME_KEY = 'search_name'
    SPECIES_KEY_KEY = 'speciesKey'
    SPECIES_NAME_KEY = 'species'
    TAXON_ID_KEY = 'taxon_id'

    REQUEST_SIMPLE_QUERY_KEY = 'q'
    REQUEST_NAME_QUERY_KEY = 'name'
    REQUEST_TAXON_KEY = 'TAXON_KEY'
    REQUEST_RANK_KEY = 'rank'
    REQUEST_DATASET_KEY = 'dataset_key'

    DATASET_BACKBONE_VALUE = 'GBIF Backbone Taxonomy'
    DATASET_BACKBONE_KEY = 'd7dddbf4-2cf0-4f39-9b2a-bb099caae36c'

    SEARCH_COMMAND = 'search'
    COUNT_COMMAND = 'count'
    MATCH_COMMAND = 'match'
    DOWNLOAD_COMMAND = 'download'
    DOWNLOAD_REQUEST_COMMAND = 'request'
    RESPONSE_NOMATCH_VALUE = 'NONE'
    
    NameMatchFieldnames = [
        'scientificName', 'kingdom', 'phylum', 'class', 'order', 'family',
        'genus', 'species', 'rank', 'genusKey', 'speciesKey', 'usageKey',
        'canonicalName', 'confidence']

    # For writing files from GBIF DarwinCore download,
    # DWC translations in lmCompute/code/sdm/gbif/constants
    # We are adding the 2 fields: LM_WKT_FIELD and LINK_FIELD
    LINK_FIELD = 'gbifurl'
    # Ends in / to allow appending unique id
    
    @classmethod
    def species_url(cls):
        return '{}/{}'.format(GBIF.VIEW_URL, GBIF.SPECIES_SERVICE)
    
    @classmethod
    def get_occurrence_view(cls, key):
        url = None
        if key:
            url = '{}/{}/{}'.format(GBIF.VIEW_URL, GBIF.OCCURRENCE_SERVICE, key)
        return url

    @classmethod
    def get_occurrence_data(cls, key):
        url = None
        if key:
            url = '{}/{}/{}'.format(GBIF.REST_URL, GBIF.OCCURRENCE_SERVICE, key)
        return url

    @classmethod
    def get_species_view(cls, key):
        url = None
        if key:
            url = '{}/{}/{}'.format(GBIF.VIEW_URL, GBIF.SPECIES_SERVICE, key)
        return url

    @classmethod
    def get_species_data(cls, key):
        url = None
        if key:
            url = '{}/{}/{}'.format(GBIF.REST_URL, GBIF.SPECIES_SERVICE, key)
        return url



# .............................................................................
class WORMS:
    """World Register of Marine Species, WoRMS, constants enumeration
    http://www.marinespecies.org/rest/AphiaRecordsByMatchNames?scientificnames[]=Plagioecia%20patina&marine_only=false
    """
    REST_URL = 'http://www.marinespecies.org/rest'
    NAME_MATCH_SERVICE = 'AphiaRecordsByMatchNames'
    NAME_SERVICE = 'AphiaNameByAphiaID'
    MATCH_PARAM = 'scientificnames[]='
    ID_FLDNAME = 'valid_AphiaID'
    
    @classmethod
    def get_species_data(cls, key):
        url = None
        if key:
            url = '{}/{}/{}'.format(WORMS.REST_URL, WORMS.NAME_SERVICE, key)
        return url

class IPNI:
    base_url = 'http://beta.ipni.org/api/1'
    
# .............................................................................
class ITIS:
    """ITIS constants enumeration
    http://www.itis.gov/ITISWebService/services/ITISService/getAcceptedNamesFromTSN?tsn=183671
    @todo: for JSON output use jsonservice instead of ITISService
    """
    DATA_NAMESPACE = '{http://data.itis_service.itis.usgs.gov/xsd}'
    NAMESPACE = '{http://itis_service.itis.usgs.gov}'
    VIEW_URL = 'https://www.itis.gov/servlet/SingleRpt/SingleRpt'
    # ...........
    # Solr Services
    SOLR_URL = 'https://services.itis.gov'
    TAXONOMY_HIERARCHY_QUERY = 'getFullHierarchyFromTSN'
    VERNACULAR_QUERY = 'getCommonNamesFromTSN'    
    NAMES_FROM_TSN_QUERY = 'getAcceptedNamesFromTSN'
    RECORD_FORMAT = 'https://www.itis.gov/solr_documentation.html'
    COUNT_KEY = 'numFound'
    RECORDS_KEY = 'docs'
    # ...........
    # Web Services
    WEBSVC_URL = 'http://www.itis.gov/ITISWebService/services/ITISService'
    JSONSVC_URL = 'https://www.itis.gov/ITISWebService/jsonservice'
    # wildcard matching
    ITISTERMS_FROM_SCINAME_QUERY = 'getITISTermsFromScientificName'
    SEARCH_KEY = 'srchKey'
    # JSON return tags
    TSN_KEY = 'tsn'
    NAME_KEY = 'nameWInd'
    HIERARCHY_KEY = 'hierarchySoFarWRanks'
    HIERARCHY_TAG = 'hierarchyList'
    RANK_TAG = 'rankName'
    TAXON_TAG = 'taxonName'
    KINGDOM_KEY = 'Kingdom'
    PHYLUM_DIVISION_KEY = 'Division'
    CLASS_KEY = 'Class'
    ORDER_KEY = 'Order'
    FAMILY_KEY = 'Family'
    GENUS_KEY = 'Genus'
    SPECIES_KEY = 'Species'
    URL_ESCAPES = [ [" ", "\%20"] ]
    
    @classmethod
    def get_taxon_view(cls, tsn):
        return '{}?search_topic=TSN&search_value={}'.format(ITIS.VIEW_URL, tsn)

    @classmethod
    def get_taxon_data(cls, tsn):
        return '{}?q=tsn:{}'.format(ITIS.SOLR_URL, tsn)

# .............................................................................
# .                           iDigBio constants                               .
# .............................................................................
class Idigbio:
    """iDigBio constants enumeration"""
    NAMESPACE_URL = ''
    NAMESPACE_ABBR = 'gbif'
    VIEW_URL = 'https://www.idigbio.org/portal/records'
    REST_URL = 'https://search.idigbio.org/v2/view/records'
    # LINK_PREFIX = 'https://www.idigbio.org/portal/records/'
    SEARCH_PREFIX = 'https://search.idigbio.org/v2'
    SEARCH_POSTFIX = 'search'
    COUNT_POSTFIX = 'summary/count'
    OCCURRENCE_POSTFIX = 'records'
    PUBLISHERS_POSTFIX = 'publishers'
    RECORDSETS_POSTFIX = 'recordsets'
    SEARCH_LIMIT = 5000
    ID_FIELD = 'uuid'
    OCCURRENCEID_FIELD = 'occurrenceid'
    LINK_FIELD = 'idigbiourl'
    GBIFID_FIELD = 'taxonid'
    BINOMIAL_REGEX = "(^[^ ]*) ([^ ]*)$"
    RECORD_CONTENT_KEY = 'data'
    RECORD_INDEX_KEY = 'indexTerms'
    QUALIFIER = 'idigbio:'
    QKEY = 'rq'
    QFILTERS = {'basisofrecord': 'preservedspecimen'}
    FILTERS = {'limit': 5000,
               'offset': 0,
               'no_attribution': False}
    COUNT_KEY = 'itemCount'
    RECORDS_KEY = 'items'
    RECORD_FORMAT = 'https://github.com/idigbio/idigbio-search-api/wiki'
    
    @classmethod
    def get_occurrence_view(cls, uuid):
        url = None
        if uuid:
            url = '{}/{}'.format(Idigbio.VIEW_URL, uuid)
        return url

    @classmethod
    def get_occurrence_data(cls, uuid):
        url = None
        if uuid:
            url = '{}/{}'.format(Idigbio.REST_URL, uuid)
        return url
    

