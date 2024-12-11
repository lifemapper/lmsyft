"""Constants for spanalyst module for analyzing GBIF species occurrence data dimensions."""
# .............................................................................
# Data constants
# .............................................................................
COMPOUND_SPECIES_FLD = "taxonkey_species"
GBIF_DATASET_KEY_FLD = "dataset_key"

# .............................................................................
# Log processing progress
LOGINTERVAL = 1000000
LOG_FORMAT = " ".join(["%(asctime)s", "%(levelname)-8s", "%(message)s"])
LOG_DATE_FORMAT = "%d %b %Y %H:%M"
LOGFILE_MAX_BYTES = 52000000
LOGFILE_BACKUP_COUNT = 5

TMP_PATH = "/tmp"
ENCODING = "utf-8"
ERR_SEPARATOR = "------------"
USER_DATA_TOKEN = "###SCRIPT_GOES_HERE###"
CSV_DELIMITER = ","
ZIP_EXTENSION = ".zip"
JSON_EXTENSION = ".json"
CSV_EXTENSION = ".csv"

SHP_EXT = "shp"
SHP_EXTENSIONS = [
    ".shp", ".shx", ".dbf", ".prj", ".sbn", ".sbx", ".fbn", ".fbx", ".ain",
    ".aih", ".ixs", ".mxs", ".atx", ".shp.xml", ".cpg", ".qix"]


# .............................................................................
class SUMMARY_FIELDS:
    """Fields used to summarize aggregated data."""
    COUNT = "count"
    TOTAL = "total"
    OCCURRENCE_COUNT = "occ_count"
    SPECIES_COUNT = "species_count"


# .............................................................................
SPECIES_DIM = {
    "code": "species",
    "key_fld": COMPOUND_SPECIES_FLD
}


# .............................................................................
class STATISTICS_TYPE:
    """Biodiversity statistics for a Site by Species presence-absence matrix (PAM)."""
    SIGMA_SITE = "sigma-site"
    SIGMA_SPECIES = "sigma-species"
    DIVERSITY = "diversity"
    SITE = "site"
    SPECIES = "species"

# ...........................
    @classmethod
    def all(cls):
        """Get all aggregated data type codes.

        Returns:
            list of supported codes for datatypes.
        """
        return (cls.SIGMA_SITE, cls.SIGMA_SPECIES, cls.DIVERSITY, cls.SITE, cls.SPECIES)


# .............................................................................
class AGGREGATION_TYPE:
    """Types of tables created for aggregate species data analyses."""
    # TODO: decide whether to keep PAM
    LIST = "list"
    COUNT = "counts"
    MATRIX = "matrix"
    PAM = "pam"
    STATISTICS = "stats"
    SUMMARY = "summary"

    # ...........................
    @classmethod
    def all(cls):
        """Get all aggregated data type codes.

        Returns:
            list of supported codes for datatypes.
        """
        return (cls.LIST, cls.COUNT, cls.MATRIX, cls.PAM, cls.STATISTICS, cls.SUMMARY)
