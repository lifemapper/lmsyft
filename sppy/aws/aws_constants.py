"""Constants for Specnet AWS Resources."""
import copy
from enum import Enum

PROJ_NAME = "specnet"
REGION = "us-east-1"
PROJ_BUCKET = f"{PROJ_NAME}-{REGION}"
SUMMARY_FOLDER = "summary"
ENCODING = "utf-8"
LOCAL_OUTDIR = "/tmp"

INPUT_PATH = "summary"
LOG_PATH = "log"

GBIF_BUCKET = "gbif-open-data-us-east-1/occurrence"
GBIF_ARN = "arn:aws:s3:::gbif-open-data-us-east-1"
GBIF_ODR_FNAME = "occurrence.parquet"

SPOT_TEMPLATE_BASENAME = "launch_template"

KEY_NAME = "aimee-aws-key"
# Allows KU Dyche hall
SECURITY_GROUP_ID = ""
SECRET_NAME = ""

# S3
TRIGGER_PATH = "trigger"
TRIGGER_FILENAME = "go.txt"

# EC2 Spot Instance
# List of instance types at https://aws.amazon.com/ec2/spot/pricing/
INSTANCE_TYPE = "t2.micro"
# INSTANCE_TYPE = "a1.large"

USER_DATA_TOKEN = "###SCRIPT_GOES_HERE###"


# .............................................................................
class LOG:
    """Standard values for logging processing progress."""
    INTERVAL = 1000000
    FORMAT = " ".join(["%(asctime)s", "%(levelname)-8s", "%(message)s"])
    DATE_FORMAT = "%d %b %Y %H:%M"
    FILE_MAX_BYTES = 52000000
    FILE_BACKUP_COUNT = 5


# .............................................................................
class SUMMARY_TABLE_TYPES:
    """Types of tables stored in S3 for aggregate species data analyses."""
    DATASET_COUNTS = "dataset_counts"
    DATASET_SPECIES_LISTS = "dataset_species_lists"
    DATASET_META = "dataset_meta"
    SPECIES_DATASET_MATRIX = "species_dataset"


# .............................................................................
class Summaries:
    """Constant metadata about aggregate tables."""
    TABLES = {
            SUMMARY_TABLE_TYPES.DATASET_COUNTS: {
                "fname": "dataset_counts_XXXX_XX_XX_000.parquet",
                "table_format": "Parquet",
                "fields": ["datasetkey", "occ_count", "species_count"],
                "key_fld": "datasetkey"
            },
            SUMMARY_TABLE_TYPES.DATASET_SPECIES_LISTS: {
                "fname": "dataset_lists_XXXX_XX_XX_000.parquet",
                "table_format": "Parquet",
                "fields": ["datasetkey", "taxonkey", "species", "occ_count"],
                "key_fld": "datasetkey",
                "species_fld": "species",
                "combine_fields": {"taxonkey_species": ("taxonkey", "species")},
                "value_fld": "occ_count",
            },
            SUMMARY_TABLE_TYPES.DATASET_META: {
                "fname": "dataset_meta_XXXX_XX_XX.parquet",
                "table_format": "Parquet",
                "fields": [
                    "dataset_key", "publishing_organization_key", "title"],
                "key_fld": "dataset_key"
            },
            SUMMARY_TABLE_TYPES.SPECIES_DATASET_MATRIX: {
                "fname": "species_dataset_matrix_XXXX_XX_XX.npz",
                "table_format": "NPZ",
                "row": "taxonkey_species",
                "column": "datasetkey",
                "value": "occ_count",
            }
        }

    # ...............................................
    @classmethod
    def update_summary_tables(cls, datestr):
        """Update filenames in the metadata dictionary and return.

        Args:
            datestr: Datestring contained in the filename indicating the current version
                of the data.

        Returns:
            tables: dictionary of summary table metadata.
        """
        tables = {}
        # Update filename in summary tables
        for key, meta in cls.TABLES.items():
            meta_cpy = copy.deepcopy(meta)
            fname_tmpl = meta["fname"]
            meta_cpy["fname"] = fname_tmpl.replace("XXXX_XX_XX", datestr)
            tables[key] = meta_cpy
        return tables

    # ...............................................
    @classmethod
    def get_table(cls, table_type, datestr):
        """Update the filename in a metadata dictionary for one table, and return.

        Args:
            table_type: type of summary table to return.
            datestr: Datestring contained in the filename indicating the current version
                of the data.

        Returns:
            tables: dictionary of summary table metadata.
        """
        try:
            cpy_table = copy.deepcopy(cls.TABLES[table_type])
        except KeyError:
            return None
        fname_tmpl = cpy_table["fname"]
        cpy_table["fname"] = fname_tmpl.replace("XXXX_XX_XX", datestr)
        return cpy_table

    # ...............................................
    @classmethod
    def get_filename(cls, table_type, datestr):
        """Update the filename in a metadata dictionary for one table, and return.

        Args:
            table_type: type of summary table to return.
            datestr: Datestring contained in the filename indicating the current version
                of the data.

        Returns:
            tables: dictionary of summary table metadata.
        """
        fname_tmpl = cls.TABLES[table_type]["fname"]
        fname = fname_tmpl.replace("XXXX_XX_XX", datestr)
        return fname


# .............................................................................
class SNKeys(Enum):
    """Dictionary keys to use for describing RowColumnComparisons of SUMMARY data.

    Note: All keys refer to the relationship between rows, columns and values.  Missing
        values in a dataset dictionary indicate that the measure is not meaningful.
    """
    (COL_IDX, COL_LABEL, COL_TOTAL, COL_TOTAL_MIN, COL_TOTAL_MAX, COL_TOTAL_MEAN,
     COL_COUNT, COL_COUNT_MIN, COL_COUNT_MAX, COL_COUNT_MEAN, COL_MAX_COUNT,
     COL_MAX_LABELS, COL_MAX_INDEXES) = range(1, 14)
    (ROW_IDX, ROW_LABEL, ROW_TOTAL, ROW_TOTAL_MIN, ROW_TOTAL_MAX, ROW_TOTAL_MEAN,
     ROW_COUNT, ROW_COUNT_MIN, ROW_COUNT_MAX, ROW_COUNT_MEAN, ROW_MAX_COUNT,
     ROW_MAX_LABELS, ROW_MAX_INDEXES) = range(101, 114)

    @classmethod
    def get_keys_for_table(cls, table_type):
        """Return keystrings for statistics dictionary for specific aggregation tables.

        Args:
            table_type (aws_constants.SUMMARY_TABLE_TYPES): type of aggregated data

        Returns:
            keys (dict): Dictionary of strings to be used as keys for each type of
                value in a dictionary of statistics.

        Raises:
            Exception: on un-implemented table type.
        """
        if table_type == SUMMARY_TABLE_TYPES.SPECIES_DATASET_MATRIX:
            keys = {
                # Column
                cls.COL_IDX: "dataset_index",
                cls.COL_LABEL: "dataset_label",
                cls.COL_TOTAL: "total_occurrences_for_dataset",
                cls.COL_TOTAL_MIN: "min_total_occurrences_for_all_datasets",
                cls.COL_TOTAL_MAX: "max_total_occurrences_for_all_datasets",
                cls.COL_TOTAL_MEAN: "mean_total_occurrences_for_all_datasets",
                cls.COL_COUNT: "occurrence_count_for_dataset",
                cls.COL_COUNT_MIN: "min_occurrence_count_for_all_datasets",
                cls.COL_COUNT_MAX: "max_occurrence_count_for_all_datasets",
                cls.COL_COUNT_MEAN: "mean_occurrence_count_for_all_datasets",
                cls.COL_MAX_COUNT: "max_occurrence_count_for_dataset",
                cls.COL_MAX_LABELS: "species_with_max_count_for_dataset",
                cls.COL_MAX_INDEXES: "species_indexes_with_max_count_for_dataset",
                # Row
                cls.ROW_IDX: "species_index",
                cls.ROW_LABEL: "species_label",
                cls.ROW_TOTAL: "total_occurrences_for_species",
                cls.ROW_TOTAL_MIN: "min_total_occurrences_for_all_species",
                cls.ROW_TOTAL_MAX: "max_total_occurrences_for_all_species",
                cls.ROW_TOTAL_MEAN: "mean_total_occurrences_for_all_species",
                cls.ROW_COUNT: "dataset_count",
                cls.ROW_COUNT_MIN: "min_occurrence_count_for_all_species",
                cls.ROW_COUNT_MAX: "max_occurrence_count_for_all_species",
                cls.ROW_COUNT_MEAN: "mean_occurrence_count_for_all_species",
                cls.ROW_MAX_COUNT: "max_occurrence_count_for_species",
                cls.ROW_MAX_LABELS: "datasets_with_max_count_for_species",
                cls.ROW_MAX_INDEXES: "dataset_indexes_with_max_count_for_species"
            }
        else:
            raise Exception(f"Keys not defined for table {table_type}")
        return keys
