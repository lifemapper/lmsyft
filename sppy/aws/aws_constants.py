"""Constants for Specnet AWS Resources."""
import copy
import os.path
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
DATESTR_TOKEN = "YYYY_MM_DD"

DATASET_GBIF_KEY = "datasetkey"


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
    SPECIES_DATASET_MATRIX = "species_dataset_matrix"
    SPECIES_DATASET_SUMMARY = "species_dataset_summary"
    DATASET_SPECIES_SUMMARY = "dataset_species_summary"


# .............................................................................
class Summaries:
    """Constant metadata about aggregate tables.

    Note: All filenames follow the pattern
        <datacontents>_<datatype>_<YYYY_MM_DD><_optional parquet extension>
    Note: Table code is the same as <datacontents>_<datatype>
    Note: "datasetkey" is the original GBIF field
    """
    TABLES = {
            SUMMARY_TABLE_TYPES.DATASET_COUNTS: {
                "code": SUMMARY_TABLE_TYPES.DATASET_COUNTS,
                "fname": f"dataset_counts_{DATESTR_TOKEN}_000",
                "table_format": "Parquet",
                "fields": [DATASET_GBIF_KEY, "occ_count", "species_count"],
                "key_fld": DATASET_GBIF_KEY
            },
            SUMMARY_TABLE_TYPES.DATASET_SPECIES_LISTS: {
                "code": SUMMARY_TABLE_TYPES.DATASET_SPECIES_LISTS,
                "fname": f"dataset_lists_{DATESTR_TOKEN}_000",
                "table_format": "Parquet",
                "fields": [DATASET_GBIF_KEY, "taxonkey", "species", "occ_count"],
                "key_fld": DATASET_GBIF_KEY,
                "species_fld": "species",
                "combine_fields": {"taxonkey_species": ("taxonkey", "species")},
                "value_fld": "occ_count",
            },
            SUMMARY_TABLE_TYPES.DATASET_META: {
                "code": SUMMARY_TABLE_TYPES.DATASET_META,
                "fname": f"dataset_meta_{DATESTR_TOKEN}",
                "table_format": "Parquet",
                "fields": [
                    "dataset_key", "publishing_organization_key", "title"],
                "key_fld": "dataset_key"
            },
            SUMMARY_TABLE_TYPES.SPECIES_DATASET_MATRIX: {
                "code": SUMMARY_TABLE_TYPES.SPECIES_DATASET_MATRIX,
                "fname": f"speciesxdataset_matrix_{DATESTR_TOKEN}",
                "table_format": "Zip",
                # Axis 0
                "row": "taxonkey_species",
                # Axis 1
                "column": DATASET_GBIF_KEY,
                # Matrix values
                "value": "occ_count",
            },
            SUMMARY_TABLE_TYPES.SPECIES_DATASET_SUMMARY: {
                "code": SUMMARY_TABLE_TYPES.SPECIES_DATASET_SUMMARY,
                "fname": f"speciesxdataset_summary{DATESTR_TOKEN}",
                "table_format": "Zip",
                # Axis 0
                "row": "taxonkey_species",
                # Axis 1
                "column": "count_type",
                # Matrix values
                "value": "count",
            },
            SUMMARY_TABLE_TYPES.DATASET_SPECIES_SUMMARY: {
                "code": SUMMARY_TABLE_TYPES.DATASET_SPECIES_SUMMARY,
                "fname": f"datasetxspecies_summary_{DATESTR_TOKEN}",
                "table_format": "Zip",
                # Axis 0
                "row": "count_type",
                # Axis 1
                "column": DATASET_GBIF_KEY,
                # Matrix values
                "value": "count",
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
            meta_cpy["fname"] = fname_tmpl.replace(DATESTR_TOKEN, datestr)
            tables[key] = meta_cpy
        return tables

    # ...............................................
    @classmethod
    def get_table(cls, table_type, datestr=None):
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
        if datestr is not None:
            fname_tmpl = cpy_table["fname"]
            cpy_table["fname"] = fname_tmpl.replace(DATESTR_TOKEN, datestr)
        return cpy_table

    # ...............................................
    @classmethod
    def get_tabletype_from_filename_prefix(cls, datacontents, datatype):
        """Get the table type from the file prefixes.

        Args:
            datacontents (str): first part of filename indicating data in table.
            datatype (str): second part of filename indicating form of data in table
                (records, lists, matrix, etc).

        Returns:
            table_type (SUMMARY_TABLE_TYPES type): type of table.

        Raises:
            Exception: on invalid file prefix.
        """
        table_type = None
        for key, meta in cls.TABLES.items():
            fname = meta["fname"]
            contents, dtp, _, _ = cls._parse_filename(fname)
            if datacontents == contents and datatype == dtp:
                table_type = key
        if table_type is None:
            raise Exception(
                f"Table with filename prefix {datacontents}_{datatype} does not exist")
        return table_type

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
        fname = fname_tmpl.replace(DATESTR_TOKEN, datestr)
        return fname

    # ...............................................
    @classmethod
    def _parse_filename(cls, filename):
        # <datacontents>_<datatype>_<YYYY_MM_DD><_optional parquet extension>
        fname = os.path.basename(filename)
        fname_noext, _ext = os.path.splitext(fname)
        fn_parts = fname_noext.split("_")
        if len(fn_parts) >= 5:
            datacontents = fn_parts[0]
            datatype = fn_parts[1]
            yr = fn_parts[2]
            mo = fn_parts[3]
            day = fn_parts[4]
            rest = fn_parts[5:]
            if len(yr) == 4 and len(mo) == 2 and len(day) == 2:
                data_datestr = f"{yr}_{mo}_{day}"
            else:
                raise Exception(
                    f"Length of elements year, month, day ({yr}, {mo}. {day}) should "
                    "be 4, 2, and 2")
        else:
            raise Exception(f"{fname_noext} does not follow the expected pattern")
        return datacontents, datatype, data_datestr, rest

    # ...............................................
    @classmethod
    def get_tabletype_datestring_from_filename(cls, filename):
        """Get the table type from the filename.

        Args:
            filename: relative or absolute filename of a SUMMARY data file.

        Returns:
            table_type (SUMMARY_TABLE_TYPES type): type of table.
            data_datestr (str): date of data in "YYYY_MM_DD" format.

        Raises:
            Exception: on failure to get tabletype and datestring from this filename.
        """
        try:
            datacontents, datatype, data_datestr, _rest = cls._parse_filename(filename)
            table_type = cls.get_tabletype_from_filename_prefix(datacontents, datatype)
        except Exception:
            raise
        return table_type, data_datestr


# .............................................................................
class SNKeys(Enum):
    """Dictionary keys to use for describing RowColumnComparisons of SUMMARY data.

    Note: All keys refer to the relationship between rows, columns and values.  Missing
        values in a dataset dictionary indicate that the measure is not meaningful.
    """
    # ----------------------------------------------------------------------
    # Column: type of aggregation
    (COL_TYPE,) = range(0, 1)
    # Column: One x
    (COL_IDX, COL_LABEL, COL_COUNT, COL_TOTAL,
     COL_MIN_COUNT, COL_MIN_LABELS, COL_MIN_INDEXES,
     COL_MAX_COUNT, COL_MAX_LABELS, COL_MAX_INDEXES
     ) = range(100, 110)
    # Column: All x
    (COLS_TOTAL, COLS_MIN, COLS_MAX, COLS_MEAN, COLS_MEDIAN,
     COLS_COUNT, COLS_COUNT_MIN, COLS_COUNT_MAX, COLS_COUNT_MEAN, COLS_COUNT_MEDIAN
     ) = range(200, 210)
    # Row: aggregation of what type of data
    (ROW_TYPE,) = range(1000, 1001)
    # Row: One y
    (ROW_IDX, ROW_LABEL, ROW_COUNT, ROW_TOTAL,
     ROW_MIN_COUNT, ROW_MIN_LABELS, ROW_MIN_INDEXES,
     ROW_MAX_COUNT, ROW_MAX_LABELS, ROW_MAX_INDEXES
     ) = range(1100, 1110)
    (ROWS_TOTAL, ROWS_MIN, ROWS_MAX, ROWS_MEAN, ROWS_MEDIAN,
     ROWS_COUNT, ROWS_COUNT_MIN, ROWS_COUNT_MAX, ROWS_COUNT_MEAN, ROWS_COUNT_MEDIAN
     ) = range(1200, 1210)

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
                # ----------------------------------------------------------------------
                # Column
                # -----------------------------
                cls.COL_TYPE: "dataset",
                # One dataset
                cls.COL_IDX: "dataset_index",
                cls.COL_LABEL: "dataset_label",
                # Count (non-zero elements in column)
                cls.COL_COUNT: "total_species_for_dataset",
                # Values (total of values in column)
                cls.COL_TOTAL: "total_occurrences_for_dataset",
                # Values: Minimum occurrence count for one dataset, species labels, indexes
                cls.COL_MIN_COUNT: "min_occurrence_count_for_dataset",
                cls.COL_MIN_LABELS: "species_with_min_occurrence_count_for_dataset",
                cls.COL_MIN_INDEXES: "species_indexes_with_min_occurrence_count_for_dataset",
                # Values: Maximum occurrence count for one dataset, species labels, indexes
                cls.COL_MAX_COUNT: "max_occurrence_count_for_dataset",
                cls.COL_MAX_LABELS: "species_with_max_occurrence_count_for_dataset",
                cls.COL_MAX_INDEXES: "species_indexes_with_max_occurrence_count_for_dataset",
                # -----------------------------
                # All datasets
                # ------------
                # COMPARES TO:  cls.COL_TOTAL: "total_occurrences_for_dataset",
                # Values: Total of all occurrences for all datasets - stats
                cls.COLS_TOTAL: "total_occurrences_for_all_datasets",
                cls.COLS_MIN: "min_occurrences_for_all_datasets",
                cls.COLS_MAX: "max_occurrences_for_all_datasets",
                cls.COLS_MEAN: "mean_occurrences_for_all_datasets",
                cls.COLS_MEDIAN: "median_occurrences_for_all_datasets",
                # ------------
                # COMPARES TO: cls.COL_COUNT: "total_species_for_dataset",
                # Counts: Count of all species (from all columns/datasets)
                cls.COLS_COUNT: "total_species_count",
                # Species counts for all datasets - stats
                cls.COLS_COUNT_MIN: "min_species_count_for_all_datasets",
                cls.COLS_COUNT_MAX: "max_species_count_for_all_datasets",
                cls.COLS_COUNT_MEAN: "mean_species_count_for_all_datasets",
                cls.COLS_COUNT_MEDIAN: "median_species_count_for_all_datasets",
                # ----------------------------------------------------------------------
                # Row
                # -----------------------------
                cls.ROW_TYPE: "species",
                # One species
                cls.ROW_IDX: "species_index",
                cls.ROW_LABEL: "species_label",
                # Count (non-zero elements in row)
                cls.ROW_COUNT: "total_datasets_for_species",
                # Values (total of values in row)
                cls.ROW_TOTAL: "total_occurrences_for_species",
                # Values: Minimum occurrence count for one species, dataset labels, indexes
                cls.ROW_MIN_COUNT: "min_occurrence_count_for_species",
                cls.ROW_MIN_LABELS: "datasets_with_min_count_for_species",
                cls.ROW_MIN_INDEXES: "dataset_indexes_with_min_count_for_species",
                # Values: Maximum occurrence count for one species, dataset labels, indexes
                cls.ROW_MAX_COUNT: "max_occurrence_count_for_species",
                cls.ROW_MAX_LABELS: "datasets_with_max_count_for_species",
                cls.ROW_MAX_INDEXES: "dataset_indexes_with_max_count_for_species",
                # -----------------------------
                # All species
                # ------------
                # COMPARES TO: cls.ROW_TOTAL: "total_occurrences_for_species",
                # Values: Total of all occurrences for all species - stats
                cls.ROWS_TOTAL: "total_occurrences_for_all_species",
                cls.ROWS_MIN: "min_occurrences_for_all_species",
                cls.ROWS_MAX: "max_occurrences_for_all_species",
                cls.ROWS_MEAN: "mean_occurrences_for_all_species",
                cls.ROWS_MEDIAN: "median_occurrences_for_all_species",
                # ------------
                # COMPARES TO: cls.ROW_COUNT: "total_datasets_for_species",
                # Counts: Count of all datasets (from all rows/species)
                cls.ROWS_COUNT: "total_dataset_count",
                # Dataset counts for all species - stats
                cls.ROWS_COUNT_MIN: "min_dataset_count_for_all_species",
                cls.ROWS_COUNT_MAX: "max_dataset_count_for_all_species",
                cls.ROWS_COUNT_MEAN: "mean_dataset_count_for_all_species",
                cls.ROWS_COUNT_MEDIAN: "median_dataset_count_for_all_species",
            }
        else:
            raise Exception(f"Keys not defined for table {table_type}")
        return keys
