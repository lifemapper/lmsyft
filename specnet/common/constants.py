"""Constants for data-specific Specify Network Analyst use of spanalyst modules."""
from copy import deepcopy
from enum import Enum
import os

from spanalyst.aws.constants import S3_RS_TABLE_SUFFIX
from spanalyst.common.constants import (
    AGGREGATION_TYPE, COMPOUND_SPECIES_FLD, SUMMARY_FIELDS, ZIP_EXTENSION
)


# .............................................................................
# Specify Network Workflow tasks: scripts, docker compose files, EC2 launch template versions,
#   Cloudwatch streams
class TASK:
    """Workflow tasks to be executed on EC2 instances."""
    TEST = "test_task"
    CALC_STATS = "calc_stats"
    userdata_extension = ".userdata.sh"

    # ...........................
    @classmethod
    def tasks(cls):
        """Get all valid tasks.

        Returns:
            (list of str): all valid tasks
        """
        return [cls.TEST, cls.CALC_STATS]

    # ...........................
    @classmethod
    def get_userdata_filename(cls, task, pth=None):
        """Get the filename containing userdata to execute this task.

        Args:
            task (str): task
            pth (str): local path for file.

        Returns:
            fname (str): filename for EC2 userdata to execute task.

        Raises:
            Exception: on unsupported task string.
        """
        if task not in cls.tasks():
            raise Exception(f"Unknown task {task}")
        fname = f"{task}{cls.userdata_extension}"
        if pth is not None:
            fname = os.path.join(pth, fname)
        return fname

    # ...........................
    @classmethod
    def get_task_from_userdata_filename(cls, fname):
        """Get the task for the userdata file.

        Args:
            fname (str): filename for EC2 userdata to execute task.

        Returns:
            task (str): task

        Raises:
            Exception: on no task for this filename.
        """
        task = fname.rstrip(cls.userdata_extension)
        if task not in cls.tasks():
            raise Exception(f"Unknown task {task} from userdata file {fname}")
        return task


# .............................................................................
class ANALYSIS_DIM:
    """All dimensions with columns used for data analyses."""
    DATASET = {
        "code": "dataset",
        "key_fld": "dataset_key",
        # In summary records
        "fields": [
            "dataset_key", COMPOUND_SPECIES_FLD, SUMMARY_FIELDS.OCCURRENCE_COUNT
        ]
    }
    SPECIES = {
        "code": "species",
        "key_fld": COMPOUND_SPECIES_FLD,
    }

    # ...........................
    @classmethod
    def species(cls):
        """Get the data species analyses dimension.

        Returns:
            Data dimension relating to species.
        """
        return ANALYSIS_DIM.SPECIES

    # ...........................
    @classmethod
    def species_code(cls):
        """Get the code for the data species analyses dimension.

        Returns:
            Code for the data dimension relating to species.
        """
        return ANALYSIS_DIM.SPECIES["code"]

    # ...........................
    @classmethod
    def analysis_dimensions(cls):
        """Get one or all data analyses dimensions to be analyzed for species.

        Returns:
            dim_lst (list): List of data dimension(s) to be analyzed for species.
        """
        dim_lst = [cls.DATASET]
        return dim_lst

    # ...........................
    @classmethod
    def analysis_codes(cls):
        """Get one or all codes for data analyses dimensions to be analyzed for species.

        Returns:
            code_lst (list): Codes of data dimension(s) to be analyzed for species.
        """
        code_lst = [cls.DATASET["code"]]
        return code_lst

    # ...........................
    @classmethod
    def get(cls, code):
        """Get the data analyses dimension for the code.

        Args:
            code (str): Code for the analysis dimension to be returned.

        Returns:
            Data dimension.

        Raises:
            Exception: on unknown code.
        """
        for dim in [cls.DATASET]:
            if code == dim["code"]:
                return dim
        raise Exception(f"No dimension `{code}` in ANALYSIS_DIM")

    # ...........................
    @classmethod
    def get_from_key_fld(cls, key_fld):
        """Get the data analyses dimension for the key_fld.

        Args:
            key_fld (str): Field name for the analysis dimension to be returned.

        Returns:
            Data dimension.

        Raises:
            Exception: on unknown code.
        """
        for dim in [cls.DATASET]:
            if key_fld == dim["key_fld"]:
                return dim
        raise Exception(f"No dimension for field `{key_fld}` in ANALYSIS_DIM")


# .............................................................................
class SUMMARY:
    """Types of tables stored in S3 for aggregate species data analyses."""
    dt_token = "YYYY_MM_DD"
    sep = "_"
    dim_sep = f"{sep}x{sep}"
    DATATYPES = AGGREGATION_TYPE.all()
    SPECIES_DIMENSION = ANALYSIS_DIM.species_code()
    SPECIES_FIELD = ANALYSIS_DIM.SPECIES["key_fld"]
    ANALYSIS_DIMENSIONS = ANALYSIS_DIM.analysis_codes()

    # ...........................
    @classmethod
    def get_table_type(cls, datatype, dim0, dim1):
        """Get the table_type string for the analysis dimension and datatype.

        Args:
            datatype (SUMMARY.DATAYPES): type of aggregated data.
            dim0 (str): code for primary dimension (bison.common.constants.ANALYSIS_DIM)
                of analysis
            dim1 (str): code for secondary dimension of analysis

        Note:
            BISON Table types include:
                list: region_x_species_list
                counts: region_counts
                summary: region_x_species_summary
                         species_x_region_summary
                matrix:  species_x_region_matrix

        Note: for matrix, dimension1 corresponds to Axis 0 (rows) and dimension2
            corresponds to Axis 1 (columns).

        Returns:
            table_type (str): code for data type and contents

        Raises:
            Exception: on datatype not one of: "counts", "list", "summary", "matrix"
            Exception: on datatype "counts", dim0 not in ANALYSIS_DIMENSIONS
            Exception: on datatype "counts", dim1 not None
            Exception: on datatype "matrix", dim0 not in ANALYSIS_DIMENSIONS
            Exception: on datatype "matrix", dim1 != ANALYSIS_DIMENSIONS.SPECIES
            Exception: on dim0 == SPECIES_DIMENSION and dim1 not in ANALYSIS_DIMENSIONS
            Exception: on dim0 in ANALYSIS_DIMENSIONS and dim0 != SPECIES_DIMENSION
        """
        if datatype not in cls.DATATYPES:
            raise Exception(f"Datatype {datatype} is not in {cls.DATATYPES}.")

        if datatype == AGGREGATION_TYPE.COUNT:
            if dim0 in cls.ANALYSIS_DIMENSIONS:
                if dim1 is None:
                    # ex: state_counts
                    table_type = f"{dim0}{cls.sep}{datatype}"
                else:
                    raise Exception("Second dimension must be None")
            else:
                raise Exception(
                    f"First dimension for counts must be in {cls.ANALYSIS_DIMENSIONS}.")
        elif datatype == AGGREGATION_TYPE.MATRIX:
            if dim0 not in cls.ANALYSIS_DIMENSIONS:
                raise Exception(
                    f"First dimension (rows) must be in {cls.ANALYSIS_DIMENSIONS}"
                )
            if dim1 != cls.SPECIES_DIMENSION:
                raise Exception(
                    f"Second dimension (columns) must be {cls.SPECIES_DIMENSION}"
                )
            table_type = f"{dim0}{cls.dim_sep}{dim1}{cls.sep}{datatype}"
        else:
            if dim0 == cls.SPECIES_DIMENSION and dim1 not in cls.ANALYSIS_DIMENSIONS:
                raise Exception(
                    f"Second dimension must be in {cls.ANALYSIS_DIMENSIONS}"
                )
            elif dim0 in cls.ANALYSIS_DIMENSIONS and dim1 != cls.SPECIES_DIMENSION:
                raise Exception(
                    f"First dimension must be {cls.SPECIES_DIMENSION} or "
                    f"in {cls.ANALYSIS_DIMENSIONS}."
                )

            table_type = f"{dim0}{cls.dim_sep}{dim1}{cls.sep}{datatype}"
        return table_type

    # ...........................
    @classmethod
    def list(cls):
        """Records of dimension, species, occ count for each dimension in project.

        Returns:
            list (dict): dict of dictionaries for each list table defined by the
                project.

        Note:
            The keys for the dictionary (and code in the metadata values) are table_type
        """
        list = {}
        for analysis_code in cls.ANALYSIS_DIMENSIONS:
            table_type = cls.get_table_type(
                AGGREGATION_TYPE.LIST, analysis_code, cls.SPECIES_DIMENSION)
            dim = ANALYSIS_DIM.get(analysis_code)
            # name == table_type, ex: county_x_species_list
            meta = {
                "code": table_type,
                "fname": f"{table_type}{cls.sep}{cls.dt_token}",
                # "table_format": "Parquet",
                "file_extension": f"{cls.sep}000.parquet",

                "fields": dim["fields"],
                "key_fld": dim["key_fld"],
                "species_fld": COMPOUND_SPECIES_FLD,
                "value_fld": SUMMARY_FIELDS.OCCURRENCE_COUNT
            }
            list[table_type] = meta
        return list

    # ...........................
    @classmethod
    def counts(cls):
        """Records of dimension, species count, occ count for each dimension in project.

        Returns:
            list (dict): dict of dictionaries for each list table defined by the
                project.

        Note:
            This table type refers to a table assembled from original data records
            and species and occurrence counts for a dimension.  The dimension can be
            any unique set of attributes, such as county + riis_status.  For simplicity,
            define each unique set of attributes as a single field/value

        Note:
            The keys for the dictionary (and code in the metadata values) are table_type

        TODO: Remove this from creation?  Can create from sparse matrix.
        """
        counts = {}
        for analysis_code in cls.ANALYSIS_DIMENSIONS:
            dim0 = ANALYSIS_DIM.get(analysis_code)
            table_type = cls.get_table_type(
                AGGREGATION_TYPE.COUNT, analysis_code, None)

            meta = {
                "code": table_type,
                "fname": f"{table_type}{cls.sep}{cls.dt_token}",
                # "table_format": "Parquet",
                "file_extension": f"{cls.sep}000.parquet",
                "data_type": "counts",

                # Dimensions: 0 is row (aka Axis 0) list of records with counts
                #   1 is column (aka Axis 1), count and total of dim for each row
                "dim_0_code": analysis_code,
                "dim_1_code": None,

                "key_fld": dim0["key_fld"],
                "occurrence_count_fld": SUMMARY_FIELDS.OCCURRENCE_COUNT,
                "species_count_fld": COMPOUND_SPECIES_FLD,
                "fields": [
                    dim0["key_fld"],
                    SUMMARY_FIELDS.OCCURRENCE_COUNT,
                    COMPOUND_SPECIES_FLD
                ]
            }
            counts[table_type] = meta
        return counts

    # ...........................
    @classmethod
    def summary(cls):
        """Summary of dimension1 count and occurrence count for each dimension0 value.

        Returns:
            sums (dict): dict of dictionaries for each summary table defined by the
                project.

        Note:
            table contains stacked records summarizing original data:
                dim0, dim1, rec count of dim1 in dim0
                ex: county, species, occ_count
        """
        sums = {}
        species_code = cls.SPECIES_DIMENSION
        for analysis_code in cls.ANALYSIS_DIMENSIONS:
            for dim0, dim1 in (
                    (analysis_code, species_code), (species_code, analysis_code)
            ):
                table_type = cls.get_table_type(
                    AGGREGATION_TYPE.SUMMARY, dim0, dim1)
                meta = {
                    "code": table_type,
                    "fname": f"{table_type}{cls.sep}{cls.dt_token}",
                    "file_extension": ".csv",
                    "data_type": "summary",

                    # Dimensions: 0 is row (aka Axis 0) list of values to summarize,
                    #   1 is column (aka Axis 1), count and total of dim for each row
                    "dim_0_code": dim0,
                    "dim_1_code": dim1,

                    # Axis 1
                    "column": "measurement_type",
                    "fields": [SUMMARY_FIELDS.COUNT, SUMMARY_FIELDS.TOTAL],
                    # Matrix values
                    "value": "measure"}
                sums[table_type] = meta
        return sums

    # ...........................
    @classmethod
    def matrix(cls):
        """Species by <dimension> matrix defined for this project.

        Returns:
            mtxs (dict): dict of dictionaries for each matrix/table defined for this
                project.

        Note:
            Similar to a Presence/Absence Matrix (PAM),
                Rows will always have analysis dimension (i.e. region or other category)
                Columns will have species
        """
        mtxs = {}
        dim1 = cls.SPECIES_DIMENSION
        for analysis_code in cls.ANALYSIS_DIMENSIONS:
            dim0 = analysis_code
            table_type = cls.get_table_type(AGGREGATION_TYPE.MATRIX, dim0, dim1)

            # Dimension/Axis 0/row is always region or other analysis dimension
            meta = {
                "code": table_type,
                "fname": f"{table_type}{cls.sep}{cls.dt_token}",
                "file_extension": ".npz",
                "data_type": "matrix",

                # Dimensions: 0 is row (aka Axis 0), 1 is column (aka Axis 1)
                "dim_0_code": dim0,
                "dim_1_code": dim1,

                # These are all filled in for compressing data, reading data
                "row_categories": [],
                "column_categories": [],
                "value_fld": "",
                "datestr": cls.dt_token,

                # Matrix values
                "value": SUMMARY_FIELDS.OCCURRENCE_COUNT,
            }
            mtxs[table_type] = meta
        return mtxs

    # ...........................
    @classmethod
    def statistics(cls):
        """Species by <dimension> statistics matrix/table defined for this project.

        Returns:
            stats (dict): dict of dictionaries for each matrix/table defined for this
                project.

        Note:
            Rows will always have analysis dimension (i.e. region or other category)
            Columns will have species
        """
        stats = {}
        # Axis 1 of PAM is always species
        dim1 = cls.SPECIES_DIMENSION
        for analysis_code in cls.ANALYSIS_DIMENSIONS:
            # Axis 0 of PAM is always 'site'
            dim0 = analysis_code
            table_type = cls.get_table_type(AGGREGATION_TYPE.STATISTICS, dim0, dim1)
            meta = {
                "code": table_type,
                "fname": f"{table_type}{cls.sep}{cls.dt_token}",
                "file_extension": ".csv",
                "data_type": "stats",
                "datestr": cls.dt_token,

                # Dimensions refer to the PAM matrix, site x species, from which the
                # stats are computed.
                "dim_0_code": dim0,
                "dim_1_code": dim1,

                # Minimum count defining 'presence' in the PAM
                "min_presence_count": 1,

                # TODO: Remove.  pandas.DataFrame contains row and column headers
                # # Categories refer to the statistics matrix headers
                # "row_categories": [],
                # "column_categories": []
            }
            stats[table_type] = meta
        return stats

    # ...........................
    @classmethod
    def pam(cls):
        """Species by <dimension> matrix defined for this project.

        Returns:
            pams (dict): dict of dictionaries for each matrix/table defined for this
                project.

        Note:
            Rows will always have analysis dimension (i.e. region or other category)
            Columns will have species
        """
        # TODO: Is this an ephemeral data structure used only for computing stats?
        #       If we want to save it, we must add compress_to_file,
        #       uncompress_zipped_data, read_data.
        #       If we only save computations, must save input HeatmapMatrix metadata
        #       and min_presence_count
        #       Note bison.spanalyst.pam_matrix.PAM
        pams = {}
        for analysis_code in cls.ANALYSIS_DIMENSIONS:
            dim0 = analysis_code
            dim1 = cls.SPECIES_DIMENSION
            table_type = cls.get_table_type(AGGREGATION_TYPE.PAM, dim0, dim1)

            # Dimension/Axis 0/row is always region or other analysis dimension
            meta = {
                "code": table_type,
                "fname": f"{table_type}{cls.sep}{cls.dt_token}",
                "file_extension": ".npz",
                "data_type": "matrix",

                # Dimensions: 0 is row (aka Axis 0), 1 is column (aka Axis 1)
                "dim_0_code": dim0,
                "dim_1_code": dim1,

                # These are all filled in for compressing data, reading data
                "row_categories": [],
                "column_categories": [],
                "value_fld": "",
                "datestr": cls.dt_token,

                # Matrix values
                "value": "presence",
                "min_presence_count": 1,
            }
            pams[table_type] = meta
        return pams

    # ...............................................
    @classmethod
    def tables(cls, datestr=None):
        """All tables of species count and occurrence count, summary, and matrix.

        Args:
            datestr (str): String in the format YYYY_MM_DD.

        Returns:
            sums (dict): dict of dictionaries for each table defined by the project.
                If datestr is provided, the token in the filename is replaced with that.

        Note:
            The keys for the dictionary (and code in the metadata values) are table_type
        """
        tables = cls.list()
        tables.update(cls.counts())
        tables.update(cls.summary())
        tables.update(cls.matrix())
        tables.update(cls.pam())
        tables.update(cls.statistics())
        if datestr is not None:
            # Update filename in summary tables
            for key, meta in tables.items():
                meta_cpy = deepcopy(meta)
                fname_tmpl = meta["fname"]
                meta_cpy["fname"] = fname_tmpl.replace(cls.dt_token, datestr)
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

        Raises:
            Exception: on invalid table_type
        """
        tables = cls.tables()
        try:
            table = tables[table_type]
        except KeyError:
            raise Exception(f"Invalid table_type {table_type}")
        cpy_table = deepcopy(table)
        if datestr is not None:
            cpy_table["datestr"] = datestr
            fname_tmpl = cpy_table["fname"]
            cpy_table["fname"] = fname_tmpl.replace(cls.dt_token, datestr)
        return cpy_table

    # ...............................................
    @classmethod
    def get_tabletype_from_filename_prefix(cls, datacontents, datatype):
        """Get the table type from the file prefixes.

        Args:
            datacontents (str): first part of filename indicating data in table.
            datatype (str): second part of filename indicating form of data in table
                (records, list, matrix, etc).

        Returns:
            table_type (SUMMARY_TABLE_TYPES type): type of table.

        Raises:
            Exception: on invalid file prefix.
        """
        tables = cls.tables()
        table_type = None
        for key, meta in tables.items():
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
    def get_filename(cls, table_type, datestr, is_compressed=False):
        """Update the filename in a metadata dictionary for one table, and return.

        Args:
            table_type (str): predefined type of data indicating type and contents.
            datestr (str): Datestring contained in the filename indicating the current version
                of the data.
            is_compressed (bool): flag indicating to return a filename for a compressed
                file.

        Returns:
            tables: dictionary of summary table metadata.
        """
        tables = cls.tables()
        table = tables[table_type]
        ext = table["file_extension"]
        if is_compressed is True:
            ext = ZIP_EXTENSION
        fname_tmpl = f"{table['fname']}{ext}"
        fname = fname_tmpl.replace(cls.dt_token, datestr)
        return fname

    # ...............................................
    @classmethod
    def parse_table_type(cls, table_type):
        """Parse the table_type into datacontents (dim0, dim1) and datatype.

        Args:
            table_type: String identifying the type of data and dimensions.

        Returns:
            datacontents (str): type of data contents
            dim0 (str): first dimension (rows/axis 0) of data in the table
            dim1 (str): second dimension (columns/axis 1) of data in the table
            datatype (str): type of data structure: summary table, stacked records
                (list or count), or matrix.

        Raises:
            Exception: on failure to parse table_type into 2 strings.
        """
        dim0 = dim1 = None
        fn_parts = table_type.split(cls.sep)
        if len(fn_parts) >= 2:
            datatype = fn_parts.pop()
            idx = len(datatype) + 1
            datacontents = table_type[:-idx]
        else:
            raise Exception(f"Failed to parse {table_type}.")
        # Some data has 2 dimensions
        dim_parts = datacontents.split(cls.dim_sep)
        dim0 = dim_parts[0]
        try:
            dim1 = dim_parts[1]
        except IndexError:
            pass
        return datacontents, dim0, dim1, datatype

    # ...............................................
    @classmethod
    def _parse_filename(cls, filename):
        # This will parse a filename for the compressed file of statistics, but
        #             not individual matrix and metadata files for each stat.
        # <datacontents>_<datatype>_<YYYY_MM_DD><_optional parquet extension>
        fname = os.path.basename(filename)
        if fname.endswith(S3_RS_TABLE_SUFFIX):
            stripped_fn = fname[:-len(S3_RS_TABLE_SUFFIX)]
            rest = S3_RS_TABLE_SUFFIX
        else:
            stripped_fn, ext = os.path.splitext(fname)
            rest = ext
        idx = len(stripped_fn) - len(cls.dt_token)
        datestr = stripped_fn[idx:]
        table_type = stripped_fn[:idx-1]
        datacontents, dim0, dim1, datatype = cls.parse_table_type(table_type)

        return datacontents, dim0, dim1, datatype, datestr, rest

    # ...............................................
    @classmethod
    def get_tabletype_datestring_from_filename(cls, filename):
        """Get the table type from the filename.

        Args:
            filename: relative or absolute filename of a SUMMARY data file.

        Returns:
            table_type (SUMMARY_TABLE_TYPES type): type of table.
            datestr (str): date of data in "YYYY_MM_DD" format.

        Raises:
            Exception: on failure to get tabletype and datestring from this filename.

        Note:
            This will parse a filename for the compressed file of statistics, but
            not individual matrix and metadata files for each stat.
        """
        try:
            datacontents, dim0, dim1, datatype, datestr, _rest = \
                cls._parse_filename(filename)
            table_type = f"{datacontents}{cls.sep}{datatype}"
        except Exception:
            raise
        return table_type, datestr


# .............................................................................
class SNKeys(Enum):
    """Dictionary keys to use for describing RowColumnComparisons of SUMMARY data.

    Note: All keys refer to the relationship between rows, columns and values.  Missing
        values in a dataset dictionary indicate that the measure is not meaningful.
    """
    # ----------------------------------------------------------------------
    # Column: type of aggregation
    (COL_TYPE,) = range(5000, 5001)
    # Column: One x
    (COL_LABEL, COL_COUNT, COL_TOTAL,
     COL_MIN_TOTAL, COL_MIN_TOTAL_NUMBER, COL_MAX_TOTAL, COL_MAX_TOTAL_LABELS,
     ) = range(5100, 5107)
    # Column: All x
    (COLS_TOTAL,
     COLS_MIN_TOTAL, COLS_MIN_TOTAL_NUMBER, COLS_MEAN_TOTAL, COLS_MEDIAN_TOTAL,
     COLS_MAX_TOTAL, COLS_MAX_TOTAL_LABELS,
     COLS_COUNT,
     COLS_MIN_COUNT, COLS_MIN_COUNT_NUMBER, COLS_MEAN_COUNT, COLS_MEDIAN_COUNT,
     COLS_MAX_COUNT, COLS_MAX_COUNT_LABELS
     ) = range(5200, 5214)
    # Row: aggregation of what type of data
    (ROW_TYPE,) = range(6000, 6001)
    # Row: One y
    (ROW_LABEL, ROW_COUNT, ROW_TOTAL,
     ROW_MIN_TOTAL, ROW_MIN_TOTAL_NUMBER, ROW_MAX_TOTAL, ROW_MAX_TOTAL_LABELS,
     ) = range(6100, 6107)
    # Rows: All y
    (ROWS_TOTAL,
     ROWS_MIN_TOTAL, ROWS_MIN_TOTAL_NUMBER, ROWS_MEAN_TOTAL, ROWS_MEDIAN_TOTAL,
     ROWS_MAX_TOTAL, ROWS_MAX_TOTAL_LABELS,
     ROWS_COUNT,
     ROWS_MIN_COUNT, ROWS_MIN_COUNT_NUMBER, ROWS_MEAN_COUNT, ROWS_MEDIAN_COUNT,
     ROWS_MAX_COUNT, ROWS_MAX_COUNT_LABELS
     ) = range(6200, 6214)
    # Type of aggregation
    (TYPE,) = range(0, 1)
    # One field of row/column header
    (ONE_LABEL, ONE_COUNT, ONE_TOTAL,
     ONE_MIN_COUNT, ONE_MIN_COUNT_NUMBER,
     ONE_MAX_COUNT, ONE_MAX_COUNT_LABELS
     ) = range(100, 107)
    # Column: All row/column headers
    (ALL_TOTAL,
     ALL_MIN_TOTAL, ALL_MIN_TOTAL_NUMBER, ALL_MEAN_TOTAL, ALL_MEDIAN_TOTAL,
     ALL_MAX_TOTAL, ALL_MAX_TOTAL_LABELS,
     ALL_COUNT,
     ALL_MIN_COUNT, ALL_MIN_COUNT_NUMBER, ALL_MEAN_COUNT, ALL_MEDIAN_COUNT,
     ALL_MAX_COUNT, ALL_MAX_COUNT_LABELS,
     ) = range(200, 214)

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
        dim_dataset = ANALYSIS_DIM.DATASET["code"]
        dim_species = ANALYSIS_DIM.SPECIES["code"]
        dataset_species_matrix_type = SUMMARY.get_table_type(
            "matrix", dim_dataset, dim_species)
        dataset_species_summary_type = SUMMARY.get_table_type(
            "summary", dim_dataset, dim_species)
        species_dataset_summary_type = SUMMARY.get_table_type(
            "summary", dim_dataset, dim_species)
        if table_type == dataset_species_matrix_type:
            keys = {
                # ----------------------------------------------------------------------
                # Column
                # -----------------------------
                cls.COL_TYPE: "dataset",
                # One dataset
                cls.COL_LABEL: "dataset_label",
                # Count (non-zero elements in column)
                cls.COL_COUNT: "total_species_for_dataset",
                # Values (total of values in column)
                cls.COL_TOTAL: "total_occurrences_for_dataset",
                # Values: Minimum occurrences for one dataset, species labels
                cls.COL_MIN_TOTAL: "min_occurrences_for_dataset",
                cls.COL_MIN_TOTAL_NUMBER: "number_of_species_with_min_occurrences_for_dataset",
                # Values: Maximum occurrence count for one dataset, species labels
                cls.COL_MAX_TOTAL: "max_occurrences_for_dataset",
                cls.COL_MAX_TOTAL_LABELS: "species_with_max_occurrences_for_dataset",
                # -----------------------------
                # All datasets
                # ------------
                # Values: Total of all occurrences for all datasets - stats
                cls.COLS_TOTAL: "total_occurrences_of_all_datasets",
                cls.COLS_MIN_TOTAL: "min_occurrences_of_all_datasets",
                cls.COLS_MIN_TOTAL_NUMBER: "number_of_datasets_with_min_occurrences_of_all",
                cls.COLS_MEAN_TOTAL: "mean_occurrences_of_all_datasets",
                cls.COLS_MEDIAN_TOTAL: "median_occurrences_of_all_datasets",
                cls.COLS_MAX_TOTAL: "max_occurrences_of_all_datasets",
                cls.COLS_MAX_TOTAL_LABELS: "datasets_with_max_occurrences_of_all",
                # ------------
                # Counts: Count of all species (from all columns/datasets)
                cls.COLS_COUNT: "total_dataset_count",
                # Species counts for all datasets - stats
                cls.COLS_MIN_COUNT: "min_species_count_of_all_datasets",
                cls.COLS_MIN_COUNT_NUMBER: "number_of_datasets_with_min_species_count_of_all",
                cls.COLS_MEAN_COUNT: "mean_species_count_of_all_datasets",
                cls.COLS_MEDIAN_COUNT: "median_species_count_of_all_datasets",
                cls.COLS_MAX_COUNT: "max_species_count_of_all_datasets",
                cls.COLS_MAX_COUNT_LABELS: "datasets_with_max_species_count_of_all",
                # ----------------------------------------------------------------------
                # Row
                # -----------------------------
                cls.ROW_TYPE: "species",
                # One species
                cls.ROW_LABEL: "species_label",
                # Count (non-zero elements in row)
                cls.ROW_COUNT: "total_datasets_for_species",
                # Values (total of values in row)
                cls.ROW_TOTAL: "total_occurrences_for_species",
                # Values: Minimum occurrence count for one species, dataset labels, indexes
                cls.ROW_MIN_TOTAL: "min_occurrences_for_species",
                # Values: Maximum occurrence count for one species, dataset labels, indexes
                cls.ROW_MAX_TOTAL: "max_occurrences_for_species",
                cls.ROW_MAX_TOTAL_LABELS: "datasets_with_max_occurrences_for_species",
                # -----------------------------
                # All species
                # ------------
                # COMPARES TO: cls.ROW_TOTAL: "total_occurrences_for_species",
                # Values: Total of all occurrences for all species - stats
                cls.ROWS_TOTAL: "total_occurrences_of_all_species",
                cls.ROWS_MIN_TOTAL: "min_occurrences_of_all_species",
                cls.ROWS_MIN_TOTAL_NUMBER: "number_of_species_with_max_occurrences_of_all",
                cls.ROWS_MEAN_TOTAL: "mean_occurrences_of_all_species",
                cls.ROWS_MEDIAN_TOTAL: "median_occurrences_of_all_species",
                cls.ROWS_MAX_TOTAL: "max_occurrences_of_all_species",
                cls.ROWS_MAX_TOTAL_LABELS: "species_with_max_occurrences_of_all",
                # ------------
                # COMPARES TO: cls.ROW_COUNT: "total_datasets_for_species",
                # Counts: Count of all datasets (from all rows/species)
                cls.ROWS_COUNT: "total_species_count",
                # Dataset counts for all species - stats
                cls.ROWS_MIN_COUNT: "min_dataset_count_of_all_species",
                cls.ROWS_MIN_COUNT_NUMBER: "species_with_min_dataset_count_of_all",
                cls.ROWS_MEAN_COUNT: "mean_dataset_count_of_all_species",
                cls.ROWS_MEDIAN_COUNT: "median_dataset_count_of_all_species",
                cls.ROWS_MAX_COUNT: "max_dataset_count_of_all_species",
                cls.ROWS_MAX_COUNT_LABELS: "species_with_max_dataset_count_of_all",
            }
        elif table_type == dataset_species_summary_type:
            keys = {
                # ----------------------------------------------------------------------
                # Column
                # -----------------------------
                cls.TYPE: "dataset",
                # One dataset
                cls.ONE_LABEL: "dataset_label",
                # Count (non-zero elements in column)
                cls.ONE_COUNT: "total_species_for_dataset",
                # Values (total of values in column)
                cls.ONE_TOTAL: "total_occurrences_for_dataset",
                # Values: Minimum occurrence count for one dataset
                cls.ONE_MIN_COUNT: "min_occurrences_for_dataset",
                cls.ONE_MIN_COUNT_NUMBER: "number_of_datasets_with_min_occurrences",
                # Values: Maximum occurrence count for one dataset, species labels, indexes
                cls.ONE_MAX_COUNT: "max_occurrences_for_dataset",
                cls.ONE_MAX_COUNT_LABELS: "datasets_with_max_occurrences",
                # -----------------------------
                # All datasets
                # ------------
                # COMPARES TO:  cls.ONE_TOTAL: "total_occurrences_for_dataset",
                # Values: Total of all occurrences for all datasets - stats
                cls.ALL_TOTAL: "total_occurrences_of_all_datasets",
                cls.ALL_MIN_TOTAL: "min_occurrences_of_all_datasets",
                cls.ALL_MIN_TOTAL_NUMBER: "number_of_datasets_with_min_occurrences_of_all",
                cls.ALL_MEAN_TOTAL: "mean_occurrences_of_all_datasets",
                cls.ALL_MEDIAN_TOTAL: "median_occurrences_of_all_datasets",
                cls.ALL_MAX_TOTAL: "max_occurrences_of_all_datasets",
                # ------------
                # COMPARES TO: cls.ONE_COUNT: "total_species_for_dataset",
                # Counts: Count of all species (from all columns/datasets)
                cls.ALL_COUNT: "total_species_count",
                # Species counts for all datasets - stats
                cls.ALL_MIN_COUNT: "min_species_count_of_all_datasets",
                cls.ALL_MEAN_COUNT: "mean_species_count_of_all_datasets",
                cls.ALL_MEDIAN_COUNT: "median_species_count_of_all_datasets",
                cls.ALL_MAX_COUNT: "max_species_count_of_all_datasets",
            }
        elif table_type == species_dataset_summary_type:
            keys = {
                # ----------------------------------------------------------------------
                # Column
                # -----------------------------
                cls.TYPE: "species",
                # One dataset
                cls.ONE_LABEL: "species_label",
                # Count (non-zero elements in column)
                cls.ONE_COUNT: "total_datasets_for_species",
                # Values (total of values in column)
                cls.ONE_TOTAL: "total_occurrences_for_species",
                # Values: Minimum occurrence count for one dataset
                cls.ONE_MIN_COUNT: "min_occurrences_for_species",
                cls.ONE_MIN_COUNT_NUMBER: "number_of_species_with_min_occurrences",
                # Values: Maximum occurrence count for one dataset, species labels, indexes
                cls.ONE_MAX_COUNT: "max_occurrences_for_species",
                cls.ONE_MAX_COUNT_LABELS: "species_with_max_occurrences",
                # -----------------------------
                # All datasets
                # ------------
                # COMPARES TO:  cls.ONE_TOTAL: "total_occurrences_for_dataset",
                # Values: Total of all occurrences for all datasets - stats
                cls.ALL_TOTAL: "total_occurrences_of_all_species",
                cls.ALL_MIN_TOTAL: "min_occurrences_of_all_species",
                cls.ALL_MIN_TOTAL_NUMBER: "number_of_species_with_min_occurrences_of_all",
                cls.ALL_MEAN_TOTAL: "mean_occurrences_of_all_species",
                cls.ALL_MEDIAN_TOTAL: "median_occurrences_of_all_species",
                cls.ALL_MAX_TOTAL: "max_occurrences_of_all_species",
                cls.ALL_MAX_TOTAL_LABELS: "species_with_max_occurrences_of_all",
                # ------------
                # COMPARES TO: cls.ONE_COUNT: "total_species_for_dataset",
                # Counts: Count of all species (from all columns/datasets)
                cls.ALL_COUNT: "total_species_count",
                # Species counts for all datasets - stats
                cls.ALL_MIN_COUNT: "min_species_count_of_all_datasets",
                cls.ALL_MEAN_COUNT: "mean_species_count_of_all_datasets",
                cls.ALL_MEDIAN_COUNT: "median_species_count_of_all_datasets",
                cls.ALL_MAX_COUNT: "max_species_count_of_all_datasets",
            }
        else:
            raise Exception(f"Keys not defined for table {table_type}")
        return keys
