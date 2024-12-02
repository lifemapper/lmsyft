"""Constants for Specify Network Analyst Data."""

# .............................................................................
# Data constants
# .............................................................................
COUNT_FLD = "count"
TOTAL_FLD = "total"

OCCURRENCE_COUNT_FLD = "occ_count"
SPECIES_COUNT_FLD = "species_count"

UNIQUE_SPECIES_FLD = "taxonkey_species"
SPECIES_CODE = "species"
# .............................................................................
class DATA_DIM:
    """All dimensions (besides species) with columns used for data analyses."""
    SPECIES = {
        "code": "species",
        "key_fld": UNIQUE_SPECIES_FLD
    }

# .............................................................................
class SUMMARY:
    """Types of tables stored in S3 for aggregate species data analyses."""
    dt_token = "YYYY_MM_DD"
    sep = "_"
    dim_sep = f"{sep}x{sep}"
    DATATYPES = AGGREGATION_TYPE.all()
    SPECIES_DIMENSION = DATA_DIM.SPECIES["code"]
    SPECIES_FIELD = DATA_DIM.SPECIES["key_fld"]
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
            Exception: on datatype "matrix", dim1 != SPECIES_DIMENSION
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
                "species_fld": UNIQUE_SPECIES_FLD,
                "value_fld": OCCURRENCE_COUNT_FLD
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
                "occurrence_count_fld": OCCURRENCE_COUNT_FLD,
                "species_count_fld": SPECIES_COUNT_FLD,
                "fields": [dim0["key_fld"], OCCURRENCE_COUNT_FLD, SPECIES_COUNT_FLD]
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
        species_code = ANALYSIS_DIM.species_code()
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
                    "fields": [COUNT_FLD, TOTAL_FLD],
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
        dim1 = ANALYSIS_DIM.species_code()
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
                "value": OCCURRENCE_COUNT_FLD,
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
        dim1 = ANALYSIS_DIM.species_code()
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
        #       Note bison.spnet.pam_matrix.PAM
        pams = {}
        for analysis_code in cls.ANALYSIS_DIMENSIONS:
            dim0 = analysis_code
            dim1 = ANALYSIS_DIM.species_code()
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
                meta_cpy = copy.deepcopy(meta)
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
        try:
            table = cls.tables()[table_type]
        except KeyError:
            raise Exception(f"Invalid table_type {table_type}")
        cpy_table = copy.deepcopy(table)
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
        table = cls.tables()[table_type]
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
