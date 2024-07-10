"""Matrix to summarize each of 2 dimensions of data by counts of the other and a third."""
from collections import OrderedDict
import json
from logging import ERROR, INFO
import os
import pandas as pd
from pandas.api.types import CategoricalDtype
from zipfile import ZipFile

from sppy.tools.s2n.aggregate_data_matrix import _AggregateDataMatrix
from sppy.tools.s2n.constants import (
    MATRIX_SEPARATOR, SNKeys, SUMMARY_FIELDS, Summaries, SUMMARY_TABLE_TYPES
)
from sppy.tools.util.logtools import logit
from sppy.tools.util.utils import upload_to_s3

# .............................................................................
class SummaryMatrix(_AggregateDataMatrix):
    """Class for holding summary counts of each of 2 dimensions of data."""

    # ...........................
    def __init__(
            self, summary_df, table_type, data_datestr, logger=None):
        """Constructor for species by dataset comparisons.

        Args:
            summary_df (pandas.DataFrame): DataFrame with a row for each element in
                category, and 2 columns of data.  Rows headers and column headers are
                labeled.
                * Column 1 contains the count of the number of columns in  that row
                * Column 2 contains the total of values in that row.
            table_type (aws_constants.SUMMARY_TABLE_TYPES): type of aggregated data
            data_datestr (str): date of the source data in YYYY_MM_DD format.
            category (CategoricalDtype): category of unique labels with ordered
                indices/codes for rows in the summary matrix.
            logger (object): An optional local logger to use for logging output
                with consistent options
        """
        self._df = summary_df
        _AggregateDataMatrix.__init__(self, table_type, data_datestr, logger=logger)

    # ...........................
    @classmethod
    def init_from_sparse_matrix(cls, sp_mtx, axis=0, logger=None):
        """Summarize a matrix into counts of one axis and values for the other axis.

        Args:
            sp_mtx (sppy.aws.SparseMatrix): A sparse matrix with count
                values for one aggregator0 (i.e. species) rows (axis 0) by another
                aggregator1 (i.e. dataset) columns (axis 1) to use for computations.
            axis (int): Summarize rows (0) or columns (1).
            logger (object): logger for saving relevant processing messages

        Returns:
            sparse_coo (pandas.DataFrame): DataFrame summarizing rows by the count and
                value of columns, or columns by the count amd value of rows.

        Note:
            The input dataframe must contain only one input record for any x and y value
                combination, and each record must contain another value for the dataframe
                contents.  The function was written for a table of records with
                datasetkey (for the column labels/x), species (for the row labels/y),
                and occurrence count.
        """
        # Column counts and totals (count along axis 0, each row)
        # Row counts and totals (count along axis 1, each column)
        totals = sp_mtx.get_totals(axis=axis).tolist()
        counts = sp_mtx.get_counts(axis=axis).tolist()
        data = {SUMMARY_FIELDS.COUNT: counts, SUMMARY_FIELDS.TOTAL: totals}
        input_table_meta = Summaries.get_table(sp_mtx.table_type)

        # Axis 0 summarizes each column (down axis 0) of sparse matrix
        if axis == 0:
            index = sp_mtx.column_category.categories
            table_type = input_table_meta["column_summary_table"]
        # Axis 1 summarizes each row (across axis 1) of sparse matrix
        elif axis == 1:
            index = sp_mtx.row_category.categories
            table_type = input_table_meta["row_summary_table"]

        # summary fields = columns, sparse matrix axis = rows
        sdf = pd.DataFrame(data=data, index=index)

        summary_matrix = SummaryMatrix(
            sdf, table_type, sp_mtx.data_datestr, logger=logger)
        return summary_matrix

    # ...............................................
    @property
    def num_y_values(self):
        """Get the number of rows.

        Returns:
            int: The count of rows
        """
        return self._df.shape[0]

    # ...............................................
    @property
    def num_x_values(self):
        """Get the number of columns.

        Returns:
            int: The count of columns
        """
        return self._df.shape[1]
    # ...............................................
    def get_random_row_labels(self, count):
        """Get random values from the labels on axis 0 of matrix.

        Args:
            count (int): number of values to return

        Returns:
            labels (list): random row headers
        """
        import random
        size = len(self._df.index)
        # Get a random sample of category indexes (0-based)
        idxs = random.sample(range(size), count)
        labels = [self._df.index(i) for i in idxs]
        return labels


    # .............................................................................
    def compress_to_file(self, local_path="/tmp"):
        """Compress this SparseMatrix to a zipped npz and json file.

        Args:
            local_path (str): Absolute path of local destination path

        Returns:
            zip_fname (str): Local output zip filename.

        Raises:
            Exception: on failure to write dataframe to CSV file.
            Exception: on failure to serialize or write metadata as JSON.
            Exception: on failure to write matrix and metadata files to zipfile.
        """
        mtx_fname, meta_fname, zip_fname = self._get_input_files(
            local_path="/tmp", do_delete=True)

        # Save matrix to csv locally
        try:
            self._df.to_csv(mtx_fname, sep=MATRIX_SEPARATOR)
        except Exception as e:
            msg = f"Failed to write {mtx_fname}: {e}"
            self._logme(msg, log_level=ERROR)
            raise Exception(msg)

        # Save table data and categories to json locally
        metadata = Summaries.get_table(self._table_type)
        try:
            self._dump_metadata(metadata, meta_fname)
        except Exception:
            raise

        # Compress matrix with metadata
        try:
            self._compress_files([mtx_fname, meta_fname], zip_fname)
        except Exception:
            raise

        return zip_fname

    # .............................................................................
    @classmethod
    def uncompress_zipped_data(
            cls, zip_filename, local_path="/tmp", overwrite=False):
        """Uncompress a zipped SparseMatrix into a coo_array and row/column categories.

        Args:
            zip_filename (str): Filename of output data to write to S3.
            local_path (str): Absolute path of local destination path
            overwrite (bool): Flag indicating whether to use existing files unzipped
                from the zip_filename.

        Returns:
            dataframe (pandas.DataFrame): dataframe containing summary matrix data.
            table_type (aws.aws_constants.SUMMARY_TABLE_TYPES): type of table data
            data_datestr (str): date string in format YYYY_MM_DD

        Raises:
            Exception: on missing input zipfile
            Exception: on missing expected file from zipfile
            Exception: on unable to load CSV file
            Exception: on unable to load JSON metadata file
            Exception: on missing table_type code in JSON
            Exception: on bad table_type code in JSON

        Note:
            All filenames have the same basename with extensions indicating which data
                they contain. The filename contains a string like YYYY-MM-DD which
                indicates which GBIF data dump the statistics were built upon.
        """
        mtx_fname, meta_fname, table_type, data_datestr = cls._uncompress_files(
            zip_filename, local_path, overwrite=overwrite)

        # Read dataframe from local CSV file
        try:
            dataframe = pd.read_csv(mtx_fname, sep=MATRIX_SEPARATOR)
        except Exception as e:
            raise Exception(f"Failed to load {mtx_fname}: {e}")
        # Read JSON dictionary as string
        try:
            meta_dict = cls.load_metadata(meta_fname)
        except Exception:
            raise

        return dataframe, meta_dict, table_type, data_datestr

    # ...........................
    def rank_summary_counts(self, sort_by, order="descending", limit=10):
        """Order records by sort_by field and return the top or bottom limit records.

        Args:
            sort_by (str): field containing value to sort on.
            order (str): return records, sorted from top (descending) or bottom
                (ascending).
            limit (int): number of records to return.

        Returns:
            ordered_rec_dict (OrderedDict): records containing all fields, sorted by the
                sort_by field.
        """
        # One per axis
        measure_flds = self._keys["fields"].copy()
        try:
            measure_flds.remove(sort_by)
        except ValueError:
            raise Exception(
                f"Field {sort_by} does not exist; sort by one of {self._keys['fields']}")
        if order == "descending":
            sorted_df = self._df.nlargest(limit, sort_by, keep="all")
        elif order == "ascending":
            sorted_df = self._df.nsmallest(limit, sort_by, keep="all")
        else:
            raise Exception(
                f"Order {sort_by} does not exist, use 'ascending' or 'descending')")
        rec_dict =  sorted_df.to_dict()
        ordered_rec_dict = OrderedDict()
        for k, v in rec_dict[sort_by]:
            # Insert records from sorted field first, in order returned
            ordered_rec_dict[k] = {sort_by: v}
            # measure_flds now contains all but sort_by
            # add other fields and values to the new records
            for other_fld in measure_flds:
                ordered_rec_dict[k][other_fld] = rec_dict[other_fld][k]
        return ordered_rec_dict

    # ...............................................
    def _logme(self, msg, refname="", log_level=None):
        logit(self._logger, msg, refname=refname, log_level=log_level)

"""
import pandas as pd
import scipy
from sppy.aws.sparse_matrix import *

d = {
    'a': [10, 0, 1, 6, 0], 
    'b': [0, 2, 13, 0, 2], 
    'c': [5, 0, 0, 0, 6], 
    'd': [0, 0, 15, 0, 0]
}

df = pd.DataFrame(data=d)
df.index = ['u', 'v', 'x', 'y', 'z']

sp_mtx = scipy.sparse.coo_array(df)

axis = 0
totals0 = sp_mtx.sum(axis=axis)
counts0 = sp_mtx.getnnz(axis=axis)
idx0 = df.columns
data0 = {"counts": counts0, "totals": totals0}
sdf0 = pd.DataFrame(data=data0, index=idx0)

limit = 3
order="descending"

sorted_df = sdf0.sort_values(by=COUNT_FLD, axis=0, ascending=(order == "ascending"))
recs_df = sorted_df.head(limit)
        
axis = 1
totals1 = sp_mtx.sum(axis=axis)
counts1 = sp_mtx.getnnz(axis=axis)
idx1 = df.index
data1 = {"counts": counts1, "totals": totals1}
sdf1 = pd.DataFrame(data=data1, index=idx1)

sorted_df = sdf1.sort_values(by=COUNT_FLD, axis=0, ascending=(order == "ascending"))
recs_df = sorted_df.head(limit)

"""