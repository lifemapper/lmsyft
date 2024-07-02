"""Matrix to summarize each of 2 dimensions of data by counts of the other and a third."""
import boto3
from botocore.exceptions import ClientError, SSLError
import json
from logging import ERROR, INFO
import numpy as np
import os
import pandas as pd
from pandas.api.types import CategoricalDtype
import random
import scipy.sparse
from zipfile import ZipFile

from sppy.aws.aws_constants import (SNKeys, Summaries)
from sppy.aws.sparse_matrix import SparseMatrix
from sppy.tools.util.logtools import logit
from sppy.tools.s2n.utils import convert_np_vals_for_json

COUNT_FLD = "count"
TOTAL_FLD = "total"
# .............................................................................
class SummaryMatrix:
    """Class for holding summary counts of each of 2 dimensions of data."""

    # ...........................
    def __init__(
            self, summary_df, table_type, data_datestr, axis, category=None,
            logger=None):
        """Constructor for species by dataset comparisons.

        Args:
            summary_df (pandas.DataFrame): DataFrame with 2 rows and 2 columns of data.
                * Column 1 contains the count of the number of columns in  that row
                * Column 2 contains the total of values in that row.
                * Row 1 contains the count of the number of rows in that column
                * Row 2 contains the total of values in that row.
            table_type (aws_constants.SUMMARY_TABLE_TYPES): type of aggregated data
            data_datestr (str): date of the source data in YYYY_MM_DD format.
            axis (int): row (0) or column (1) for summaries.
            category (CategoricalDtype): category of unique labels with ordered
                indices/codes for rows (y, axis 0) or columns (x, axis 1)
            logger (object): An optional local logger to use for logging output
                with consistent options

        Note: in the first implementation, summaries are stored on the axis of the
            original sparse matrix.  Species are generally far more numerous, so rows
            are always species, columns are datasets.
        """
        self._df = summary_df
        self._table_type = table_type
        self._data_datestr = data_datestr
        self._keys = SNKeys.get_keys_for_table(self._table_type)
        self._axis = axis
        self._categ = category
        self._logger = logger
        self._report = {}

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
        totals = sp_mtx.get_totals(axis=axis)
        counts = sp_mtx.get_counts(axis=axis)
        data = {COUNT_FLD: counts, TOTAL_FLD: totals}

        if axis == 0:
            index = sp_mtx.column_category.categories
        elif axis == 1:
            index = sp_mtx.row_category.categories

        sdf = pd.DataFrame(data=data, index=index)

        summary_matrix = SummaryMatrix(
            sdf, sp_mtx.table_type, sp_mtx.data_datestr, logger=logger)
        return summary_matrix


    # ...........................
    def rank_summary_counts(self, sort_by=COUNT_FLD, order="descending", limit=10):
        measure_flds = [COUNT_FLD, TOTAL_FLD]
        if sort_by not in measure_flds:
            raise Exception(
                f"Field {sort_by} does not exist; sort by {COUNT_FLD} or {TOTAL_FLD}")
        if order == "descending":
            sorted_df = self._df.nlargest(limit, sort_by, keep="all")
        elif order == "ascending":
            sorted_df = self._df.nsmallest(limit, sort_by, keep="all")
        else:
            raise Exception(
                f"Order {sort_by} does not exist, use 'ascending' or 'descending')")
        rec_dict =  sorted_df.to_dict()
        flds = rec_dict.keys()
        # for k, v in rec_dict[sort_by]:
        #     r = {"name": k, sort_by: val}
        #     for item_name, val in valdict.items():
        #         r = {item_name:}
        recs = []
        return recs



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