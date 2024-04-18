"""Tools to compute dataset x species statistics from S3 data."""
import boto3
from botocore.exceptions import ClientError, SSLError
import io
from logging import ERROR, INFO, WARNING
import numpy as np
import os
import pandas as pd
from pandas.api.types import CategoricalDtype
import random
import scipy.sparse

from sppy.aws.aws_constants import (
    LOCAL_OUTDIR, PROJ_BUCKET, REGION, RowColumnComparisonKeys, SUMMARY_FOLDER,
    Summaries, SUMMARY_TABLE_TYPES)
from sppy.aws.aws_tools import get_current_datadate_str, get_today_str
from sppy.tools.util.logtools import Logger, logit


# ...............................................
def _sum_stacked_data_vals_for_column(stacked_df, filter_label, filter_value, val_label):
    """Sum the values for rows where column 'filter_label' = 'filter_value'.

    Args:
        stacked_df: dataframe containing stacked data records
        filter_label: column name for filtering.
        filter_value: column value for filtering.
        val_label: column name for summation.

    Returns:
        tmp_df: dataframe containing only rows with a value of filter_value in column
            filter_label.
    """
    # Create a dataframe of rows where column 'filter_label' = 'filter_value'.
    tmp_df = stacked_df.loc[stacked_df[filter_label] == filter_value]
    # Sum the values for those rows
    count = tmp_df[val_label].sum()
    return count


# ...............................................
def _get_random_values_from_stacked_data(stacked_df, col_label, count):
    # Get a random sample of row indexes
    row_idxs = random.sample(range(1, stacked_df.shape[0]), count)
    x_vals = [stacked_df[col_label][i] for i in row_idxs]
    return x_vals


# ...............................................
def test_stacked_to_aggregate(
        stacked_df, x_col_label, y_col_label, val_col_label, aggregate_sparse_mtx):
    """Test for equality of sums in stacked and aggregated dataframes.

    Args:
        stacked_df: dataframe of stacked data, containing records with columns of
            categorical values and counts.
        x_col_label: column label in stacked_df to be used as the column labels of
            the aggregate_df
        y_col_label: column label in stacked_df to be used as the row labels of
            the aggregate_df
        val_col_label: column label in stacked_df to be used as the row labels of
            the aggregate_df, aggregate_df
        aggregate_sparse_mtx (SparseMatrix): object containing a scipy.sparse.coo_array
            with 3 columns from the stacked_df arranged as rows and columns with values

    Postcondition:
        Printed information for successful or failed tests.

    Note: The aggregate_df must have been created from the stacked_df.
    """
    test_count = 5
    x_vals = _get_random_values_from_stacked_data(stacked_df, x_col_label, test_count)
    y_vals = _get_random_values_from_stacked_data(stacked_df, y_col_label, test_count)
    #
    # Test stacked column totals against aggregate x columns
    for x_col in x_vals:
        stk_sum = _sum_stacked_data_vals_for_column(
            stacked_df, x_col_label, x_col, val_col_label)
        agg_sum = aggregate_sparse_mtx[x_col].sum()
        if stk_sum == agg_sum:
            print(
                f"Total {stk_sum}: Stacked data for {x_col_label} = {x_col} == "
                f"aggregate data in column {x_col}"
            )
        else:
            print(
                f"{stk_sum} != {agg_sum}: Stacked data for {x_col_label} = {x_col} != "
                f"aggregate data in column {x_col}"
            )
    # Test stacked column totals against aggregate y rows
    for y_col in y_vals:
        stk_sum = _sum_stacked_data_vals_for_column(
            stacked_df, y_col_label, y_col, val_col_label)
        agg_sum = aggregate_sparse_mtx.loc[y_col].sum()
        if stk_sum == agg_sum:
            print(
                f"Total {stk_sum}: Stacked data for {x_col_label} = {y_col} ==  "
                f"aggregate data in column {y_col}"
            )
        else:
            print(
                f"{stk_sum} != {agg_sum}: Stacked data for {x_col_label} = {y_col} !=  "
                f"aggregate data in column {y_col}"
            )


# .............................................................................
def read_s3_parquet_to_pandas(
        bucket, bucket_path, filename, logger=None, s3_client=None, region=REGION, **args):
    """Read a parquet file from a folder on S3 into a pd DataFrame.

    Args:
        bucket (str): Bucket identifier on S3.
        bucket_path (str): Folder path to the S3 parquet data.
        filename (str): Filename of parquet data to read from S3.
        logger (object): logger for saving relevant processing messages
        s3_client (object): object for interacting with Amazon S3.
        region (str): AWS region to query.
        args: Additional arguments to be sent to the pd.read_parquet function.

    Returns:
        pd.DataFrame containing the tabular data.
    """
    s3_key = f"{bucket_path}/{filename}"
    if s3_client is None:
        s3_client = boto3.client("s3", region_name=region)
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=s3_key)
    except SSLError:
        logit(
            logger, f"Failed with SSLError getting {bucket}/{s3_key} from S3",
            log_level=ERROR)
    except ClientError as e:
        logit(
            logger, f"Failed to get {bucket}/{s3_key} from S3, ({e})",
            log_level=ERROR)
    else:
        logit(logger, f"Read {bucket}/{s3_key} from S3")
    dataframe = pd.read_parquet(io.BytesIO(obj["Body"].read()), **args)
    return dataframe


# .............................................................................
def read_s3_multiple_parquets_to_pandas(
        bucket, bucket_path, logger=None, s3=None, s3_client=None, region=REGION, **args):
    """Read multiple parquets from a folder on S3 into a pd DataFrame.

    Args:
        bucket (str): Bucket identifier on S3.
        bucket_path (str): Parent folder path to the S3 parquet data.
        logger (object): logger for saving relevant processing messages
        s3 (object): Connection to the S3 resource
        s3_client (object): object for interacting with Amazon S3.
        region: AWS region to query.
        args: Additional arguments to be sent to the pd.read_parquet function.

    Returns:
        pd.DataFrame containing the tabular data.
    """
    if not bucket_path.endswith("/"):
        bucket_path = bucket_path + "/"
    if s3_client is None:
        s3_client = boto3.client("s3", region_name=region)
    if s3 is None:
        s3 = boto3.resource("s3", region_name=region)

    s3_keys = [
        item.key for item in s3.Bucket(bucket).objects.filter(Prefix=bucket_path)
        if item.key.endswith(".parquet")]
    if not s3_keys:
        logit(
            logger, f"No parquet found in {bucket} {bucket_path}",
            log_level=ERROR)
        return None

    dfs = [
        read_s3_parquet_to_pandas(
            bucket, bucket_path, key, logger, s3_client=s3_client, region=region,
            **args) for key in s3_keys
    ]
    return pd.concat(dfs, ignore_index=True)


# # .............................................................................
# def write_npz_array_to_s3(
#         sparse_coo_array, bucket, bucket_path, filename, logger=None):
#     """Write a pd DataFrame to CSV or parquet on S3.
#
#     Args:
#         sparse_coo_array (scipy.coo_array): matrix in COO format to write to S3.
#         bucket (str): Bucket identifier on S3.
#         bucket_path (str): Folder path to the S3 output data.
#         filename (str): Filename of output data to write to S3.
#         logger (object): logger for saving relevant processing messages
#     """
#     s3_filename = None
#     tmp_fname = f"/tmp/{filename}"
#     if os.path.exists(tmp_fname):
#         os.remove(tmp_fname)
#     try:
#         scipy.sparse.save_npz(tmp_fname, sparse_coo_array, compressed=True)
#     except Exception as e:
#         logit(
#             logger, f"Failed to write {tmp_fname}: {e}", log_level=ERROR)
#     else:
#         s3_filename = upload_to_s3(tmp_fname, bucket, bucket_path, logger=logger)
#
#     return s3_filename

# # .............................................................................
# def read_npz_array_from_s3(
#         bucket, bucket_path, filename, local_path=None, region=REGION, logger=None, **args):
#     """Write a pd DataFrame to CSV or parquet on S3.
#
#     Args:
#         bucket (str): Bucket identifier on S3.
#         bucket_path (str): Folder path to the S3 output data.
#         filename (str): Filename of output data to write to S3.
#         local_path (str): local path for temporary download of npz file.
#         region (str): AWS region to query.
#         logger (object): logger for saving relevant processing messages
#         args: Additional arguments to be sent to the pd.read_parquet function.
#
#     Returns:
#         sparse_coo_array (scipy.coo_array): matrix in COO format.
#     """
#     sparse_coo = None
#     if local_path is None:
#         local_path = os.getcwd()
#     tmp_fname = download_from_s3(
#         bucket, bucket_path, filename, local_path, region=region, overwrite=True)
#     try:
#         sparse_coo = scipy.sparse.load_npz(tmp_fname)
#     except Exception as e:
#         logit(logger, f"Failed to read {tmp_fname}: {e}", log_level=ERROR)
#
#     return sparse_coo


# # .............................................................................
# def upload_to_s3(full_filename, bucket, bucket_path, region=REGION, logger=None):
#     """Upload a file to S3.
#
#     Args:
#         full_filename (str): Full filename to the file to upload.
#         bucket (str): Bucket identifier on S3.
#         bucket_path (str): Parent folder path to the S3 data.
#         region (str): AWS region to query.
#         logger (object): logger for saving relevant processing messages
#
#     Returns:
#         s3_filename (str): path including bucket, bucket_folder, and filename for the
#             uploaded data
#     """
#     s3_filename = None
#     s3_client = boto3.client("s3", region_name=region)
#     obj_name = os.path.basename(full_filename)
#     if bucket_path:
#         obj_name = f"{bucket_path}/{obj_name}"
#     try:
#         s3_client.upload_file(full_filename, bucket, obj_name)
#     except SSLError:
#         logit(
#             logger, f"Failed with SSLError to upload {obj_name} to {bucket}",
#             log_level=ERROR)
#     except ClientError as e:
#         logit(
#             logger, f"Failed to upload {obj_name} to {bucket}, ({e})",
#             log_level=ERROR)
#     else:
#         s3_filename = f"s3://{bucket}/{obj_name}"
#         logit(logger, f"Uploaded {s3_filename} to S3")
#     return s3_filename
#

# .............................................................................
def download_from_s3(bucket, bucket_path, filename, local_path, region=REGION, overwrite=True):
    """Download a file from S3 to a local file.

    Args:
        bucket (str): Bucket identifier on S3.
        bucket_path (str): Folder path to the S3 parquet data.
        filename (str): Filename of data to read from S3.
        local_path (str): local path for download.
        region (str): AWS region to query.
        overwrite (boolean):  flag indicating whether to overwrite an existing file.

    Returns:
        local_filename (str): full path to local filename containing downloaded data.
    """
    local_filename = os.path.join(local_path, filename)
    obj_name = f"{bucket_path}/{filename}"
    # Delete if needed
    if os.path.exists(local_filename):
        if overwrite is True:
            os.remove(local_filename)
        else:
            print(f"{local_filename} already exists")
    # Download current
    if not os.path.exists(local_filename):
        s3_client = boto3.client("s3", region_name=region)
        try:
            s3_client.download_file(bucket, obj_name, local_filename)
        except SSLError:
            logit(
                logger, f"Failed with SSLError to download s3://{bucket}/{obj_name}",
                log_level=ERROR)
        except ClientError as e:
            logit(
                logger,
                f"Failed to download s3://{bucket}/{obj_name}, ({e})")
        else:
            print(f"Downloaded {filename} from S3 to {local_filename}")
    return local_filename


# # ...............................................
# def create_sparse_df_from_stacked_data(orig_df, x_fld, y_fld, val_fld):
#     """Create a dataframe of rows by columns containing values from a table.
#
#     Args:
#         orig_df (pandas.DataFrame): DataFrame of records containing columns to be used
#             as the new rows, new columns, and values.
#         x_fld: column in the input dataframe containing values to be used as
#             columns (axis 1)
#         y_fld: column in the input dataframe containing values to be used as rows
#             (axis 0)
#         val_fld: : column in the input dataframe containing values to be used as
#             values for the intersection of x and y fields
#
#     Returns:
#         sdf (pandas.SparseDataFrame): dataframe of y values (rows, y axis=0) by
#             x values (columnns, x axis=1), with values from another column.
#
#     Note:
#         The input dataframe must contain only one input record for any x and y value
#             combination, and each record must contain another value for the dataframe
#             contents.  The function was written for a table of records with
#             datasetkey (for the column labels/x), species (for the row labels/y),
#             and occurrence count.
#     """
#     # Get unique values to use as categories for scipy column and row indexes, remove None
#     unique_x_vals = list(orig_df[x_fld].dropna().unique())
#     unique_y_vals = list(orig_df[y_fld].dropna().unique())
#
#     # Categories allow using codes as the integer index for scipy matrix
#     y_categ = CategoricalDtype(unique_y_vals, ordered=True)
#     x_categ = CategoricalDtype(unique_x_vals, ordered=True)
#
#     # Create a list of category codes matching original stacked data records to replace
#     #   column names from stacked data dataframe with integer codes for row and column
#     #   indexes in the new scipy matrix
#     col_idx = orig_df[x_fld].astype(x_categ).cat.codes
#     row_idx = orig_df[y_fld].astype(y_categ).cat.codes
#
#     # This compresses original rows with the same y_fld values and multiple
#     #   x_fld values into a row (y) with different values (counts) for the columns (x)
#     sparse_matrix = scipy.sparse.csr_array(
#         (orig_df[val_fld], (row_idx, col_idx)),
#         shape=(y_categ.categories.size, x_categ.categories.size))
#
#     sdf = pd.DataFrame.sparse.from_spmatrix(
#         sparse_matrix, index=y_categ.categories, columns=x_categ.categories)
#
#     return sdf

# .............................................................................
# .............................................................................
class SparseMatrix:
    """Class for managing computations for counts of aggregator0 x aggregator1."""

    # ...........................
    def __init__(
            self, sparse_coo_array, table_type, row_category=None, column_category=None,
            logger=None):
        """Constructor for species by dataset comparisons.

        Args:
            sparse_coo_array (scipy.sparse.coo_array): A 2d sparse array with count
                values for one aggregator0 (i.e. species) rows (axis 0) by another
                aggregator1 (i.e. dataset) columns (axis 1) to use for computations.
            table_type (Summaries.TABLE_TYPE):
            row_category (CategoricalDtype): category of unique labels with ordered
                indices/codes for rows (y, axis 0)
            column_category (CategoricalDtype): category of unique labels with ordered
                indices/codes for columns (x, axis 1)
            logger (object): An optional local logger to use for logging output
                with consistent options
        """
        self._coo_array = sparse_coo_array
        self._table_type = table_type
        self._row_categ = row_category
        self._col_categ = column_category
        self._logger = logger
        self._report = {}

    # ...........................
    @classmethod
    def init_from_stacked_data(cls, stacked_df, x_fld, y_fld, val_fld):
        """Create a sparse matrix of rows by columns containing values from a table.

        Args:
            stacked_df (pandas.DataFrame): DataFrame of records containing columns to be used
                as the new rows, new columns, and values.
            x_fld: column in the input dataframe containing values to be used as
                columns (axis 1)
            y_fld: column in the input dataframe containing values to be used as rows
                (axis 0)
            val_fld: : column in the input dataframe containing values to be used as
                values for the intersection of x and y fields

        Returns:
            sparse_coo (scipy.coo_array): matrix of y values (rows, y axis=0) by
                x values (columnns, x axis=1), with values from another column.

        Note:
            The input dataframe must contain only one input record for any x and y value
                combination, and each record must contain another value for the dataframe
                contents.  The function was written for a table of records with
                datasetkey (for the column labels/x), species (for the row labels/y),
                and occurrence count.
        """
        # Get unique values to use as categories for scipy column and row indexes, remove None
        unique_x_vals = list(stacked_df[x_fld].dropna().unique())
        unique_y_vals = list(stacked_df[y_fld].dropna().unique())
        # Categories allow using codes as the integer index for scipy matrix
        y_categ = CategoricalDtype(unique_y_vals, ordered=True)
        x_categ = CategoricalDtype(unique_x_vals, ordered=True)
        # Create a list of category codes matching original stacked data records to replace
        #   column names from stacked data dataframe with integer codes for row and column
        #   indexes in the new scipy matrix
        col_idx = stacked_df[x_fld].astype(x_categ).cat.codes
        row_idx = stacked_df[y_fld].astype(y_categ).cat.codes
        # This creates a new matrix in Coordinate list (COO) format.  COO stores a list of
        # (row, column, value) tuples.  Convert to CSR or CSC for efficient Row or Column
        # slicing, respectively
        sparse_coo = scipy.sparse.coo_array(
            (stacked_df[val_fld], (row_idx, col_idx)),
            shape=(y_categ.categories.size, x_categ.categories.size))
        sparse_matrix = SparseMatrix(sparse_coo, y_categ, x_categ)
        return sparse_matrix

    # .............................................................................
    def _to_dataframe(self):
        sdf = pd.DataFrame.sparse.from_spmatrix(
            self._coo_array,
            index=self._row_categ.categories,
            columns=self._col_categ.categories)
        return sdf

    # .............................................................................
    @classmethod
    def init_from_s3(
            cls, bucket, bucket_path, filename, local_path=None, region=REGION, logger=None):
        """Write a pd DataFrame to CSV or parquet on S3.

        Args:
            bucket (str): Bucket identifier on S3.
            bucket_path (str): Folder path to the S3 output data.
            filename (str): Filename of output data to write to S3.
            local_path (str): local path for temporary download of npz file.
            region (str): AWS region to query.
            logger (object): logger for saving relevant processing messages
            args: Additional arguments to be sent to the pd.read_parquet function.

        Returns:
            sparse_coo_array (scipy.coo_array): matrix in COO format.
        """
        sparse_coo = None
        if local_path is None:
            local_path = os.getcwd()
        tmp_fname = download_from_s3(
            bucket, bucket_path, filename, local_path, region=region, overwrite=True)
        try:
            sparse_coo = scipy.sparse.load_npz(tmp_fname)
        except Exception as e:
            logit(logger, f"Failed to read {tmp_fname}: {e}", log_level=ERROR)

        sparse_matrix = SparseMatrix(sparse_coo)

        return sparse_coo

    # ...............................................
    def _get_code_from_category(self, label, axis=0):
        if axis == 0:
            categ = self._row_categ
        elif axis == 1:
            categ = self._col_categ
        else:
            raise Exception("Axis must be 0 or 1")
        code = None
        # returns a tuple of a single 1-dimensional array of locations
        arr = np.where(categ.categories == label)[0]
        try:
            # labels are unique in categories so there will be 0 or 1 value in the array
            code = arr[0]
        except:
            pass
        return code

    # ...............................................
    def _get_category_from_code(self, code, axis=0):
        if axis == 0:
            categ = self._row_categ
        elif axis == 1:
            categ = self._col_categ
        else:
            raise Exception("Axis must be 0 or 1")
        category = categ.categories[code]
        return category

    # ...............................................
    def _get_categories_from_code(self, code_list, axis=0):
        if axis == 0:
            categ = self._row_categ
        elif axis == 1:
            categ = self._col_categ
        else:
            raise Exception("Axis must be 0 or 1")
        category_labels = []
        for code in code_list:
            category_labels.append(categ.categories[code])
        return category_labels

    # ...............................................
    def _logme(self, msg, refname="", log_level=None):
        logit(self._logger, msg, refname=refname, log_level=log_level)

    # ...........................
    def _to_csr(self):
        # Convert to CSR format for efficient row slicing
        csr = self._coo_array.tocsr()
        return csr

    # ...........................
    def _to_csc(self):
        # Convert to CSC format for efficient column slicing
        csc = self._coo_array.tocsr()
        return csc

    # ...............................................
    @property
    def num_y_values(self):
        """Get the number of rows.

        Returns:
            int: The count of rows where the value > 0 in at least one column.

        Note:
            Also used as gamma diversity (species richness over entire landscape)
        Note: because the sparse data will only from contain unique rows and columns
            with data, this should ALWAYS equal the number of rows
        """
        return self._coo_array.shape[0]

    # ...............................................
    @property
    def num_x_values(self):
        """Get the number of columns.

        Returns:
            int: The count of columns where the value > 0 in at least one row

        Note: because the sparse data will only from contain unique rows and columns
            with data, this should ALWAYS equal the number of columns
        """
        return self._coo_array.shape[1]

    # ...............................................
    def get_column_from_label(self, col_label):
        """Return the column with label `col_label`.

        Args:
            col_label: label for column of interest

        Returns:
            column(scipy.sparse.csr_array): 1-d array of the column for 'col_label'.
        """
        col_idx = self._get_code_from_category(col_label, axis=1)
        column = self._coo_array.getcol(col_idx)
        return column, col_idx

    # ...............................................
    def get_row_from_label(self, row_label):
        """Return the row with label `row_label`.

        Args:
            row_label: label for row of interest

        Returns:
            row (scipy.sparse.csr_array): 1-d array of the row for 'row_label'.
        """
        row_idx = self._get_code_from_category(row_label, axis=0)
        row = self._coo_array.getrow(row_idx)
        return row, row_idx

    # ...............................................
    @property
    def sum_column(self, col_label):
        """Get the total of values in a single column.

        Args:
            col_label: label on the column to total.

        Returns:
            int: The total of all values in one column
        """
        col = self.get_column(col_label)
        total = col.sum()
        return total

    # ...............................................
    def sum_row(self, row_label):
        """Get the total of values in a single row.

        Args:
            row_label: label on the row to total.

        Returns:
            int: The total of all values in one row
        """
        row, row_idx = self.get_row_from_label(row_label)
        total = row.sum(axis=1)
        return total

    # ...............................................
    @property
    def get_all_row_counts(self):
        """Get an array of totals for each row.

        Returns:
            (numpy.ndarray): array of totals of all rows.

        Note:
            The column (1-d array) returned will have totals ordered by row.
        """
        # Sum all rows to return a column (axis=1)
        return self._coo_array.sum(axis=1)

    # ...............................................
    @property
    def get_all_col_counts(self):
        """Return stats (min, max, mean) of totals and counts for all columns.

        Returns:
            all_col_stats (numpy.ndarray): array of totals of all columns.
            counts (numpy.ndarray): array of number of values in all columns.

        Note:
            The rows (1-d arrays) returned will have totals/counts ordered by
                column index.
        """
        keys = RowColumnComparisonKeys.AGG[self._table_type]
        # Sum all columns to return a row (numpy.ndarray, axis=0)
        all_totals = self._coo_array.sum(axis=0)
        # Get number of non-zero entries for every column (row, numpy.ndarray)
        all_counts = self._coo_array.getnnz(axis=0)
        all_col_stats = {
            keys.COL_TOTAL_MIN: all_totals.min(),
            keys.COL_TOTAL_MAX: all_totals.max(),
            keys.COL_TOTAL_MEAN: all_totals.mean(),
            keys.COL_COUNT_MIN: all_counts.min(),
            keys.COL_COUNT_MAX: all_counts.max(),
            keys.COL_COUNT_MEAN: all_counts.mean()
        }
        return all_col_stats


    # ...............................................
    @property
    def get_column_stats(self, col_label):
        """Get a dictionary of statistics for the column with this col_label.

        Args:
            col_label: label on the column to gather stats for.

        Returns:
            stats (dict): quantitative measures of the column.

        Note:
            Inline comments are specific to a SUMMARY_TABLE_TYPES.SPECIES_DATASET_MATRIX
                with row/column/value = species/dataset/occ_count
        """
        stats = {}
        keys = RowColumnComparisonKeys.AGG[self._table_type]
        # Get column (sparse array), and its index
        col, col_idx = self.get_column_from_label(col_label)
        # Return the column index and label
        stats[keys.COL_INDEX] = col_idx
        stats[keys.COL_LABEL] = col_label
        # Total Occurrence count for Dataset
        total = col.sum()
        stats[keys.COL_TOTAL] = total
        # Largest Occurrence count for Dataset
        mx = col.max()
        stats[keys.COL_MAX_COUNT] = mx
        # Species with largest Occurrence count for Dataset
        max_idxs = list((col == mx).nonzero()[0])
        row_labels = self._get_categories_from_code(max_idxs, axis=0)
        # Return indexes (for further matrix examination) and labels (for people)
        stats[keys.COL_MAX_INDEXES] = max_idxs
        stats[keys.COL_MAX_LABELS] = row_labels
        return stats

    # ...............................................
    @property
    def get_row_stats(self, row_label):
        """Get a dictionary of statistics for the row with this row_label.

        Args:
            row_label: label on the row to gather stats for.

        Returns:
            stats (dict): quantitative measures of the row.

        Note:
            Inline comments are specific to a SUMMARY_TABLE_TYPES.SPECIES_DATASET_MATRIX
                with row/column/value = species/dataset/occ_count
        """
        stats = {}
        keys = RowColumnComparisonKeys.AGG[self._table_type]
        # Get row (sparse array), and its index
        row, row_idx = self.get_row_from_label(row_label)
        # Return the row index and label
        stats[keys.ROW_INDEX] = row_idx
        stats[keys.ROW_LABEL] = row_label
        # Total Occurrence count for this Species
        total = row.sum()
        stats[keys.ROW_TOTAL] = total
        # Count of Datasets containing this Species
        count = row.nnz
        stats[keys.ROW_COUNT] = count
        # Largest Occurrence count for this Species
        mx = row.max()
        stats[keys.ROW_MAX_COUNT] = mx
        # Datasets with largest Occurrence count for this Species
        max_idxs = list((row == mx).nonzero()[0])
        max_labels = self._get_categories_from_code(max_idxs, axis=1)
        # Return indexes (for further matrix examination) and labels (for people)
        stats[keys.ROW_MAX_INDEXES] = max_idxs
        stats[keys.ROW_MAX_LABELS] = max_labels
        return stats

    # ...............................................
    @property
    def compare_column_to_others(self, col_label):
        """Compare the number of rows and counts in rows to those of other columns.

        Args:
            col_label: label on the column to compare.

        Returns:
            comparisons (dict): comparison measures
        """
        comparisons = {}
        keys = RowColumnComparisonKeys.AGG[self._table_type]
        # Get this column stats
        stats = self.get_column_stats(col_label)
        print(stats)
        # Get other column totals and counts
        all_totals, all_counts = self.get_all_col_counts()

        this_total = stats[keys.COL_TOTAL]
        this_count = stats[keys.COL_COUNT]
        print()
        # TODO: Compare the rows (species) returned by this column's max counts against
        # with other columns (datasets) counts of those rows (species)
        return comparisons


    # ...............................................
    def compare_row_to_others(self, row_label):
        """Compare the number of columns and counts in columns to those of other rows.

        Args:
            row_label: label on the row to compare.

        Returns:
            comparisons (dict): comparison measures
        """
        comparisons = {}
        row_stats = self.get_row_stats(row_label)
        return comparisons

    # .............................................................................
    def _upload_to_s3(self, full_filename, bucket, bucket_path, region):
        """Upload a file to S3.

        Args:
            full_filename (str): Full filename to the file to upload.
            bucket (str): Bucket identifier on S3.
            bucket_path (str): Parent folder path to the S3 data.
            region (str): AWS region to upload to.

        Returns:
            s3_filename (str): path including bucket, bucket_folder, and filename for the
                uploaded data
        """
        s3_filename = None
        s3_client = boto3.client("s3", region_name=region)
        obj_name = os.path.basename(full_filename)
        if bucket_path:
            obj_name = f"{bucket_path}/{obj_name}"
        try:
            s3_client.upload_file(full_filename, bucket, obj_name)
        except SSLError:
            self._logme(
                f"Failed with SSLError to upload {obj_name} to {bucket}",
                log_level=ERROR)
        except ClientError as e:
            self._logme(
                f"Failed to upload {obj_name} to {bucket}, ({e})",
                log_level=ERROR)
        else:
            s3_filename = f"s3://{bucket}/{obj_name}"
            self._logme(f"Uploaded {s3_filename} to S3")
        return s3_filename

    # .............................................................................
    def write_to_s3(self, bucket, bucket_path, filename, region):
        """Write a pd DataFrame to CSV or parquet on S3.

        Args:
            bucket (str): Bucket identifier on S3.
            bucket_path (str): Folder path to the S3 output data.
            filename (str): Filename of output data to write to S3.
        """
        s3_filename = None
        tmp_fname = f"/tmp/{filename}"
        if os.path.exists(tmp_fname):
            os.remove(tmp_fname)
        try:
            scipy.sparse.save_npz(tmp_fname, self._coo_array, compressed=True)
        except Exception as e:
            self._logme(f"Failed to write {tmp_fname}: {e}", log_level=ERROR)
        else:
            s3_filename = self._upload_to_s3(tmp_fname, bucket, bucket_path, region)
        return s3_filename


# .............................................................................
# .............................................................................
class AggregateMatrix:
    """Class for managing computations for counts of aggregator0 x aggregator1."""

    # ...........................
    def __init__(self, aggregate_df, logger=None):
        """Constructor for species by dataset comparisons.

        Args:
            aggregate_df (pandas.DataFrame): A 2d sparse matrix with count values for
                one aggregator0 (i.e. species) rows (axis 0) by another aggregator1
                (i.e. dataset) columns (axis 1) to use for computations.
            logger (object): An optional local logger to use for logging output
                with consistent options
        """
        self._df = aggregate_df
        self._logger = logger
        self._report = {}

    # ...............................................
    def _logme(self, msg, refname="", log_level=None):
        logit(self._logger, msg, refname=refname, log_level=log_level)

    # ...............................................
    @property
    def num_y_values(self):
        """Get the number of rows.

        Returns:
            int: The count of rows where the value > 0 in at least one column.

        Note:
            Also used as gamma diversity (species richness over entire landscape)
        Note: because these data should be constructed only from rows and columns
            containing data, this should ALWAYS equal the number of rows
        """
        count = -1
        if self._df is not None:
            # If this axis is very large, it may throw a MemoryError
            try:
                count = self._df.any(axis=0).sum()
            except Exception as e:
                self._logme(
                    f"Exception {e} on sum; returning row count.",
                    log_level=WARNING)
                count = self._df.shape[0]

            else:
                if count != self._df.shape[0]:
                    self._logme(
                        f"Count of non-null rows {count} != row count "
                        f"{self._df.shape[0]}.", log_level=WARNING)
        return count

    # ...............................................
    @property
    def num_x_values(self):
        """Get the number of columns.

        Returns:
            int: The count of columns where the value > 0 in at least one row

        Note: because these data should be constructed only from rows and columns
            containing data, this should ALWAYS equal the number of columns
        """
        count = -1
        if self._df is not None:
            # If this axis is very large, it may throw a MemoryError
            try:
                count = self._df.any(axis=1).sum()
            except Exception as e:
                self._logme(
                    f"Exception {e} on sum; returning column count.",
                    log_level=WARNING)
                count = self._df.shape[1]
            else:
                if count != self._df.shape[1]:
                    self._logme(
                        f"Count of non-null columns {count} != column count "
                        f"{self._df.shape[1]}.", log_level=WARNING)
        return count

    # ...............................................
    @property
    def sum_column(self, col):
        """Get the total of values in a single column.

        Args:
            col: label on the column to total.

        Returns:
            int: The total of all values in one column
        """
        # Access column by label with dataframe[column]
        total = self._df[col].sum()
        return total

    # ...............................................
    def sum_row(self, row):
        """Get the total of values in a single row.

        Args:
            row: label on the row to total.

        Returns:
            int: The total of all values in one row
        """
        # Access row by label with dataframe.loc[row]
        total = self._df.loc[row].sum(axis=1)
        return total

    # ...............................................
    def count_values_for_column(self, col_label, value):
        """Count the number of occurrences of `value` in column `col_name`.

        Args:
            col_label: label for column of interest
            value: value to count in the column

        Returns:
            count: the number of times value occurs in column col_label.
        """
        count = self._df[col_label].value_counts()[value]
        return count

    # ...............................................
    def count_values_for_row(self, row_label, value):
        """Count the number of occurrences of `value` in row `row_label`.

        Args:
            row_label: label for row of interest
            value: value to count in the row

        Returns:
            count: the number of times value occurs in column col_name.
        """
        count = self._df.loc[row_label].value_counts()[value]
        return count

    # ...............................................
    def write_to_s3(
            self, bucket, bucket_path, filename, format="Parquet", **args):
        """Write a pd DataFrame to CSV or parquet on S3.

        Args:
            bucket (str): Bucket identifier on S3.
            bucket_path (str): Folder path to the S3 output data.
            filename (str): Filename of output data to write to S3.
            format (str): output format, "csv" and "parquet" supported.
            args: Additional arguments to be sent to the pd.read_parquet function.

        Raises:
            Exception: on format other than "csv" or "parquet"
        """
        target = f"s3://{bucket}/{bucket_path}/{filename}"
        if format.lower() not in ("csv", "parquet"):
            raise Exception(f"Format {format} not supported.")
        if format.lower() == "csv":
            try:
                self._df.to_csv(target)
            except Exception as e:
                logit(
                    self._logger, f"Failed to write {target} as csv: {e}",
                    log_level=ERROR)
        else:
            try:
                self._df.to_parquet(target)
            except Exception as e:
                logit(
                    logger, f"Failed to write {target} as parquet: {e}",
                    log_level=ERROR)

    # .............................................................................
    def reframe_to_binary(self, min_val):
        """Create a binary dataframe of values from a numeric dataframe.

        Args:
            min_val(numeric): Minimum value to be considered present/1 in the output matrix.

        Returns:
            bin_df (pandas.DataFrame): DF with values = 1 (presence) or 0 (absence).
        """
        try:
            # pd 2.1.0, upgrade then replace "applymap" with "map"
            bin_df = self._df.applymap(lambda x: 1 if x >= min_val else 0)
        except AttributeError:
            bin_df = self._df.applymap(lambda x: 1 if x >= min_val else 0)
        return bin_df


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    """Main script creates a SPECIES_DATASET_MATRIX from DATASET_SPECIES_LISTS."""
    # Create a logger
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    todaystr = get_today_str()
    log_name = f"{script_name}_{todaystr}"
    # Create logger with default INFO messages
    logger = Logger(
        log_name, log_path=LOCAL_OUTDIR, log_console=True, log_level=INFO)

    data_datestr = get_current_datadate_str()
    table = Summaries.get_table(SUMMARY_TABLE_TYPES.DATASET_SPECIES_LISTS, data_datestr)
    x_fld = table["key_fld"]
    val_fld = table["value_fld"]
    # Dict of new fields constructed from existing fields, just 1 for species key/name
    fld_mods = table["combine_fields"]
    y_fld = list(fld_mods.keys())[0]
    (fld1, fld2) = fld_mods[y_fld]

    # Read directly into DataFrame
    stacked_df = read_s3_parquet_to_pandas(
        PROJ_BUCKET, SUMMARY_FOLDER, table["fname"], logger, s3_client=None
    )

    # ......................
    def _combine_columns(row):
        return str(row[fld1]) + ' ' + str(row[fld2])

    # ......................
    stacked_df[y_fld] = stacked_df.apply(_combine_columns, axis=1)

    sp_mtx = SparseMatrix.init_from_stacked_data(stacked_df, x_fld, y_fld, val_fld)

    out_filename = Summaries.get_filename(
        SUMMARY_TABLE_TYPES.SPECIES_DATASET_MATRIX, data_datestr)
    # sp_mtx.write_to_s3(PROJ_BUCKET, SUMMARY_FOLDER, out_filename)

    test_stacked_to_aggregate(
        stacked_df, x_fld, y_fld, val_fld, sp_mtx)

    # # Upload logfile to S3
    # s3_log_filename = upload_to_s3(logger.filename, PROJ_BUCKET, LOG_PATH, logger)

"""
from sppy.aws.aggregate_matrix import *
ds = '8092785 Sourniaea diacantha'
sp = '2480637 Accipiter nisus'

# Create a logger
script_name = "testing_aggregate_matrix"
todaystr = get_today_str()
log_name = f"{script_name}_{todaystr}"
# Create logger with default INFO messages
logger = Logger(
    log_name, log_path=LOCAL_OUTDIR, log_console=True, log_level=INFO)

data_datestr = get_current_datadate_str()
table = Summaries.get_table(DATASET_SPECIES_LISTS, data_datestr)
x_fld = table["key_fld"]
val_fld = table["value_fld"]
# Dict of new fields constructed from existing fields, just 1 for species key/name
fld_mods = table["combine_fields"]
y_fld = list(fld_mods.keys())[0]
(fld1, fld2) = fld_mods[y_fld]

# Read directly into DataFrame
stacked_df = read_s3_parquet_to_pandas(
    PROJ_BUCKET, SUMMARY_FOLDER, table["fname"], logger, s3_client=None
)

# ......................
def combine_columns(row):
    return str(row[fld1]) + ' ' + str(row[fld2])

# ......................
stacked_df[y_fld] = stacked_df.apply(combine_columns, axis=1)

sp_mtx = SparseMatrix.init_from_stacked_data(stacked_df, x_fld, y_fld, val_fld)

out_table = Summaries.get_table(SPECIES_DATASET_MATRIX, data_datestr)

sp_mtx.write_to_s3(PROJ_BUCKET, SUMMARY_FOLDER, out_table["fname"])


"""
