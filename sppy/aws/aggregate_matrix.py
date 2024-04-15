"""Tools to compute dataset x species statistics from S3 data."""
import boto3
from botocore.exceptions import ClientError
import io
from logging import ERROR, INFO, WARNING
import os
import pandas as pd
from pandas.api.types import CategoricalDtype
import random
from scipy.sparse import csr_matrix

from sppy.aws.aws_constants import (
    LOCAL_OUTDIR, LOG_PATH, PROJ_BUCKET, REGION, SUMMARY_FOLDER, Summaries)
from sppy.aws.aws_tools import get_current_datadate_str, get_today_str
from sppy.tools.util.logtools import Logger, logit


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
    if s3_client is None:
        s3_client = boto3.client("s3", region_name=region)
    s3_key = f"{bucket_path}/{filename}"
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=s3_key)
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


# .............................................................................
def write_pandas_to_s3(
        dataframe, bucket, bucket_path, filename, logger=None, format="Parquet", **args):
    """Write a pd DataFrame to CSV or parquet on S3.

    Args:
        dataframe (pandas.DataFrame): dataframe to write to S3.
        bucket (str): Bucket identifier on S3.
        bucket_path (str): Folder path to the S3 output data.
        filename (str): Filename of output data to write to S3.
        logger (object): logger for saving relevant processing messages
        format (str): output format, "csv" and "parquet" supported.
        args: Additional arguments to be sent to the pd.read_parquet function.

    Raises:
        Exception: on format other than "csv" or "parquet"
    """
    target = f"s3://{bucket}/{bucket_path}/{filename}"
    format = format.lower()
    if format not in ("csv", "parquet"):
        raise Exception(f"Format {format} not supported.")
    if format == "csv":
        try:
            dataframe.to_csv(target)
        except Exception as e:
            logit(
                logger, f"Failed to write {target} as csv: {e}", log_level=ERROR)
    else:
        try:
            dataframe.to_parquet(target)
        except Exception as e:
            logit(
                logger, f"Failed to write {target} as parquet: {e}",
                log_level=ERROR)


# .............................................................................
def upload_to_s3(full_filename, bucket, bucket_path, logger=None):
    """Upload a file to S3.

    Args:
        full_filename (str): Full filename to the file to upload.
        bucket (str): Bucket identifier on S3.
        bucket_path (str): Parent folder path to the S3 parquet data.
        logger (object): logger for saving relevant processing messages

    Returns:
        s3_filename (str): path including bucket, bucket_folder, and filename for the
            uploaded data
    """
    s3_filename = None
    s3_client = boto3.client("s3")
    obj_name = os.path.basename(full_filename)
    if bucket_path:
        obj_name = f"{bucket_path}/{obj_name}"
    try:
        s3_client.upload_file(full_filename, bucket, obj_name)
    except ClientError as e:
        logit(
            logger, f"Failed to upload {obj_name} to {bucket}, ({e})",
            log_level=ERROR)
    else:
        s3_filename = f"s3://{bucket}/{obj_name}"
        logit(logger, f"Uploaded {s3_filename} to S3")
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
        count = sdf[col_label].value_counts()[value]
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
        count = sdf.loc[row_label].value_counts()[value]
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


# ...............................................
def add_combo_column_to_df(orig_df, new_fld, fld1, fld2):
    """Add a new field to the dataframe, with fld1 concatenated to fld2.

    Args:
        orig_df: dataframe
        new_fld: label for new column to add
        fld1: name of the column to use the value in first part of the new column
        fld2: name of the column to use the value in second part of the new column
    """
    def combine_columns(row):
        return str(row[fld1]) + ' ' + str(row[fld2])
    # .......
    orig_df[new_fld] = orig_df.apply(combine_columns, axis=1)


# ...............................................
def create_sparse_df_from_stacked_data(orig_df, x_fld, y_fld, val_fld):
    """Create a dataframe of rows by columns containing values from a table.

    Args:
        orig_df (pandas.DataFrame): DataFrame of records containing columns to be used
            as the new rows, new columns, and values.
        x_fld: column in the input dataframe containing values to be used as
            columns (axis 1)
        y_fld: column in the input dataframe containing values to be used as rows
            (axis 0)
        val_fld: : column in the input dataframe containing values to be used as
            values for the intersection of x and y fields

    Returns:
        sdf (pandas.SparseDataFrame): dataframe of y values (rows, y axis=0) by
            x values (columnns, x axis=1), with values from another column.

    Note:
        The input dataframe must contain only one input record for any x and y value
            combination, and each record must contain another value for the dataframe
            contents.  The function was written for a table of records with
            datasetkey (for the column labels/x), species (for the row labels/y),
            and occurrence count.
    """
    # Get unique values to use as categories for scipy column and row indexes, remove None
    unique_x_vals = list(orig_df[x_fld].dropna().unique())
    unique_y_vals = list(orig_df[y_fld].dropna().unique())

    # Categories allow using codes as the integer index for scipy matrix
    y_categ = CategoricalDtype(unique_y_vals, ordered=True)
    x_categ = CategoricalDtype(unique_x_vals, ordered=True)

    # Create a list of category codes matching original stacked data records to replace
    #   column names from stacked data dataframe with integer codes for row and column
    #   indexes in the new scipy matrix
    col_idx = orig_df[x_fld].astype(x_categ).cat.codes
    row_idx = orig_df[y_fld].astype(y_categ).cat.codes

    # This compresses original rows with the same y_fld values and multiple
    #   x_fld values into a row (y) with different values (counts) for the columns (x)
    sparse_matrix = csr_matrix(
        (orig_df[val_fld], (row_idx, col_idx)),
        shape=(y_categ.categories.size, x_categ.categories.size))

    sdf = pd.DataFrame.sparse.from_spmatrix(
        sparse_matrix, index=y_categ.categories, columns=x_categ.categories)

    return sdf


# ...............................................
def _filter_stacked_data_on_column_val(stacked_df, filter_label, filter_value):
    """Create a dataframe containing only rows where a column contains a value.

    Args:
        stacked_df: dataframe containing stacked data records
        filter_label: column name for filtering.
        filter_value: column value for filtering.

    Returns:
        tmp_df: dataframe containing only rows with a value of filter_value in column
            filter_label.
    """
    tmp_df = stacked_df.loc[stacked_df[filter_label] == filter_value]
    return tmp_df


# ...............................................
def _sum_stacked_data_vals_for_column(stacked_df, filter_label, filter_value, val_label):
    tmp_df = _filter_stacked_data_on_column_val(stacked_df, filter_label, filter_value)
    count = tmp_df[val_label].sum()
    return count


# ...............................................
def _get_random_values_from_stacked_data(stacked_df, col_label, count):
    idxs = random.sample(range(1, stacked_df.shape[0]), count)
    x_vals = [stacked_df[col_label][i] for i in idxs]
    return x_vals


# ...............................................
def test_stacked_to_aggregate(
        stacked_df, x_col_label, y_col_label, val_col_label, aggregate_df):
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
        aggregate_df: dataframe containing a matrix with 3 columns from the stacked_df
            arranged as rows and columns with values

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
        agg_sum = aggregate_df[x_col].sum()
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
        agg_sum = aggregate_df.loc[y_col].sum()
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


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    # Create a logger
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    todaystr = get_today_str()
    log_name = f"{script_name}_{todaystr}"
    # Create logger with default INFO messages
    logger = Logger(
        log_name, log_path=LOCAL_OUTDIR, log_console=True, log_level=INFO)

    data_datestr = get_current_datadate_str()
    table = Summaries.get_table("dataset_species_lists", data_datestr)
    x_fld = table["key_fld"]
    val_fld = table["value_fld"]
    # Dict of new fields constructed from existing fields, just 1 for species key/name
    fld_mods = table["combine_fields"]
    y_fld = list(fld_mods.keys())[0]
    (fld1, fld2) = fld_mods[y_fld]

    # Read directly into DataFrame
    orig_df = read_s3_parquet_to_pandas(
        PROJ_BUCKET, SUMMARY_FOLDER, table["fname"], logger, s3_client=None
    )

    # ......................
    def _combine_columns(row):
        return str(row[fld1]) + ' ' + str(row[fld2])

    # ......................
    orig_df[y_fld] = orig_df.apply(_combine_columns, axis=1)

    sdf = create_sparse_df_from_stacked_data(orig_df, x_fld, y_fld, val_fld)

    out_table = Summaries.get_table("species_dataset_matrix", data_datestr)
    write_pandas_to_s3(
        sdf, PROJ_BUCKET, SUMMARY_FOLDER, out_table["fname"], logger=logger,
        format="Parquet")

    # Upload logfile to S3
    s3_log_filename = upload_to_s3(logger.filename, PROJ_BUCKET, LOG_PATH, logger)

"""
from sppy.aws.aggregate_matrix import *

# Create a logger
script_name = "testing_aggregate_matrix"
todaystr = get_today_str()
log_name = f"{script_name}_{todaystr}"
# Create logger with default INFO messages
logger = Logger(
    log_name, log_path=LOCAL_OUTDIR, log_console=True, log_level=INFO)

data_datestr = get_current_datadate_str()
table = Summaries.get_table("dataset_species_lists", data_datestr)
x_fld = table["key_fld"]
val_fld = table["value_fld"]
# Dict of new fields constructed from existing fields, just 1 for species key/name
fld_mods = table["combine_fields"]
y_fld = list(fld_mods.keys())[0]
(fld1, fld2) = fld_mods[y_fld]

# Read directly into DataFrame
orig_df = read_s3_parquet_to_pandas(
    PROJ_BUCKET, SUMMARY_FOLDER, table["fname"], logger, s3_client=None
)
# ......................
def combine_columns(row):
    return str(row[fld1]) + ' ' + str(row[fld2])
# ......................
orig_df[y_fld] = orig_df.apply(combine_columns, axis=1)

sdf = create_sparse_df_from_stacked_data(orig_df, x_fld, y_fld, val_fld)

out_table = Summaries.get_table("species_dataset_matrix", data_datestr)
write_pandas_to_s3(
    sdf, PROJ_BUCKET, SUMMARY_FOLDER, out_table["fname"], logger=logger,
    format="Parquet")

# Upload logfile to S3
s3_log_filename = upload_to_s3(logger.filename, PROJ_BUCKET, LOG_PATH, logger)

"""
