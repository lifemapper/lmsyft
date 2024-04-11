"""Tools to compute dataset x species statistics from S3 data."""
import boto3
from botocore.exceptions import ClientError
import io
import logging
import os
import numpy as np
import pandas as pd

from sppy.aws.aws_constants import (
    LOCAL_OUTDIR, LOG_PATH, PROJ_BUCKET, REGION, SUMMARY_FOLDER, Summaries)
from sppy.aws.aws_tools import get_current_datadate_str, get_today_str
from sppy.tools.s2n.utils import get_traceback
from sppy.tools.util.logtools import Logger, logit


# .............................................................................
def download_from_s3(bucket, bucket_path, filename, logger=None, overwrite=True):
    """Download a file from S3 to a local file.

    Args:
        bucket (str): Bucket identifier on S3.
        bucket_path (str): Folder path to the S3 parquet data.
        filename (str): Filename of parquet data to read from S3.
        logger (object): logger for saving relevant processing messages
        overwrite (boolean):  flag indicating whether to overwrite an existing file.

    Returns:
        local_filename (str): full path to local filename containing downloaded data.
    """
    local_path = os.getcwd()
    local_filename = os.path.join(local_path, filename)
    if os.path.exists(local_filename):
        if overwrite is True:
            os.remove(local_filename)
        else:
            logit(logger, f"{local_filename} already exists")
    else:
        s3_client = boto3.client("s3")
        try:
            s3_client.download_file(bucket, f"{bucket_path}/{filename}", local_filename)
        except ClientError as e:
            logit(
                logger,
                f"Failed to download {filename} from {bucket}/{bucket_path}, ({e})",
                log_level=logging.ERROR)
        else:
            logit(logger,f"Downloaded {filename} from S3 to {local_filename}")
    return local_filename


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
            log_level=logging.ERROR)
    else:
        logit(logger, f"Read {bucket}/{s3_key} from S3")
    dataframe = pd.read_parquet(io.BytesIO(obj["Body"].read()), **args)
    return dataframe


# .............................................................................
def write_pandas_to_s3(
        dataframe, bucket, bucket_path, filename, logger=None, format="csv", **args):
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
    if format.lower() not in ("csv", "parquet"):
        raise Exception(f"Format {format} not supported.")
    if format.lower() == "csv":
        try:
            dataframe.to_csv(target)
        except Exception as e:
            logit(
                logger, f"Failed to write {target} as csv: {e}", 
                log_level=logging.ERROR)
    else:
        try:
            dataframe.to_parquet(target)
        except Exception as e:
            logit(
                logger, f"Failed to write {target} as parquet: {e}", 
                log_level=logging.ERROR)


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
            log_level=logging.ERROR)
        return None

    dfs = [
        read_s3_parquet_to_pandas(
            bucket, bucket_path, key, logger, s3_client=s3_client, region=region,
            **args) for key in s3_keys
    ]
    return pd.concat(dfs, ignore_index=True)


# .............................................................................
def read_species_to_dict(orig_df, aggregate_fld):
    """Create a dictionary of species keys, with values containing counts for counties.

    Args:
        orig_df (pandas.DataFrame): DataFrame of records containing columns:
            census_state, census_county. taxonkey, species, riis_assessment, occ_count

    Returns:
        dictionary of {county: [species: count, species: count, ...]}
    """
    # for each county, create a list of species/count
    county_species_counts = {}
    for _, row in orig_df.iterrows():
        sp = row["species"]
        cty = row[aggregate_fld]
        total = row["occ_count"]
        try:
            county_species_counts[cty].append((sp, total))
        except KeyError:
            county_species_counts[cty] = [(sp, total)]
    return county_species_counts


# .............................................................................
def reframe_to_binary(species_dataset_df, min_val):
    """Create a dataframe of species columns by county rows from county species lists.

    Args:
        heat_df (pandas.DataFrame): DataFrame of species (rows, y axis=0) by
            counties (columns, x_axis=1, sites), with values = number of occurrences.
        min_val(numeric): Minimum value to be considered present/1 in the output matrix.

    Returns:
        pam_df (pandas.DataFrame): DF of species (rows, y axis=0) by datasets
            (columns, x axis=1, sites), with values = 1 (presence) or 0 (absence).
    """
    try:
        # pd 2.1.0, upgrade then replace "applymap" with "map"
        pam_df = species_dataset_df.applymap(lambda x: 1 if x >= min_val else 0)
    except AttributeError:
        pam_df = species_dataset_df.applymap(lambda x: 1 if x >= min_val else 0)
    return pam_df


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
            log_level=logging.ERROR)
    else:
        s3_filename = f"s3://{bucket}/{obj_name}"
        logit(logger, f"Uploaded {s3_filename} to S3")
    return s3_filename


# .............................................................................
class AggregateMatrix:
    """Class for managing computations for occurrence counts of species x datasets."""

    # ...........................
    def __init__(self, species_dataset_df, logger=None):
        """Constructor for species by dataset comparisons.

        Args:
            species_dataset_df (pandas.DataFrame): A 2d matrix with species rows
                (axis 0) by dataset columns (axis 1) to use for computations.
            logger (object): An optional local logger to use for logging output
                with consistent options
        """
        self._df = species_dataset_df
        self._logger = logger
        self._report = {}

    # ...............................................
    @classmethod
    def reframe_from_columns(cls, orig_df, x_fld, y_fld, val_fld, logger=None):
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
            logger (object): logger for saving relevant processing messages

        Returns:
            sd_mtx (AggregateMatrix): matrix containing a dataframe of y values (rows, y axis=0) by
                x values (columnns, x axis=1), with values from another column.

        Note:
            The input dataframe must contain only one input record for any x and y value
                combination, and each record must contain another value for the dataframe
                contents.  The function was written for a table of records with
                datasetkey (for the column labels/x), species (for the row labels/y),
                and occurrence count.
        """
        # Get rid of extra columns
        fields = list(orig_df.columns)
        extra_cols = list(set(fields).difference([x_fld, y_fld, val_fld]))
        orig_df.drop(columns=extra_cols, inplace=True)

        # Get column and row indexes, remove None
        unique_x_vals = orig_df[x_fld].dropna().unique()
        unique_y_vals = orig_df[y_fld].dropna().unique()

        sparse_dtype = pd.SparseDtype("int32", 0)
        # Must not use index or columns, to ensure DF is sparse
        new_df = pd.DataFrame(0, index=[], columns=[], dtype=sparse_dtype)

        # NOTE: Works but too slow. For each column
        for x in unique_x_vals:
            # Subset a dataframe containing only rows with x_fld = x
            xdf = orig_df.loc[orig_df[x_fld] == x]
            # Create a 1 column temp DF for the x column, with y/row indices
            tdf = pd.DataFrame(
                pd.arrays.SparseArray(
                    xdf[val_fld].values), index=xdf[y_fld], columns=[x], dtype=sparse_dtype
            )
            # Add an empty column to the dataframe, then overwrite null values with the temp DF
            new_df = new_df.assign(**{x: None})
            new_df = new_df.combine_first(tdf)

        sd_mtx = AggregateMatrix(new_df, logger=logger)
        return sd_mtx

    # ...............................................
    @property
    def num_y_values(self):
        """Get the number of rows.

        Returns:
            int: The number of species that are present in at least one dataset.

        Note:
            Also used as gamma diversity (species richness over entire landscape)
        """
        count = 0
        if self._df is not None:
            count = int(self._df.any(axis=0).sum())
        return count

    # ...............................................
    @property
    def num_x_values(self):
        """Get the number of columns.

        Returns:
            int: The number of datasets in the matrix.
        """
        count = 0
        if self._df is not None:
            count = int(self._df.any(axis=1).sum())
        return count



    # ...............................................
    def write_to_csv(self, filename):
        """Write dataframe as CSV format, with comma separator, utf-8 format.

        Args:
            filename: full path to output file.
        """
        try:
            self._df.to_csv(filename)
        except Exception as e:
            self._logger.log(logging.ERROR, f"Failed to write {filename}: {e}")

    # ...............................................
    def write_to_s3(
            self, bucket, bucket_path, filename, logger, format="Parquet", **args):
        """Write a pd DataFrame to CSV or parquet on S3.

        Args:
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
        if format.lower() not in ("csv", "parquet"):
            raise Exception(f"Format {format} not supported.")
        if format.lower() == "csv":
            try:
                self._df.to_csv(target)
            except Exception as e:
                logit(
                    logger, f"Failed to write {target} as csv: {e}",
                    log_level=logging.ERROR)
        else:
            try:
                self._df.to_parquet(target)
            except Exception as e:
                logit(
                    logger,f"Failed to write {target} as parquet: {e}",
                    log_level=logging.ERROR)


# .............................................................................
def reframe_to_species_x_dataset(df, species_fld, dataset_fld, logger=None):
    """Create a dataframe of dataset columns by species rows from dataset species lists.

    Args:
        orig_df (pandas.DataFrame): DataFrame of records containing columns:
            datasetkey, taxonkey, species, occ_count
        logger (object): logger for saving relevant processing messages

    Returns:
        species_x_dataset_df (pandas.DataFrame): DF of species (rows, y axis=0) by
            datasets (columnns, x axis=1), with values = number of occurrences.
    """
    # Create dataframe of zeros with rows=species and columns=datasets
    datasetkey = orig_df.datasetkey.unique()
    species = orig_df.species.unique()
    species_dataset_df = pd.DataFrame(0, index=species, columns=datasetkey)
    # Fill dataframe
    dataset_species_counts = read_species_to_dict(orig_df, species_fld, dataset_fld)
    for dataset, sp_counts in dataset_species_counts.items():
        for (sp, count) in sp_counts:
            # axis = 0 = row = species
            species_dataset_df.loc[sp][dataset] = count
    return species_dataset_df


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
        log_name, log_path=LOCAL_OUTDIR, log_console=True, log_level=logging.INFO)

    data_datestr = get_current_datadate_str()
    table = Summaries.get_table("dataset_species_lists", data_datestr)
    fields = table["fields"]
    x_fld = table["key_fld"]
    y_fld = table["species_fld"]
    val_fld = table["value_fld"]

    # Read directly into DataFrame
    orig_df = read_s3_parquet_to_pandas(
        PROJ_BUCKET, SUMMARY_FOLDER, table["fname"], logger, s3_client=None
    )

    species_dataset_matrix = AggregateMatrix.reframe_from_columns(
        orig_df, x_fld, y_fld, val_fld, logger=logger)

    # Upload logfile to S3
    s3_log_filename = upload_to_s3(logger.filename, PROJ_BUCKET, LOG_PATH, logger)

"""
from sppy.aws.aggregate_matrix import * 

todaystr = get_today_str()
log_name = f"testing_{todaystr}"
logger = Logger(
        log_name, log_path=LOCAL_OUTDIR, log_console=True, log_level=logging.INFO)

data_datestr = get_current_datadate_str()
table = Summaries.get_table("dataset_species_lists", data_datestr)

x_fld = table["key_fld"]
y_fld = table["species_fld"]
val_fld = table["value_fld"]

# Read directly into DataFrame
orig_df = read_s3_parquet_to_pandas(
    PROJ_BUCKET, SUMMARY_FOLDER, table["fname"], logger=logger)

fields = list(orig_df.columns)
extra_cols = list(set(fields).difference([x_fld, y_fld, val_fld]))
orig_df.drop(columns=extra_cols, inplace=True)

# Get column and row indexes, remove None
unique_x_vals = orig_df[x_fld].dropna().unique()
unique_y_vals = orig_df[y_fld].dropna().unique()

# Create empty sparse dataframe
sparse_dtype = pd.SparseDtype("int32", 0)
new_df = pd.DataFrame(0, index=[], columns=[], dtype=sparse_dtype)

# For each column (x value)
x = unique_x_vals[0]
x1 = unique_x_vals[1]
x2 = unique_x_vals[2]

# For each column (x value)
start = DT.datetime.now()
for x in unique_x_vals:
    # Subset a dataframe containing only rows with x_fld = x
    xdf = orig_df.loc[orig_df[x_fld] == x]
    # Create a 1 column temp DF for the x column, with y/row indices
    tdf = pd.DataFrame(
        pd.arrays.SparseArray(
            xdf[val_fld].values), index=xdf[y_fld], columns=[x]
    )
    # Add an empty column to the dataframe, then overwrite null values with the temp DF
    new_df = new_df.assign(**{x: None})
    new_df = new_df.combine_first(tdf)

end = DT.datetime.now()


# Upload logfile to S3
s3_log_filename = upload_to_s3(logger.filename, PROJ_BUCKET, LOG_PATH, logger)

"""
