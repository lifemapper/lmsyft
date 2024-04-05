"""Tools to compute dataset x species statistics from S3 data."""
import boto3
from botocore.exceptions import ClientError
import datetime as DT
import io
import logging
from logging.handlers import RotatingFileHandler
import os
import pandas as pd
import sys

from sppy.aws.aws_constants import ENCODING, LOG_PATH, PROJ_BUCKET, REGION, SUMMARY_FOLDER
from sppy.aws.aws_tools import get_current_datadate_str, get_today_str
from sppy.tools.s2n.utils import get_traceback


# Also in bison_ec2_constants, but provided here to avoid populating EC2 template with
# multiple files for userdata.
LOCAL_OUTDIR = "/tmp"

n = DT.datetime.now()
# underscores for Redshift data
datestr = get_current_datadate_str()
# date for logfile
todaystr = get_today_str()

species_county_list_fname = f"county_lists_{datestr}_000.parquet"
# Log processing progress
LOGINTERVAL = 1000000
LOG_FORMAT = " ".join(["%(asctime)s", "%(levelname)-8s", "%(message)s"])
LOG_DATE_FORMAT = "%d %b %Y %H:%M"
LOGFILE_MAX_BYTES = 52000000
LOGFILE_BACKUP_COUNT = 5

LOCAL_OUTDIR = "/tmp"
diversity_stats_dataname = os.path.join(LOCAL_OUTDIR, f"diversity_stats_{datestr}.csv")
species_stats_dataname = os.path.join(LOCAL_OUTDIR, f"species_stats_{datestr}.csv")
site_stats_dataname = os.path.join(LOCAL_OUTDIR, f"site_stats_{datestr}.csv")


# .............................................................................
def get_logger(log_name, log_dir=None, log_level=logging.INFO):
    """Get a logger for saving messages to disk.

    Args:
        log_name: name of the logger and logfile
        log_dir: path for the output logfile.
        log_level: Minimum level for which to log messages

    Returns:
        logger (logging.Logger): logger instance.
        filename (str): full path for output logfile.
    """
    filename = f"{log_name}.log"
    if log_dir is not None:
        filename = os.path.join(log_dir, f"{filename}")
        os.makedirs(log_dir, exist_ok=True)
    # create file handler
    handlers = []
    # for debugging in place
    handlers.append(logging.StreamHandler(stream=sys.stdout))
    # for saving onto S3
    handler = RotatingFileHandler(
        filename, mode="w", maxBytes=LOGFILE_MAX_BYTES, backupCount=10,
        encoding="utf-8"
    )
    formatter = logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)
    handler.setLevel(log_level)
    handler.setFormatter(formatter)
    # Get logger
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.DEBUG)
    # Add handler to logger
    logger.addHandler(handler)
    logger.propagate = False
    return logger, filename


# .............................................................................
def download_from_s3(bucket, bucket_path, filename, logger, overwrite=True):
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
            logger.log(logging.INFO, f"{local_filename} already exists")
    else:
        s3_client = boto3.client("s3")
        try:
            s3_client.download_file(bucket, f"{bucket_path}/{filename}", local_filename)
        except ClientError as e:
            logger.log(
                logging.ERROR,
                f"Failed to download {filename} from {bucket}/{bucket_path}, ({e})")
        else:
            logger.log(
                logging.INFO, f"Downloaded {filename} from S3 to {local_filename}")
    return local_filename


# .............................................................................
def read_s3_parquet_to_pandas(
        bucket, bucket_path, filename, logger, s3_client=None, region=REGION, **args):
    """Read a parquet file from a folder on S3 into a pandas DataFrame.

    Args:
        bucket (str): Bucket identifier on S3.
        bucket_path (str): Folder path to the S3 parquet data.
        filename (str): Filename of parquet data to read from S3.
        logger (object): logger for saving relevant processing messages
        s3_client (object): object for interacting with Amazon S3.
        region (str): AWS region to query.
        args: Additional arguments to be sent to the pandas.read_parquet function.

    Returns:
        pandas.DataFrame containing the tabular data.
    """
    if s3_client is None:
        s3_client = boto3.client("s3", region_name=region)
    s3_key = f"{bucket_path}/{filename}"
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=s3_key)
    except ClientError as e:
        logger.log(logging.ERROR, f"Failed to get {bucket}/{s3_key} from S3, ({e})")
    else:
        logger.log(logging.INFO, f"Read {bucket}/{s3_key} from S3")
    dataframe = pandas.read_parquet(io.BytesIO(obj["Body"].read()), **args)
    return dataframe


# .............................................................................
def write_pandas_to_s3(
        dataframe, bucket, bucket_path, filename, logger, format="csv", **args):
    """Write a pandas DataFrame to CSV or parquet on S3.

    Args:
        dataframe (pandas.DataFrame): dataframe to write to S3.
        bucket (str): Bucket identifier on S3.
        bucket_path (str): Folder path to the S3 output data.
        filename (str): Filename of output data to write to S3.
        logger (object): logger for saving relevant processing messages
        format (str): output format, "csv" and "parquet" supported.
        args: Additional arguments to be sent to the pandas.read_parquet function.

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
            logger.log(logging.ERROR, f"Failed to write {target} as csv: {e}")
    else:
        try:
            dataframe.to_parquet(target)
        except Exception as e:
            logger.log(logging.ERROR, f"Failed to write {target} as parquet: {e}")


# .............................................................................
def read_s3_multiple_parquets_to_pandas(
        bucket, bucket_path, logger, s3=None, s3_client=None, verbose=False,
        region=REGION, **args):
    """Read multiple parquets from a folder on S3 into a pandas DataFrame.

    Args:
        bucket (str): Bucket identifier on S3.
        bucket_path (str): Parent folder path to the S3 parquet data.
        logger (object): logger for saving relevant processing messages
        s3 (object): Connection to the S3 resource
        s3_client (object): object for interacting with Amazon S3.
        verbose (boolean): flag indicating whether to log verbose messages
        region: AWS region to query.
        args: Additional arguments to be sent to the pandas.read_parquet function.

    Returns:
        pandas.DataFrame containing the tabular data.
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
        logger.log(logging.ERROR, f"No parquet found in {bucket} {bucket_path}")
    elif verbose:
        logger.log(logging.INFO, "Load parquets:")
        for p in s3_keys:
            logger.log(logging.INFO, f"   {p}")
    dfs = [
        read_s3_parquet_to_pandas(
            bucket, bucket_path, key, logger, s3_client=s3_client, region=region,
            **args) for key in s3_keys
    ]
    return pandas.concat(dfs, ignore_index=True)


# .............................................................................
def read_species_to_dict(orig_df):
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
        cty = row["state_county"]
        total = row["occ_count"]
        try:
            county_species_counts[cty].append((sp, total))
        except KeyError:
            county_species_counts[cty] = [(sp, total)]
    return county_species_counts


# .............................................................................
def reframe_to_heatmatrix(orig_df, logger):
    """Create a dataframe of species columns by county rows from county species lists.

    Args:
        orig_df (pandas.DataFrame): DataFrame of records containing columns:
            census_state, census_county. taxonkey, species, riis_assessment, occ_count
        logger (object): logger for saving relevant processing messages

    Returns:
        heat_df (Pandas.DataFrame): DF of species (columnns, x axis=1) by counties
            (rows, y axis=0, sites), with values = number of occurrences.
    """
    # Create ST_county column to handle same-named counties in different states
    orig_df["state_county"] = orig_df["census_state"] + "_" + orig_df["census_county"]
    # Create dataframe of zeros with rows=sites and columns=species
    counties = orig_df.state_county.unique()
    species = orig_df.species.unique()
    heat_df = pandas.DataFrame(0, index=counties, columns=species)
    # Fill dataframe
    county_species_counts = read_species_to_dict(orig_df)
    for cty, sp_counts in county_species_counts.items():
        for (sp, count) in sp_counts:
            heat_df.loc[cty][sp] = count
    return heat_df


# .............................................................................
def reframe_to_pam(heat_df, min_val):
    """Create a dataframe of species columns by county rows from county species lists.

    Args:
        heat_df (pandas.DataFrame): DataFrame of species (columnns, x axis=1) by
            counties (rows, y axis=0, sites), with values = number of occurrences.
        min_val(numeric): Minimum value to be considered present/1 in the output matrix.

    Returns:
        pam_df (Pandas.DataFrame): DF of species (columnns, x axis=1) by counties
            (rows, y axis=0, sites), with values = 1 (presence) or 0 (absence).
    """
    try:
        # pandas 2.1.0, upgrade then replace "applymap" with "map"
        pam_df = heat_df.applymap(lambda x: 1 if x >= min_val else 0)
    except AttributeError:
        pam_df = heat_df.applymap(lambda x: 1 if x >= min_val else 0)
    return pam_df


# .............................................................................
def upload_to_s3(full_filename, bucket, bucket_path, logger):
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
        msg = f"Failed to upload {obj_name} to {bucket}, ({e})"
        if logger is not None:
            logger.log(logging.ERROR, msg)
        else:
            print(f"Error: {msg}")
    else:
        s3_filename = f"s3://{bucket}/{obj_name}"
        msg = f"Uploaded {s3_filename} to S3"
        if logger is not None:
            logger.log(logging.INFO, msg)
        else:
            print(f"INFO: {msg}")
    return s3_filename


# .............................................................................
class SpeciesDatasetMatrix:
    """Class for managing computations for occurrence counts of species x datasets."""

    # ...........................
    def __init__(self, species_dataset_df, logger):
        """Constructor for species by dataset comparisons.

        Args:
            species_dataset_df (pandas.DataFrame): A 2d matrix with species rows 
                (axis 0) by dataset columns (axis 1) to use for computations.
            logger (object): An optional local logger to use for logging output
                with consistent options
        """
        self._df = pam_df
        self.logger = logger
        self._report = {}

    # ...............................................
    @property
    def num_species(self):
        """Get the number of species (rows).

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
    def num_sites(self):
        """Get the number of datasets (columns)

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
            self.logger.log(logging.ERROR, f"Failed to write {filename}: {e}")

def reframe_to_species_x_dataset(df, logger):
    return df

# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    # Create a logger
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    logger, log_filename = get_logger(f"{script_name}_{todaystr}")

    # Read directly into DataFrame
    orig_df = read_s3_parquet_to_pandas(
        PROJ_BUCKET, SUMMARY_FOLDER, species_county_list_fname, logger, s3_client=None
    )

    species_dataset_df = reframe_to_species_x_dataset(orig_df, logger)
    pam = SpeciesDatasetMatrix(species_dataset_df, logger)


    # Upload logfile to S3
    s3_log_filename = upload_to_s3(log_filename, PROJ_BUCKET, LOG_PATH, logger)

"""
"""
