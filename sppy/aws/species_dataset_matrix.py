"""Tools to compute dataset x species statistics from S3 data."""
import boto3
from botocore.exceptions import ClientError
import datetime as DT
import io
import logging
from logging.handlers import RotatingFileHandler
import os
import pandas
import sys

# Also in bison_ec2_constants, but provided here to avoid populating EC2 template with
# multiple files for userdata.
REGION = "us-east-1"
BUCKET = f"bison-321942852011-{REGION}"
BUCKET_PATH = "out_data"
LOG_PATH = "log"
LOCAL_OUTDIR = "/tmp"

n = DT.datetime.now()
# underscores for Redshift data
datestr = f"{n.year}_{n.month:02d}_01"
# date for logfile
todaystr = f"{n.year}-{n.month:02d}-{n.day:02d}"

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
    def __init__(self, pam_df, logger):
        """Constructor for PAM stats computations.

        Args:
            pam_df (pandas.DataFrame): A presence-absence matrix to use for computations.
            logger (object): An optional local logger to use for logging output
                with consistent options
        """
        self._pam_df = pam_df
        self.logger = logger
        self._report = {}

    # ...............................................
    @property
    def num_species(self):
        """Get the number of species with at least one site present.

        Returns:
            int: The number of species that are present somewhere.

        Note:
            Also used as gamma diversity (species richness over entire landscape)
        """
        count = 0
        if self._pam_df is not None:
            count = int(self._pam_df.any(axis=0).sum())
        return count

    # ...............................................
    @property
    def num_sites(self):
        """Get the number of sites with presences.

        Returns:
            int: The number of sites that have present species.
        """
        count = 0
        if self._pam_df is not None:
            count = int(self._pam_df.any(axis=1).sum())
        return count

    # ...............................................
    def alpha(self):
        """Calculate alpha diversity, the number of species in each site.

        Returns:
            alpha_series (pandas.Series): alpha diversity values for each site.
        """
        alpha_series = None
        if self._pam_df is not None:
            alpha_series = self._pam_df.sum(axis=1)
            alpha_series.name = "alpha_diversity"
        return alpha_series

    # ...............................................
    def alpha_proportional(self):
        """Calculate proportional alpha diversity - percentage of species in each site.

        Returns:
            alpha_pr_series (pandas.Series): column of proportional alpha diversity values for
                each site.
        """
        alpha_pr_series = None
        if self._pam_df is not None:
            alpha_pr_series = self._pam_df.sum(axis=1) / float(self.num_species)
            alpha_pr_series.name = "alpha_proportional_diversity"
        return alpha_pr_series

    # .............................................................................
    def phi(self):
        """Calculate phi, the range size per site.

        Returns:
            phi_series (pandas.Series): column of sum of the range sizes for the species present at each
                site in the PAM.
        """
        phi_series = None
        if self._pam_df is not None:
            phi_series = self._pam_df.dot(self._pam_df.sum(axis=0))
            phi_series.name = "phi_range_sizes"
        return phi_series

    # .............................................................................
    def phi_average_proportional(self):
        """Calculate proportional range size per site.

        Returns:
            phi_avg_pr_series (pandas.Series): column of the proportional value of the
                sum of the range sizes for the species present at each site in the PAM.
        """
        phi_avg_pr_series = None
        if self._pam_df is not None:
            phi_avg_pr_series = self._pam_df.dot(
                self.omega()).astype(float) / (self.num_sites * self.alpha())
            phi_avg_pr_series.name = "phi_average_proportional_range_sizes"
        return phi_avg_pr_series

    # ...............................................
    def beta(self):
        """Calculate beta diversity for each site, Whitaker's ratio: gamma/alpha.

        Returns:
            beta_series (pandas.Series): ratio of gamma to alpha for each site.

        TODO: revisit this definition, also consider beta diversity region compared
            to region
        """
        import numpy
        beta_series = None
        if self._pam_df is not None:
            beta_series = float(self.num_species) / self._pam_df.sum(axis=1)
            beta_series.replace([numpy.inf, -numpy.inf], 0, inplace=True)
            beta_series.name = "whittakers_gamma/alpha_ratio_beta_diversity"
        return beta_series

    # ...............................................
    def omega(self):
        """Calculate the range size (number of counties) per species.

        Returns:
            omega_series (pandas.Series): A row of range sizes for each species.
        """
        omega_series = None
        if self._pam_df is not None:
            omega_series = self._pam_df.sum(axis=0)
            omega_series.name = "omega"
        return omega_series

    # ...............................................
    def omega_proportional(self):
        """Calculate the mean proportional range size of each species.

        Returns:
            beta_series (pandas.Series): A row of the proportional range sizes for
                each species.
        """
        omega_pr_series = None
        if self._pam_df is not None:
            omega_pr_series = self._pam_df.sum(axis=0) / float(self.num_sites)
            omega_pr_series.name = "omega_proportional"
        return omega_pr_series

    # .............................................................................
    def psi(self):
        """Calculate the range richness of each species.

        Returns:
            psi_df (pandas.Series): A Series of range richness for the sites that
                each species is present in.

        TODO: revisit this
        """
        psi_series = None
        if self._pam_df is not None:
            psi_series = self._pam_df.sum(axis=1).dot(self._pam_df)
            psi_series.name = "psi"
        return psi_series

    # .............................................................................
    def psi_average_proportional(self):
        """Calculate the mean proportional range richness.

        Returns:
            psi_avg_df (pandas.DataFrame): A Series of proportional range richness
                for the sites that each species in the PAM is present.

        TODO: revisit this
        """
        psi_avg_series = None
        if self._pam_df is not None:
            psi_avg_series = (
                    self.alpha().dot(self._pam_df).astype(float)
                    / (self.num_species * self.omega())
            )
            psi_avg_series.name = "psi_average_proportional"
        return psi_avg_series

    # ...............................................
    def whittaker(self):
        """Calculate Whittaker's beta diversity metric for a PAM.

        Returns:
            whittaker_dict: Whittaker's beta diversity for the PAM.
        """
        whittaker_dict = {}
        if self._pam_df is not None:
            whittaker_dict["whittaker_beta_diversity"] = float(
                self.num_species / self.omega_proportional().sum())
        return whittaker_dict

    # ...............................................
    def lande(self):
        """Calculate Lande's beta diversity metric for a PAM.

        Returns:
            lande_dict: Lande's beta diversity for the PAM.
        """
        lande_dict = {}
        if self._pam_df is not None:
            lande_dict["lande_beta_diversity"] = float(
                self.num_species -
                (self._pam_df.sum(axis=0).astype(float) / self.num_sites).sum()
            )
        return lande_dict

    # ...............................................
    def legendre(self):
        """Calculate Legendre's beta diversity metric for a PAM.

        Returns:
            legendre_dict: Legendre's beta diversity for the PAM.
        """
        legendre_dict = {}
        if self._pam_df is not None:
            legendre_dict["legendre_beta_diversity"] = float(
                self.omega().sum() -
                (float((self.omega() ** 2).sum()) / self.num_sites)
            )
        return legendre_dict

    # ...............................................
    def calculate_diversity_statistics(self):
        """Compute PAM diversity statistics.

        Returns:
            diversity_df (pandas.DataFrame): A matrix of values with columns as
                diversity metric names, and one row with values.
        """
        diversity_df = None
        if self._pam_df is not None:
            # Merge dictionaries using unpack operator d5 = {**d1, **d2}
            diversity_stats = {
                **self.lande(), **self.legendre(), **self.whittaker(),
                **{"num_sites": self.num_sites}, **{"num_species": self.num_species}
            }
            diversity_df = pandas.DataFrame(diversity_stats, index=["value"])
        return diversity_df

    # ...............................................
    def calculate_site_statistics(self):
        """Calculate site-based statistics.

        Returns:
            site_stats_matrix(pandas.DataFrame): A matrix of site-based statistics for
                the selected metrics.  Columns are statistic names, rows are sites
                (counties).
        """
        site_stats_df = None
        if self._pam_df is not None:
            site_stats = [
                self.alpha(), self.alpha_proportional(), self.beta(), self.phi(),
                self.phi_average_proportional()]
            # Create matrix with each series as a row, columns = sites, rows = stats
            site_stats_df = pandas.DataFrame(site_stats)
            # Transpose to put statistics in columns, sites/counties in rows
            site_stats_df = site_stats_df.T
        return site_stats_df

    # ...............................................
    def calculate_species_statistics(self):
        """Calculate site-based statistics.

        Returns:
            species_stats_df (pandas.DataFrame): A matrix of species-based statistics
                for the selected metrics.  Columns are statistics, rows are species.
        """
        species_stats_df = None
        if self._pam_df is not None:
            species_stats = [
                self.omega(), self.omega_proportional(), self.psi(),
                self.psi_average_proportional()
            ]
            # Create matrix with each series as a row, columns = species, rows = stats
            species_stats_df = pandas.DataFrame(species_stats)
            # Transpose to put columns = stats, rows = species
            species_stats_df = species_stats_df.T
        return species_stats_df

    # ...............................................
    def write_to_csv(self, filename):
        """Write dataframe as CSV format, with comma separator, utf-8 format.

        Args:
            filename: full path to output file.
        """
        try:
            self._pam_df.to_csv(filename)
        except Exception as e:
            self.logger.log(logging.ERROR, f"Failed to write {filename}: {e}")


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    # Create a logger
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    logger, log_filename = get_logger(f"{script_name}_{todaystr}")

    # Read directly into DataFrame
    orig_df = read_s3_parquet_to_pandas(
        BUCKET, BUCKET_PATH, species_county_list_fname, logger, s3_client=None
    )

    heat_df = reframe_to_heatmatrix(orig_df, logger)
    pam_df = reframe_to_pam(heat_df, 1)
    pam = SiteMatrix(pam_df, logger)

    diversity_df = pam.calculate_diversity_statistics()
    site_stat_df = pam.calculate_site_statistics()
    species_stat_df = pam.calculate_species_statistics()

    diversity_stats_dataname = os.path.join(LOCAL_OUTDIR, f"diversity_stats_{datestr}.csv")
    species_stats_dataname = os.path.join(LOCAL_OUTDIR, f"species_stats_{datestr}.csv")
    site_stats_dataname = os.path.join(LOCAL_OUTDIR, f"site_stats_{datestr}.csv")

    diversity_df.to_csv(diversity_stats_dataname)
    site_stat_df.to_csv(site_stats_dataname)
    species_stat_df.to_csv(species_stats_dataname)

    # Write CSV and Parquet versions to S3
    s3_outpath = f"s3://{BUCKET}/{BUCKET_PATH}"
    diversity_df.to_csv(f"{s3_outpath}/diversity_stats_{datestr}.csv")
    site_stat_df.to_csv(f"{s3_outpath}/site_stats_{datestr}.csv")
    species_stat_df.to_csv(f"{s3_outpath}/species_stats_{datestr}.csv")
    diversity_df.to_parquet(f"{s3_outpath}/diversity_stats_{datestr}.parquet")
    site_stat_df.to_parquet(f"{s3_outpath}/site_stats_{datestr}.parquet")
    species_stat_df.to_parquet(f"{s3_outpath}/species_stats_{datestr}.parquet")

    # Upload logfile to S3
    s3_log_filename = upload_to_s3(log_filename, BUCKET, LOG_PATH, logger)

"""
from aws_scripts.bison_matrix_stats import (
    read_s3_parquet_to_pandas,  reframe_to_heatmatrix, reframe_to_pam, get_logger,
    SiteMatrix)

import boto3
from botocore.exceptions import ClientError
import datetime as DT
from importlib import reload
import io
import logging
from logging.handlers import RotatingFileHandler
import os
import pandas
import sys

REGION = "us-east-1"
BUCKET = f"bison-321942852011-{REGION}"
BUCKET_PATH = "out_data"
LOG_PATH = "log"

n = DT.datetime.now()
# underscores for Redshift data
datestr = f"{n.year}_{n.month:02d}_01"
todaystr = f"{n.year}_{n.month:02d}_{n.day:02d}"

species_county_list_fname = f"county_lists_{datestr}_000.parquet"
# Log processing progress
LOGINTERVAL = 1000000
LOG_FORMAT = " ".join(["%(asctime)s", "%(levelname)-8s", "%(message)s"])
LOG_DATE_FORMAT = "%d %b %Y %H:%M"
LOGFILE_MAX_BYTES = 52000000
LOGFILE_BACKUP_COUNT = 5
LOCAL_OUTDIR = "."

s3_outpath = f"s3://{BUCKET}/{BUCKET_PATH}"

# Create a logger
script_name = "testing"
logger, log_filename = get_logger(f"{script_name}_{todaystr}")

orig_df = read_s3_parquet_to_pandas(
    BUCKET, BUCKET_PATH, species_county_list_fname, logger, s3_client=None)
heat_df = reframe_to_heatmatrix(orig_df, logger)
pam_df = reframe_to_pam(heat_df, 1)

# Pandas DataFrame matrix
pam = SiteMatrix(pam_df, logger)
diversity_df = pam.calculate_diversity_statistics()
site_stat_df = pam.calculate_site_statistics()
species_stat_df = pam.calculate_species_statistics()

LOCAL_OUTDIR = "."
diversity_stats_dataname = os.path.join(LOCAL_OUTDIR, f"diversity_stats_{datestr}.csv")
species_stats_dataname = os.path.join(LOCAL_OUTDIR, f"species_stats_{datestr}.csv")
site_stats_dataname = os.path.join(LOCAL_OUTDIR, f"site_stats_{datestr}.csv")

diversity_df.to_csv(diversity_stats_dataname)
site_stat_df.to_csv(site_stats_dataname)
species_stat_df.to_csv(species_stats_dataname)

diversity_df.to_csv(f"{s3_outpath}/diversity_stats_{datestr}.csv")
site_stat_df.to_csv(f"{s3_outpath}/site_stats_{datestr}.csv")
species_stat_df.to_csv(f"{s3_outpath}/species_stats_{datestr}.csv")

diversity_df.to_parquet(f"{s3_outpath}/diversity_stats_{datestr}.parquet")
site_stat_df.to_parquet(f"{s3_outpath}/site_stats_{datestr}.parquet")
species_stat_df.to_parquet(f"{s3_outpath}/species_stats_{datestr}.parquet")




# # new diversity stats
# lande_new = pam.lande()
# legendre_new = pam.legendre()
# whittaker_new = pam.whittaker()
# species_ct_new = pam.num_species
# site_ct_new = pam.num_sites
#
# # new site stats
# beta_new = pam.beta()
# alpha_new = pam.alpha()
# alpha_proportional_new = pam.alpha_proportional()
# phi_new = pam.phi()
# phi_average_proportional_new = pam.phi_average_proportional()
#
# # new species stats
# omega_new = pam.omega()
# omega_proportional_new = pam.omega_proportional()
# psi_new = pam.psi()
# psi_average_proportional_new = pam.psi_average_proportional()
#
# # OLD matrix
# # create a multi-index for rows required by old-style SiteMatrix
# full_idx = pandas.MultiIndex.from_arrays(
#     [[i for i in range(len(pam_df.index))], pam_df.index], names=["row_idx", "region"])
# pam_df.index = full_idx
# pam_old = SiteMatrix(dataframe=pam_df, logger=logger)
# pam_old._min_presence = 1
#
# # old diversity stats
# lande_old = pam_old.lande()
# legendre_old = pam_old.legendre()
# whittaker_old = pam_old.whittaker()
# species_ct_old = pam_old.num_species
# site_ct_old = pam_old.num_sites
# # old site stats
# beta_old = pam_old.beta()
# alpha_old = pam_old.alpha()
# alpha_proportional_old = pam_old.alpha_proportional()
# # old species stats
# omega_old = pam_old.omega()
# omega_proportional_old = pam_old.omega_proportional()
# psi_old = pam_old.psi(),
# psi_average_proportional_old = pam_old.psi_average_proportional()
#
# # COMPARE diversity stats
# print(f"lande: old {lande_old} vs new {lande_new}")
# print(f"legendre: old {legendre_old} vs new {legendre_new}")
# print(f"whittaker: old {whittaker_old} vs new {whittaker_new}")
# print(f"species_ct: old {species_ct_old} vs new {species_ct_new}")
# print(f"site_ct: old {site_ct_old} vs new {site_ct_new}")
# print(f"beta old = new: {beta_old.equals(beta_new)}")
# print(f"alpha old = new: {alpha_old.equals(alpha_new)}")
# print(f"alpha_proportional old = new: {alpha_proportional_old.equals(alpha_proportional_new)}")
# print(f"omega old = new: {omega_old.equals(omega_new)}")
# print(f"omega_proportional old = new: {omega_proportional_old.equals(omega_proportional_new)}")
# print(f"psi old = new: {psi_old.equals(psi_new)}")
# print(f"psi_average_proportional old = new: {psi_average_proportional_old.equals(psi_average_proportional_new)}")



# Upload logfile to S3
s3_log_filename = upload_to_s3(log_filename, BUCKET, LOG_PATH)

"""
