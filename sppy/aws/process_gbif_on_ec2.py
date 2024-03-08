import boto3
import csv
import datetime as dt
import logging
from logging.handlers import RotatingFileHandler
import os
import pandas
import requests
import zipfile

GBIF_BASE_URL = "https://api.gbif.org/v1/occurrence/download/request/"
ZIP_EXT = ".zip"
GBIF_OCC_FNAME = "occurrence.txt"
FIELD_SUBSET = [
    "gbifID",
    "datasetKey",
    "occurrenceID",
    "date",
    "locality",
    "decimalLongitude",
    "decimalLatitude",
    "countryCode",
    "stateProvince",
    "acceptedScientificName",
    "vernacularName",
    "taxonRank",
    "taxonomicStatus",
    "kingdom",
    "phylum",
    "class",
    "order",
    "family",
    ]

# Log processing progress
LOGINTERVAL = 1000000
LOG_FORMAT = " ".join(["%(asctime)s", "%(levelname)-8s", "%(message)s"])
LOG_DATE_FORMAT = '%d %b %Y %H:%M'
LOGFILE_MAX_BYTES = 52000000
LOGFILE_BACKUP_COUNT = 5

# TODO: Note that these constants are contained in the user_data script
#       and should be updated concurrently
gbif_basename = "0013468-230828120925497"
s3_dev_bucket = "specify-network-dev"
s3_bucket_path = "gbif_test/gbif_dwc_extract"

# ----------------------------------------------------
def get_logger(log_directory, log_name, log_level=logging.INFO):
    filename = f"{log_name}.log"
    if log_directory is not None:
        filename = os.path.join(log_directory, f"{filename}")
        os.makedirs(log_directory, exist_ok=True)
    # create file handler
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
    return logger


# ----------------------------------------------------
def logit(logger, msg, refname="", log_level=logging.INFO):
    """Log a message.

    Args:
        logger (obj): A logger object for writing messages to file.
        msg (str): A message to write to the logger.
        refname (str): Class or function name to use in logging message.
        log_level (int): A level to use when logging the message.
    """
    if logger is not None:
        logger.log(log_level, refname + ': ' + msg)


# ----------------------------------------------------
def download_from_gbif(gbif_basename, logger):
    local_path = os.getcwd()
    zip_filename = os.path.join(local_path, f"{gbif_basename}{ZIP_EXT}")
    r = requests.get(f"{GBIF_BASE_URL}{gbif_basename}{ZIP_EXT}", stream=True)
    with open(f"{gbif_basename}{ZIP_EXT}", "wb") as zfile:
        for chunk in r.iter_content(chunk_size=1024):
            # write one chunk at a time to zipfile
            if chunk:
                zfile.write(chunk)
    if os.path.exists(zip_filename):
        return zip_filename
    else:
        print(f"Failed to download to {zip_filename}")
        return None


# ----------------------------------------------------
def extract_occurrences_from_dwca(zip_filename, logger):
    local_path = os.path.dirname(zip_filename)
    orig_filename = os.path.join(local_path, GBIF_OCC_FNAME)
    # Extracts to PWD aka local_path
    with zipfile.ZipFile(zip_filename, "r") as zfile:
        zfile.extract(GBIF_OCC_FNAME)
    if os.path.exists(orig_filename):
        n = dt.datetime.now()
        new_filename = os.path.join(local_path, f"gbif-{n.year}-{n.month}-{n.day}.csv")
        os.rename(orig_filename, new_filename)
        logit(logger, f"Extracted {orig_filename} to {new_filename}")
        return new_filename
    else:
        logit(logger, f"Failed to extract {orig_filename}")
        return None


# ----------------------------------------------------
def trim_gbifcsv_to_parquet(csv_filename, logger):
    local_path, fname = os.path.split(csv_filename)
    basename = os.path.splitext(fname)[0]
    parquet_filename = os.path.join(local_path, f"{basename}.parquet")
    # Read into DataFrame
    gbif_dataframe = pandas.read_csv(
        csv_filename, delimiter="\t", encoding="utf-8", low_memory=False,
        quoting=csv.QUOTE_NONE)
    logit(logger, f"Read data from {csv_filename}")
    # Trim the DataFrame to the subset of fieldnames
    trimmed_gbif_dataframe = gbif_dataframe[FIELD_SUBSET]
    logit(logger, "Trimmed dataframe")
    # Write the trimmed DataFrame to Parquet file format
    trimmed_gbif_dataframe.to_parquet(parquet_filename)
    if os.path.exists(parquet_filename):
        logit(logger, f"Wrote trimmed data to {parquet_filename}")
        return parquet_filename
    else:
        logit(logger, f"Failed to write trimmed data to {parquet_filename}")
        return None


# ----------------------------------------------------
def upload_to_s3(parquet_filename, s3_bucket, s3_bucket_path, logger):
    s3_client = boto3.client("s3")
    s3_client.upload_file(parquet_filename, s3_bucket, s3_bucket_path)
    base_filename = os.path.basename(parquet_filename)
    logit(
        logger,
        f"Successfully uploaded {base_filename} to s3://{s3_bucket}/{s3_bucket_path}"
    )
    return f"s3://{s3_bucket}/{s3_bucket_path}/{base_filename}"


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    # Get logger
    logger = get_logger("/tmp", "spot_user_data", log_level=logging.INFO)
    logit(logger, "Got logger")

    # Download
    zip_filename = download_from_gbif(gbif_basename, logger)
    if zip_filename is None:
        logit(logger, f"Failed to download {zip_filename}")
        exit(-1)
    logit(logger, f"Succesfully downloaded {zip_filename}")

    # Unzip to dated filename
    csv_filename = extract_occurrences_from_dwca(zip_filename, logger)
    if csv_filename is None:
        logit(logger, f"Failed to extract {GBIF_OCC_FNAME}")
        exit(-1)
    logit(logger, f"Successfully extracted csv {csv_filename}")

    # Trim and save
    parquet_filename = trim_gbifcsv_to_parquet(csv_filename, logger)
    if parquet_filename is None:
        logit(logger, f"Failed to trim {csv_filename} to parquet")
        exit(-1)
    logit(logger, f"Successfully trimmed to parquet {parquet_filename}")

    # Upload to S3
    s3_filename = upload_to_s3(parquet_filename, s3_dev_bucket, s3_bucket_path, logger)
    # Delete old data
    os.remove(zip_filename)
    logit(logger, f"Removed {zip_filename}")
    os.remove(csv_filename)
    logit(logger, f"Removed {csv_filename}")
