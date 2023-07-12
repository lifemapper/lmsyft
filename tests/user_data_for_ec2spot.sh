#!/bin/bash

sudo su -
apt-get update -y
apt-get upgrade -y
apt-get install -y python3-pip

pip3 install pandas pyarrow requests

cat <<EOF > process_data.py
import os
import pandas
import requests
import zipfile

GBIF_BASE_URL = "https://api.gbif.org/v1/occurrence/download/request/"
ZIP_EXT = ".zip"
FIELD_SUBSET = [
    "gbifID",
    "datasetKey",
    "occurrenceID",
    "date",
    "locality",
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

gbif_basename = "0146234-230224095556074"
s3_dev_bucket = "specify-network-dev"
s3_bucket_path = "gbif_test/gbif_dwc_extract"

def download_from_gbif(gbif_basename):
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


def extract_occurrences_from_dwca(zip_filename):
    local_path = os.path.dirname(zip_filename)
    csv_filename = os.path.join(local_path, "occurrences.txt")
    with zipfile.ZipFile(zip_filename, "r") as zfile:
        zfile.extractall(local_path)
    if os.path.exists(csv_filename):
        return csv_filename
    else:
        print(f"Failed to extract to {csv_filename}")
        return None


def trim_gbifcsv_to_parquet(csv_filename, parquet_basename):
    local_path = os.path.dirname(csv_filename)
    parquet_filename = os.path.join(local_path, f"{parquet_basename}.parquet")
    # Read into DataFrame
    gbif_dataframe = pandas.read_csv(
        csv_filename, delimiter="\t", encoding="utf-8", low_memory=False,
        quoting=csv.QUOTE_NONE)
    # Trim the DataFrame to the subset of fieldnames
    trimmed_gbif_dataframe = gbif_dataframe[FIELD_SUBSET]
    # Write the trimmed DataFrame to Parquet file format
    trimmed_gbif_dataframe.to_parquet(parquet_filename)
    if os.path.exists(parquet_filename):
        return parquet_filename
    else:
        print(f"Failed to write to {parquet_filename}")
        return None


def upload_to_s3(filename, s3_bucket, s3_bucket_path):
    s3_client = boto3.client("s3")
    s3_client.upload_file(parquet_filename, s3_bucket, s3_bucket_path)
    base_filename = os.path.basename(filename)
    print(f"Successfully uploaded {base_filename} to s3://{s3_bucket}/{s3_bucket_path}")
    return s3://{s3_bucket}/{s3_bucket_path}/{base_filename}

# Download
zip_filename = download_from_gbif(gbif_basename)
if zip_filename is not None:
    print(f"Succesfully downloaded {zip_filename}")
    # Unzip
    csv_filename = extract_occurrences_from_dwca(zip_filename)
    if csv_filename is not None:
        print(f"Succesfully extracted {csv_filename}")
        # Trim and save
        parquet_filename = trim_gbifcsv_to_parquet(csv_filename)
        if parquet_filename is not None:
            print(f"Succesfully trimmed and saved {parquet_filename}")
            # Upload to S3
            s3_filename = upload_to_s3(parquet_filename, s3_dev_bucket, s3_bucket_path)
            print(f"Succesfully uploaded to s3 {s3_filename}")
            # Delete old data
            os.remove(zip_filename)
            os.remove(csv_filename)
EOF

python3 process_data.py


