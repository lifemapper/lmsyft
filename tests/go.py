# --------------------------------------------------------------------------------------
# Download from GBIF to EC2
# --------------------------------------------------------------------------------------
import os
import requests

base_url = "https://api.gbif.org/v1/occurrence/download/request/"
basename = "0042658-230530130749713"
ext = ".zip"

r = requests.get(f"{base_url}{basename}{ext}", stream=True)

with open(f"{basename}{ext}", "wb") as zfile:
    for chunk in r.iter_content(chunk_size=1024):
        # write one chunk at a time to zipfile
        if chunk:
            zfile.write(chunk)

# --------------------------------------------------------------------------------------
# Upload local file to EC2
# --------------------------------------------------------------------------------------
import subprocess

pth = "/mnt/sata8/tank/testgbif/occ_2023_06_23"
fname = "occurrence_100k.csv"
ec2_key = "/home/astewart/.ssh/aimee-aws-key.pem"
ec2_user = "ubuntu"
ec2_ip = "54.156.84.82"

local_filename = os.path.join(pth, fname)

cmd = f"scp -i {ec2_key} {local_filename} {ec2_user}@{ec2_ip}:"

info, err = subprocess.Popen(
    cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
if not err:
    ec2_file = f"/home/{ec2_user}/{fname}"
    print(f"Successfully uploaded {ec2_file} to {ec2_ip}")


# --------------------------------------------------------------------------------------
# Upload file from EC2 to S3
# --------------------------------------------------------------------------------------
import boto3

s3_client = boto3.client("s3")

dev_bucket = "specify-network-dev"
obj_path = "gbif_test/gbif_dwc_extract"
s3_dest = os.path.join(obj_path, fname)

s3_client.upload_file(ec2_file, dev_bucket, s3_dest)

print(f"Successfully uploaded {fname} to s3://{dev_bucket}/{s3_dest}")



# spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
# gbif_data = spark.read.csv(fname).cache()
#
# numAs = gbif_data
#
# # Set your source database and table name
# source_database = "gbif"
# source_table = "gbif_extract"


# # Set your Glue region
# glue_region = "us-east-1"
#
# spark.stop()
#
#
