# --------------------------------------------------------------------------------------
# Constants for GBIF data, local dev machine, EC2, S3
# --------------------------------------------------------------------------------------
# GBIF data
base_url = "https://api.gbif.org/v1/occurrence/download/request/"
basename = "0042658-230530130749713"
ext = ".zip"
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

# Local machine data (and EC2 fname)
local_path = "/mnt/sata8/tank/testgbif/occ_2023_06_23"
gbif_basename = "occurrence_100k"
csv_fname = f"{gbif_basename}.csv"

# EC2
ec2_key = "/home/astewart/.ssh/aimee-aws-key.pem"
ec2_user = "ubuntu"
ec2_ip = "54.156.84.82"

# EC2 Spot Instance
iam_fleet_role = "arn:aws:iam::321942852011:user/aimee.stewart"
image_id = "spot_sp_network_analysis"
instance_type = "instant"
key_name = "aimee-aws-key"
# KU Dyche hall
security_group_id = "sg-0b379fdb3e37389d1"

# subnet_id = 'subnet-12345678'  # Replace with your subnet ID
# iam_instance_profile = 'arn:aws:iam::YOUR_ACCOUNT_ID:instance-profile/your-instance-profile'  # Replace with your IAM instance profile ARN

# S3
dev_bucket = "specify-network-dev"
bucket_path = "gbif_test/gbif_dwc_extract"
s3_csv_path = f"{bucket_path}/{gbif_basename}.csv"
s3_parquet_path = f"{bucket_path}/{gbif_basename}.parquet"

# ----------------------------------------------------
def get_key_pair():
    ec2 = boto3.client("ec2")
    response = ec2.describe_key_pairs()
    kps = response["KeyPairs"]
    for kp in kps:
        if kp["KeyName"] == key_name:
            return kp
    return None

# ----------------------------------------------------
def create_spot_ec2_instance(bucket, csv_path):
    key_pair = get_key_pair()
    if key_pair is not None:
        SpotFleetRequestConfig = {
            "IamFleetRole": iam_fleet_role,
            "LaunchSpecifications": [
                {
                    "ImageId": image_id,
                    "InstanceType": instance_type,
                    "KeyName": key_name,
                    "SecurityGroupIds": [security_group_id],
                    # "SubnetId": subnet_id,
                    # "IamInstanceProfile": {
                    #     "Arn": iam_instance_profile
                    # },
                },
            ],
            'TargetCapacity': 1,
            'Type': 'maintain',
            'InstanceInterruptionBehavior': 'terminate',
        }

    ec2_client = boto3.client("ec2")
    response = ec2_client.create_spot_fleet(
        SpotFleetRequestConfig={
            'SpotPrice': spot_price,
            'TargetCapacity': 1,
            'IamFleetRole': iam_fleet_role,
            'LaunchSpecifications': [
                {
                    'InstanceType': instance_type,
                    'ImageId': image_id,
                    'SubnetId': subnet_id
                }
            ]
        }
    )

    # Get the Spot Fleet request ID
    spot_fleet_request_id = response['SpotFleetRequestId']


# ----------------------------------------------------
def create_dataframe_from_s3_GBIF_csv(bucket, csv_path):
    # Read CSV file from S3 into a pandas DataFrame
    s3_client = boto3.client("s3")
    s3_obj = s3_client.get_object(Bucket=bucket, Key=csv_path)
    df = pandas.read_csv(
        s3_obj["Body"], delimiter="\t", encoding="utf-8", low_memory=False,
        quoting=csv.QUOTE_NONE)
    return df


# ----------------------------------------------------
def write_dataframe_to_s3_parquet(df, bucket, parquet_path):
    # Write DataFrame to Parquet format and upload to S3
    s3_client = boto3.client("s3")
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine="pyarrow")
    parquet_buffer.seek(0)
    s3_client.upload_fileobj(parquet_buffer, bucket, parquet_path)

# --------------------------------------------------------------------------------------
# On EC2: download from GBIF to EC2
# --------------------------------------------------------------------------------------
import requests

r = requests.get(f"{base_url}{basename}{ext}", stream=True)

with open(f"{basename}{ext}", "wb") as zfile:
    for chunk in r.iter_content(chunk_size=1024):
        # write one chunk at a time to zipfile
        if chunk:
            zfile.write(chunk)

# --------------------------------------------------------------------------------------
# On local machine: Create spot instance of EC2 and upload local file
# --------------------------------------------------------------------------------------
import os
import subprocess

local_filename = os.path.join(local_path, csv_fname)
cmd = f"scp -i {ec2_key} {local_filename} {ec2_user}@{ec2_ip}:"

info, err = subprocess.Popen(
    cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
if not err:
    ec2_file = f"/home/{ec2_user}/{csv_fname}"
    print(f"Successfully uploaded {ec2_file} to {ec2_ip}")


# --------------------------------------------------------------------------------------
# On EC2: Upload file from EC2 to S3
# --------------------------------------------------------------------------------------
import boto3

s3_client = boto3.client("s3")
s3_client.upload_file(ec2_file, dev_bucket, s3_csv_path)
print(f"Successfully uploaded {csv_fname} to s3://{dev_bucket}/{s3_csv_path}")

# --------------------------------------------------------------------------------------
# On local machine: Trim CSV file to named columns and save in parquet format
# --------------------------------------------------------------------------------------
import csv
import os
import pandas

local_filename = os.path.join(local_path, csv_fname)

gbif_dataframe = pandas.read_csv(
    local_filename, delimiter="\t", encoding="utf-8", low_memory=False,
    quoting=csv.QUOTE_NONE)

# Trim the DataFrame to the subset of fieldnames
trimmed_gbif_dataframe = gbif_dataframe[FIELD_SUBSET]

# Write the trimmed DataFrame to Parquet file format
parquet_file = 'output.parquet'
trimmed_gbif_dataframe.to_parquet(parquet_file)

# --------------------------------------------------------------------------------------
# On EC2: Create a trimmed dataframe from CSV and save to S3 in parquet format
# --------------------------------------------------------------------------------------
import boto3
import io
import pandas

# Create DataFrame from S3 CSV file
df = create_dataframe_from_s3_GBIF_csv(dev_bucket, bucket_path)

# Trim DataFrame to a subset of field names
trimmed_df = df[FIELD_SUBSET]

# Write DataFrame to S3 in Parquet format
write_dataframe_to_s3_parquet(trimmed_df, s3_parquet_path)


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
