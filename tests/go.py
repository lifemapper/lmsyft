import boto3
import csv
import datetime
import io
import os
import pandas

# --------------------------------------------------------------------------------------
# Constants for GBIF data, local dev machine, EC2, S3
# --------------------------------------------------------------------------------------
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



# subnet_id = "subnet-12345678"  # Replace with your subnet ID
# iam_instance_profile = "arn:aws:iam::YOUR_ACCOUNT_ID:instance-profile/your-instance-profile"  # Replace with your IAM instance profile ARN

# ----------------------------------------------------
def get_key_pair(key_name):
    ec2_client = boto3.client("ec2")
    response = ec2_client.describe_key_pairs()
    kps = response["KeyPairs"]
    for kp in kps:
        if kp["KeyName"] == key_name:
            return kp
    return None

# ----------------------------------------------------
def create_token(type):
    token = f"{type}_{datetime.datetime.now().timestamp()}"
    return token

# ----------------------------------------------------
def create_spot_launch_template(spot_template_data, spot_template_name):
    template_token = create_token("template")
    ec2_client = boto3.client("ec2")
    response = ec2_client.create_launch_template(
        DryRun = False,
        ClientToken = template_token,
        LaunchTemplateName = spot_template_name,
        VersionDescription = "0.0.1",
        LaunchTemplateData = spot_template_data
    )
    success = (response["HTTPStatusCode"] == 200)
    return success

# ----------------------------------------------------
def create_spot_ec2_instance(spot_template_name, key_name):
    key_pair = get_key_pair(key_name)
    if key_pair is None:
        raise Exception(f"No credentials for key {key_name}")

    ec2_client = boto3.client("ec2")
    fleet_token = create_token("fleet")

    response = ec2_client.create_fleet(
        AllocationStrategy="priceCapacityOptimized",
        TargetCapacity=1,
        Type="request",
        DryRun=False,
        ClientToken=fleet_token,

        TargetCapacitySpecification={
            "TotalTargetCapacity": 1,
            "SpotTargetCapacity": 1,
            "DefaultTargetCapacityType": "spot",
            # "TargetCapacityUnitType": "vcpu" | "memory-mib" | "units"
        },
        # TerminateInstancesWithExpiration=True | False,
        SpotOptions={
            "AllocationStrategy": "price-capacity-optimized",
            "InstanceInterruptionBehavior": "terminate",
            "SingleInstanceType": True,
            "SingleAvailabilityZone": True
        },
        LaunchTemplateConfigs=[
            {
                "LaunchTemplateSpecification": {
                    "LaunchTemplateName": spot_template_name,
                    "Version": "1"
                }
            }
        ]
    )

    # Get the Spot Fleet request ID
    spot_fleet_request_id = response["SpotFleetRequestId"]
    return spot_fleet_request_id


# ----------------------------------------------------
def create_dataframe_from_gbifcsv_s3_bucket(bucket, csv_path):
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
# Run on EC2 or local: download from GBIF
# --------------------------------------------------------------------------------------
def download_from_gbif(gbif_basename):
    import requests
    r = requests.get(f"{GBIF_BASE_URL}{gbif_basename}{ZIP_EXT}", stream=True)
    with open(f"{gbif_basename}{ZIP_EXT}", "wb") as zfile:
        for chunk in r.iter_content(chunk_size=1024):
            # write one chunk at a time to zipfile
            if chunk:
                zfile.write(chunk)

# --------------------------------------------------------------------------------------
# On local machine: Upload local file
# --------------------------------------------------------------------------------------
def upload_to_ec2(local_path, filename, ec2_key, ec2_user, ec2_ip):
    import os
    import subprocess
    
    local_filename = os.path.join(local_path, filename)
    cmd = f"scp -i {ec2_key} {local_filename} {ec2_user}@{ec2_ip}:"
    
    info, err = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    if not err:
        ec2_file = f"/home/{ec2_user}/{filename}"
        print(f"Successfully uploaded {ec2_file} to {ec2_ip}")


# --------------------------------------------------------------------------------------
# Run on EC2 or local: Upload file to S3
# --------------------------------------------------------------------------------------
def upload_to_s3(local_path, filename, dev_bucket, s3_path):
    s3_client = boto3.client("s3")
    local_filename = os.path.join(local_path, filename)
    # ec2_file = f"/home/{ec2_user}/{csv_fname}"
    s3_client.upload_file(local_filename, dev_bucket, s3_path)
    print(f"Successfully uploaded {filename} to s3://{dev_bucket}/{s3_path}")

# --------------------------------------------------------------------------------------
# On local machine: Trim CSV file to named columns and save in parquet format
# --------------------------------------------------------------------------------------

def trim_gbifcsv_to_parquet(local_filename, parquet_filename):
    gbif_dataframe = pandas.read_csv(
        local_filename, delimiter="\t", encoding="utf-8", low_memory=False,
        quoting=csv.QUOTE_NONE)

    # Trim the DataFrame to the subset of fieldnames
    trimmed_gbif_dataframe = gbif_dataframe[FIELD_SUBSET]

    # Write the trimmed DataFrame to Parquet file format
    trimmed_gbif_dataframe.to_parquet(parquet_filename)

# --------------------------------------------------------------------------------------
# On EC2: Create a trimmed dataframe from CSV and save to S3 in parquet format
# --------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    # General AWS
    key_name = "aimee-aws-key"
    aws_zone = "us-east-1"
    # Allows KU Dyche hall
    security_group_id = "sg-0b379fdb3e37389d1"

    # Local machine data
    local_ec2_key = f"/home/astewart/.ssh/{key_name}.pem"
    local_path = "/mnt/sata8/tank/testgbif/occ_2023_06_23"
    test_basename = "occurrence_100k"
    gbif_basename = "0042658-230530130749713"
    csv_fname = f"{test_basename}.csv"

    # S3
    dev_bucket = "specify-network-dev"
    bucket_path = "gbif_test/gbif_dwc_extract"
    s3_csv_path = f"{bucket_path}/{test_basename}.csv"
    s3_parquet_path = f"{bucket_path}/{test_basename}.parquet"

    # EC2 persistent
    ec2_user = "ubuntu"
    ec2_ip = "54.156.84.82"

    # EC2 Spot Instance
    iam_fleet_role = "arn:aws:iam::321942852011:user/aimee.stewart"
    iam_instance_role = "arn:aws:iam::321942852011:user/aimee.stewart"
    image_id = "spot_sp_network_analysis"
    instance_type = "instant"
    spot_template_name = "transform_gbif_spot_launch_template"

    test_sn_spot_instance_id = "i-0997d00d72be2e1fc"

    spot_template_data = {
        "EbsOptimized": False,
        "BlockDeviceMappings": [
            {
                "DeviceName": "/dev/xvda",
                "Ebs": {"Encrypted": False,
                        "DeleteOnTermination": True,
                        "Iops": 3000,
                        # "SnapshotId": "snap-0d80fb72f6dbd3ff5",
                        "VolumeSize": 8,
                        "VolumeType": "gp3",
                        "Throughput": 125}
            },
            {
                "DeviceName": "/dev/sdb",
                "Ebs": {"Encrypted": False,
                        "DeleteOnTermination": False,
                        # "SnapshotId": "",
                        "VolumeSize": 400,
                        "VolumeType": "standard"}
            }
        ],
        "NetworkInterfaces": [
            {
                "AssociatePublicIpAddress": True,
                "DeleteOnTermination": True,
                "Description": "",
                "DeviceIndex": 0,
                "Groups": [security_group_id],
                "InterfaceType": "interface",
                "Ipv6Addresses": [],
                # "PrivateIpAddresses": [{"Primary": True, "PrivateIpAddress": "172.31.86.127"}],
                # "SubnetId": "subnet-0fec22aebe18e9994", "NetworkCardIndex": 0
            }
        ],
        "ImageId": "ami-06ca3ca175f37dd66",
        "InstanceType": "rd5.large",
        "KeyName": "aimee-aws-key",
        "Monitoring": {"Enabled": False},
        "Placement": {
            "AvailabilityZone": aws_zone,
            "GroupName": "",
            "Tenancy": "default"},
        "DisableApiTermination": False,
        "InstanceInitiatedShutdownBehavior": "terminate",
        "TagSpecifications": [
            {
                "ResourceType": "instance",
                "Tags": [{"Key": "Name", "Value": "test_sn_spot_instance"}]
            }
        ],
        "InstanceMarketOptions": {
            "MarketType": "spot",
            "SpotOptions": {
                # "MaxPrice": "0.011600",
                "SpotInstanceType": "one-time",
                "InstanceInterruptionBehavior": "terminate"
            }
        },
        "CreditSpecification": {"CpuCredits": "standard"},
        "CapacityReservationSpecification": {"CapacityReservationPreference": "open"},
        "HibernationOptions": {"Configured": False},
        "MetadataOptions": {
            "HttpTokens": "required",
            "HttpPutResponseHopLimit": 2,
            "HttpEndpoint": "enabled",
            "HttpProtocolIpv6": "disabled",
            "InstanceMetadataTags": "disabled"
        },
        "EnclaveOptions": {"Enabled": False},
        "PrivateDnsNameOptions": {
            "HostnameType": "ip-name",
            "EnableResourceNameDnsARecord": True,
            "EnableResourceNameDnsAAAARecord": False},
        "MaintenanceOptions": {"AutoRecovery": "default"},
        "DisableApiStop": False
    }

    # Local: Create a Spot EC2 instance
    #        Connect to Spot instance
    # Spot:  Download from GBIF
    #        Trim data and save as parquet format on Spot instance
    #        Upload data to S3, delete on Spot

    # Get or create template
    launch_template_id = create_spot_launch_template(spot_template_data, spot_template_name)
    spot_fleet_request_id = create_spot_ec2_instance(spot_template_data, key_name)
    # Create DataFrame from S3 CSV file
    df = create_dataframe_from_gbifcsv_s3_bucket(dev_bucket, bucket_path)

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
"""
import boto3
import csv
import datetime
import io
import os
import pandas

# --------------------------------------------------------------------------------------
# Constants for GBIF data, local dev machine, EC2, S3
# --------------------------------------------------------------------------------------
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

key_name = "aimee-aws-key"
aws_zone = "us-east-1"
# Allows KU Dyche hall
security_group_id = "sg-0b379fdb3e37389d1"

# Local machine data
local_ec2_key = f"/home/astewart/.ssh/{key_name}.pem"
local_path = "/mnt/sata8/tank/testgbif/occ_2023_06_23"
test_basename = "occurrence_100k"
gbif_basename = "0042658-230530130749713"
csv_fname = f"{test_basename}.csv"

# S3
dev_bucket = "specify-network-dev"
bucket_path = "gbif_test/gbif_dwc_extract"
s3_csv_path = f"{bucket_path}/{test_basename}.csv"
s3_parquet_path = f"{bucket_path}/{test_basename}.parquet"

# EC2 persistent
ec2_user = "ubuntu"
ec2_ip = "54.156.84.82"

# EC2 Spot Instance
# iam_fleet_role = "arn:aws:iam::321942852011:user/aimee.stewart"
iam_instance_role = "arn:aws:iam::321942852011:user/aimee.stewart"
# iam_instance_profile = arn:aws:iam::321942852011:instance-profile/<instance-profile-name
image_id = "spot_sp_network_analysis"
instance_type = "instant"
spot_template_name = "transform_gbif_spot_launch_template"

key_pair = get_key_pair(key_name)

ec2_client = boto3.client("ec2")

response = ec2_client.create_fleet(
    DryRun=True, LaunchTemplateConfigs=spot_config)

# Get the Spot Fleet request ID
spot_fleet_request_id = response["SpotFleetRequestId"]
return spot_fleet_request_id

"""