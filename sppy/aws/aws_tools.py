"""Tools to use locally initiate BISON AWS EC2 Spot Instances."""
# --------------------------------------------------------------------------------------
# Imports
# --------------------------------------------------------------------------------------
import base64
import boto3
from botocore.exceptions import ClientError
import csv
import datetime
from http import HTTPStatus
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
import os
import requests
import xml.etree.ElementTree as ET

from sppy.aws.aws_constants import (
    ENCODING, INSTANCE_TYPE, KEY_NAME, LOGFILE_MAX_BYTES, LOG_FORMAT, LOG_DATE_FORMAT,
    PROJ_BUCKET, PROJ_NAME, REGION, SECURITY_GROUP_ID, SPOT_TEMPLATE_BASENAME,
    USER_DATA_TOKEN)


# --------------------------------------------------------------------------------------
# Methods for constructing and instantiating EC2 instances
# --------------------------------------------------------------------------------------
# ----------------------------------------------------
def get_secret(secret_name, region):
    """Get a secret from the Secrets Manager for connection authentication.

    Args:
        secret_name: name of the secret to retrieve.
        region: AWS region containint the secret.

    Returns:
        a dictionary containing the secret data.

    Raises:
        ClientError:  an AWS error in communication.
    """
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region)
    try:
        secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise (e)
    # Decrypts secret using the associated KMS key.
    secret_str = secret_value_response["SecretString"]
    return eval(secret_str)


# ----------------------------------------------------
def create_spot_launch_template_name(desc_str=None):
    """Create a name identifier for a Spot Launch Template.

    Args:
        desc_str (str): optional descriptor to include in the name.

    Returns:
        template_name (str): name for identifying this Spot Launch Template.
    """
    if desc_str is None:
        template_name = f"{PROJ_NAME}_{SPOT_TEMPLATE_BASENAME}"
    else:
        template_name = f"{PROJ_NAME}_{desc_str}_{SPOT_TEMPLATE_BASENAME}"
    return template_name


# ----------------------------------------------------
def define_spot_launch_template_data(
        template_name, user_data_filename, script_filename,
        token_to_replace=USER_DATA_TOKEN):
    """Create the configuration data for a Spot Launch Template.

    Args:
        template_name: unique name for this Spot Launch Template.
        user_data_filename: full filename for script to be included in the
            template and executed on Spot instantiation.
        script_filename: full filename for script to be inserted into user_data file.
        token_to_replace: string within the user_data_filename which will be replaced
            by the text in the script filename.

    Returns:
        launch_template_data (dict): Dictionary of configuration data for the template.
    """
    user_data_64 = get_user_data(
        user_data_filename, script_filename, token_to_replace=token_to_replace)
    launch_template_data = {
        "EbsOptimized": True,
        "IamInstanceProfile":
            {"Name": "AmazonEMR-InstanceProfile-20230404T163626"},
            #  "Arn":
            #      "arn:aws:iam::321942852011:instance-profile/AmazonEMR-InstanceProfile-20230404T163626",
            # },
        "BlockDeviceMappings": [
            {
                "DeviceName": "/dev/sda1",
                "Ebs": {
                    "Encrypted": False,
                    "DeleteOnTermination": True,
                    # "SnapshotId": "snap-0a6ff81ccbe3194d1",
                    "VolumeSize": 50, "VolumeType": "gp2"
                }
            }],
        "NetworkInterfaces": [
            {
                "AssociatePublicIpAddress": True,
                "DeleteOnTermination": True,
                "Description": "",
                "DeviceIndex": 0,
                "Groups": [SECURITY_GROUP_ID],
                "InterfaceType": "interface",
                "Ipv6Addresses": [],
                # "PrivateIpAddresses": [
                #     {"Primary": True, "PrivateIpAddress": "172.31.16.201"}
                # ],
                # "SubnetId": "subnet-0beb8b03a44442eef",
                # "NetworkCardIndex": 0
            }],
        "ImageId": "ami-0a0c8eebcdd6dcbd0",
        "InstanceType": INSTANCE_TYPE,
        "KeyName": KEY_NAME,
        "Monitoring": {"Enabled": False},
        "Placement": {
            "AvailabilityZone": "us-east-1c", "GroupName": "", "Tenancy": "default"},
        "DisableApiTermination": False,
        "InstanceInitiatedShutdownBehavior": "terminate",
        "UserData": user_data_64,
        "TagSpecifications": [
            {
                "ResourceType": "instance",
                "Tags": [{"Key": "TemplateName", "Value": template_name}]
            }],
        "InstanceMarketOptions": {
            "MarketType": "spot",
            "SpotOptions": {
                "MaxPrice": "0.033600",
                "SpotInstanceType": "one-time",
                "InstanceInterruptionBehavior": "terminate"
            }},
        "CreditSpecification": {"CpuCredits": "unlimited"},
        "CpuOptions": {"CoreCount": 2, "ThreadsPerCore": 1},
        "CapacityReservationSpecification": {"CapacityReservationPreference": "open"},
        "HibernationOptions": {"Configured": False},
        "MetadataOptions": {
            "HttpTokens": "optional",
            "HttpPutResponseHopLimit": 1,
            "HttpEndpoint": "enabled",
            "HttpProtocolIpv6": "disabled",
            "InstanceMetadataTags": "disabled"},
        "EnclaveOptions": {"Enabled": False},
        "PrivateDnsNameOptions": {
            "HostnameType": "ip-name",
            "EnableResourceNameDnsARecord": True,
            "EnableResourceNameDnsAAAARecord": False},
        "MaintenanceOptions": {"AutoRecovery": "default"},
        "DisableApiStop": False
    }
    return launch_template_data


# ----------------------------------------------------
def get_user_data(
        user_data_filename, script_filename=None, token_to_replace=USER_DATA_TOKEN):
    """Return the EC2 user_data script as a Base64 encoded string.

    Args:
        user_data_filename: Filename containing the user-data script to be executed on
            EC2 instantiation.
        script_filename: Filename containing a python script to be written to a file on
            the EC2 instantiation.
        token_to_replace: string within the user_data_filename which will be replaced
            by the text in the script filename.

    Returns:
        A Base64-encoded string of the user_data file to create on an EC2 instance.
    """
    # Insert an external script if provided
    if script_filename is not None:
        fill_user_data_script(user_data_filename, script_filename, token_to_replace)
    try:
        with open(user_data_filename, "r") as infile:
            script_text = infile.read()
    except Exception:
        return None
    else:
        text_bytes = script_text.encode("ascii")
        text_base64_bytes = base64.b64encode(text_bytes)
        base64_script_text = text_base64_bytes.decode("ascii")
        return base64_script_text


# ----------------------------------------------------
def fill_user_data_script(
        user_data_filename, script_filename, token_to_replace):
    """Fill the EC2 user_data script with a python script in another file.

    Args:
        user_data_filename: Filename containing the user-data script to be executed on
            EC2 instantiation.
        script_filename: Filename containing a python script to be written to a file on
            the EC2 instantiation.
        token_to_replace: string within the user_data_filename which will be replaced
            by the text in the script filename.

    Postcondition:
        The user_data file contains the text of the script file.
    """
    # Safely read the input filename using 'with'
    with open(user_data_filename) as f:
        s = f.read()
        if token_to_replace not in s:
            print(f"{token_to_replace} not found in {user_data_filename}.")
            return

    with open(script_filename) as sf:
        script = sf.read()

    # Safely write the changed content, if found in the file
    with open(user_data_filename, "w") as uf:
        print(
            f"Changing {token_to_replace} in {user_data_filename} to contents in "
            f"{script_filename}")
        s = s.replace(token_to_replace, script)
        uf.write(s)


# ----------------------------------------------------
def create_token(type=None):
    """Create a token to name and identify an AWS resource.

    Args:
        type (str): optional descriptor to include in the token string.

    Returns:
        token(str): token for AWS resource identification.
    """
    if type is None:
        type = PROJ_NAME
    token = f"{type}_{datetime.datetime.now().timestamp()}"
    return token


# ----------------------------------------------------
def get_today_str():
    """Get a string representation of the current date.

    Returns:
        date_str(str): string representing date in YYYY-MM-DD format.
    """
    n = datetime.datetime.now()
    date_str = f"{n.year}_{n.month:02d}_{n.day:02d}"
    return date_str


# ----------------------------------------------------
def get_current_datadate_str():
    """Get a string representation of the first day of the current month.

    Returns:
        date_str(str): string representing date in YYYY-MM-DD format.
    """
    n = datetime.datetime.now()
    date_str = f"{n.year}_{n.month:02d}_01"
    return date_str


# ----------------------------------------------------
def get_previous_datadate_str():
    """Get a string representation of the first day of the previous month.

    Returns:
        date_str(str): string representing date in YYYY-MM-DD format.
    """
    n = datetime.datetime.now()
    yr = n.year
    mo = n.month - 1
    if n.month == 0:
        mo = 12
        yr -= 1
    date_str = f"{yr}_{mo:02d}_01"
    return date_str


# ----------------------------------------------------
def create_spot_launch_template(
        ec2_client, template_name, user_data_filename, insert_script_filename=None,
        overwrite=False):
    """Create an EC2 Spot Instance Launch template on AWS.

    Args:
        ec2_client: an object for communicating with EC2.
        template_name: name for the launch template0
        user_data_filename: script to be installed and run on EC2 instantiation.
        insert_script_filename: optional script to be inserted into user_data_filename.
        overwrite: flag indicating whether to use an existing template with this name,
            or create a new

    Returns:
        success: boolean flag indicating the success of creating launch template.
    """
    success = False
    if overwrite is True:
        delete_launch_template(template_name)
    template = get_launch_template(template_name)
    if template is not None:
        success = True
    else:
        spot_template_data = define_spot_launch_template_data(
            template_name, user_data_filename, insert_script_filename)
        template_token = create_token("template")
        try:
            response = ec2_client.create_launch_template(
                DryRun=False,
                ClientToken=template_token,
                LaunchTemplateName=template_name,
                VersionDescription="Spot for Specify Network process",
                LaunchTemplateData=spot_template_data
            )
        except ClientError as e:
            print(f"Failed to create launch template {template_name}, ({e})")
        else:
            success = (response["ResponseMetadata"]["HTTPStatusCode"] == 200)
    return success


# ----------------------------------------------------
def upload_trigger_to_s3(trigger_name, s3_bucket, s3_bucket_path, region=REGION):
    """Upload a file to S3 which will trigger a workflow.

    Args:
        trigger_name: Name of workflow to trigger.
        s3_bucket: name of the S3 bucket destination.
        s3_bucket_path: the data destination inside the S3 bucket (without filename).
        region: AWS region to query.

    Returns:
        s3_filename: the URI to the file in the S3 bucket.
    """
    filename = f"{trigger_name}.txt"
    with open(filename, "r") as f:
        f.write("go!")
    s3_client = boto3.client("s3", region_name=region)
    obj_name = f"{s3_bucket_path}/{filename}"
    try:
        s3_client.upload_file(filename, s3_bucket, obj_name)
    except ClientError as e:
        print(f"Failed to upload {obj_name} to {s3_bucket}, ({e})")
    else:
        s3_filename = f"s3://{s3_bucket}/{obj_name}"
        print(f"Successfully uploaded {filename} to {s3_filename}")
    return s3_filename


# # ----------------------------------------------------
# def write_dataframe_to_s3_parquet(df, bucket, parquet_path, region=REGION):
#     """Convert DataFrame to Parquet format and upload to S3.
#
#     Args:
#         df: pandas DataFrame containing data.
#         bucket: name of the S3 bucket destination.
#         parquet_path: the data destination inside the S3 bucket
#         region: AWS region to query.
#     """
#     s3_client = boto3.client("s3", region_name=region)
#     parquet_buffer = io.BytesIO()
#     df.to_parquet(parquet_buffer, engine="pyarrow")
#     parquet_buffer.seek(0)
#     s3_client.upload_fileobj(parquet_buffer, bucket, parquet_path)


# ----------------------------------------------------
def upload_to_s3(local_filename, bucket, s3_path, region=REGION):
    """Upload a file to S3.

    Args:
        local_filename: Full path to local file for upload.
        bucket: name of the S3 bucket destination.
        s3_path: the data destination inside the S3 bucket (without filename).
        region: AWS region to query.
    """
    s3_client = boto3.client("s3", region_name=region)
    filename = os.path.split(local_filename)[1]
    s3_client.upload_file(local_filename, bucket, s3_path)
    print(f"Successfully uploaded {filename} to s3://{bucket}/{s3_path}")


# ----------------------------------------------------
def get_instance(instance_id, region=REGION):
    """Describe an EC2 instance with instance_id.

    Args:
        instance_id: EC2 instance identifier.
        region: AWS region to query.

    Returns:
        instance: metadata for the EC2 instance
    """
    ec2_client = boto3.client("ec2", region_name=region)
    response = ec2_client.describe_instances(
        InstanceIds=[instance_id],
        DryRun=False,
    )
    try:
        instance = response["Reservations"][0]["Instances"][0]
    except Exception:
        instance = None
    return instance


# ----------------------------------------------------
def run_instance_spot(ec2_client, template_name):
    """Run an EC2 Spot Instance on AWS.

    Args:
        ec2_client: an object for communicating with EC2.
        template_name: name for the launch template to be used for instantiation.

    Returns:
        instance_id: unique identifier of the new Spot instance.
    """
    instance_id = None
    spot_token = create_token(type="spot")
    instance_name = create_token()
    try:
        response = ec2_client.run_instances(
            # KeyName=key_name,
            ClientToken=spot_token,
            MinCount=1, MaxCount=1,
            LaunchTemplate={"LaunchTemplateName": template_name, "Version": "1"},
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [
                        {"Key": "Name", "Value": instance_name},
                        {"Key": "TemplateName", "Value": template_name}
                    ]
                }
            ]
        )
    except ClientError as e:
        print(f"Failed to instantiate Spot instance {spot_token}, ({e})")
    else:
        try:
            instance = response["Instances"][0]
        except KeyError:
            print("No instance returned")
        else:
            instance_id = instance["InstanceId"]
    return instance_id


# --------------------------------------------------------------------------------------
# Tools for experimentation
# --------------------------------------------------------------------------------------

# ----------------------------------------------------upload_to_s3
def _print_inst_info(reservation):
    resid = reservation["ReservationId"]
    inst = reservation["Instances"][0]
    print(f"ReservationId: {resid}")
    name = temp_id = None
    try:
        tags = inst["Tags"]
    except Exception:
        pass
    else:
        for t in tags:
            if t["Key"] == "Name":
                name = t["Value"]
            if t["Key"] == "aws:ec2launchtemplate:id":
                temp_id = t["Value"]
    ip = inst["PublicIpAddress"]
    state = inst["State"]["Name"]
    print(f"Instance name: {name}, template: {temp_id}, IP: {ip}, state: {state}")


# ----------------------------------------------------
def find_instances(key_name, launch_template_name):
    """Describe all EC2 instances with name or launch_template_id.

    Args:
        key_name: EC2 instance name
        launch_template_name: EC2 launch template name

    Returns:
        instances: list of metadata for EC2 instances
    """
    ec2_client = boto3.client("ec2")
    filters = []
    if launch_template_name is not None:
        filters.append({"Name": "tag:TemplateName", "Values": [launch_template_name]})
    if key_name is not None:
        filters.append({"Name": "key-name", "Values": [key_name]})
    response = ec2_client.describe_instances(
        Filters=filters,
        DryRun=False,
        MaxResults=123,
        # NextToken="string"
    )
    instances = []
    try:
        ress = response["Reservations"]
    except Exception:
        pass
    else:
        for res in ress:
            _print_inst_info(res)
            instances.extend(res["Instances"])
    return instances


# ----------------------------------------------------
def get_launch_template_from_instance(instance_id, region=REGION):
    """Return a JSON formatted template from an existing EC2 instance.

    Args:
        instance_id: unique identifier for the selected EC2 instance.
        region: AWS region to query.

    Returns:
        launch_template_data: a JSON formatted launch template.
    """
    ec2_client = boto3.client("ec2", region_name=region)
    launch_template_data = ec2_client.get_launch_template_data(InstanceId=instance_id)
    return launch_template_data


# --------------------------------------------------------------------------------------
# On local machine: Describe the launch_template with the template_name
def get_launch_template(template_name, region=REGION):
    """Return a JSON formatted template for a template_name.

    Args:
        template_name: unique name for the requested template.
        region: AWS region to query.

    Returns:
        launch_template_data: a JSON formatted launch template.
    """
    ec2_client = boto3.client("ec2", region_name=region)
    lnch_temp = None
    # Find pre-existing template
    try:
        response = ec2_client.describe_launch_templates(
            LaunchTemplateNames=[template_name],
        )
    except Exception:
        pass
    else:
        # LaunchTemplateName is unique
        try:
            lnch_temp = response["LaunchTemplates"][0]
        except Exception:
            pass
    return lnch_temp


# ----------------------------------------------------
def delete_launch_template(template_name, region=REGION):
    """Delete an EC2 launch template AWS.

    Args:
        template_name: name of the selected EC2 launch template.
        region: AWS region to query.

    Returns:
        response: a JSON formatted AWS response.
    """
    response = None
    ec2_client = boto3.client("ec2", region_name=region)
    lnch_tmpl = get_launch_template(template_name)
    if lnch_tmpl is not None:
        response = ec2_client.delete_launch_template(LaunchTemplateName=template_name)
    return response


# ----------------------------------------------------
def delete_instance(instance_id, region=REGION):
    """Delete an EC2 instance.

    Args:
        instance_id: unique identifier for the selected EC2 instance.
        region: AWS region to query.

    Returns:
        response: a JSON formatted AWS response.
    """
    ec2_client = boto3.client("ec2", region_name=region)
    response = ec2_client.delete_instance(InstanceId=instance_id)
    return response


# ----------------------------------------------------
def get_logger(log_name, log_dir=None, log_level=logging.INFO):
    """Get a logger for writing logging messages to file and console.

    Args:
        log_name: Name for the log object and output log file.
        log_dir: absolute path for the logfile.
        log_level: logging constant error level (logging.INFO, logging.DEBUG,
                logging.WARNING, logging.ERROR)

    Returns:
        logger: logging.Logger object
    """
    filename = f"{log_name}.log"
    if log_dir is not None:
        filename = os.path.join(log_dir, f"{filename}")
        os.makedirs(log_dir, exist_ok=True)
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
def create_dataframe_from_gbifcsv_s3_bucket(bucket, csv_path, region=REGION):
    """Read CSV data from S3 into a pandas DataFrame.

    Args:
        bucket: name of the bucket containing the CSV data.
        csv_path: the CSV object name with enclosing S3 bucket folders.
        region: AWS region to query.

    Returns:
        df: pandas DataFrame containing the CSV data.
    """
    s3_client = boto3.client("s3", region_name=region)
    s3_obj = s3_client.get_object(Bucket=bucket, Key=csv_path)
    df = pd.read_csv(
        s3_obj["Body"], delimiter="\t", encoding="utf-8", low_memory=False,
        quoting=csv.QUOTE_NONE)
    return df


# ----------------------------------------------------
def create_dataframe_from_s3obj(
        bucket, s3_path, datatype="parquet", region=REGION, encoding="utf-8"):
    """Read CSV data from S3 into a pandas DataFrame.

    Args:
        bucket: name of the bucket containing the CSV data.
        s3_path: the object name with enclosing S3 bucket folders.
        region: AWS region to query.
        datatype: tabular datatype, options are "csv", "parquet"

    Returns:
        df: pandas DataFrame containing the CSV data.
    """
    # import pyarrow.parquet as pq
    # import s3fs
    datatype = datatype.lower()
    if datatype == "csv":
        s3_client = boto3.client("s3", region_name=region)
        s3_obj = s3_client.get_object(Bucket=bucket, Key=s3_path)
        df = pd.read_csv(
            s3_obj["Body"], delimiter="\t", encoding=encoding, low_memory=False,
            quoting=csv.QUOTE_NONE)
    elif datatype == "parquet":
        s3_uri = f"s3://{bucket}/{s3_path}"
        # s3_fs = s3fs.S3FileSystem
        df = pd.read_parquet(s3_uri)
    return df


# # ...............................................
# def _get_nested_output_val(output, key_list):
#     while key_list:
#         key = key_list[0]
#         key_list = key_list[1:]
#         try:
#             output = output[key]
#             if not key_list:
#                 val = output
#                 # if type(val) is bytes:
#                 #     val = val.decode(ENCODING)
#                 return val
#         except Exception:
#             return None


# ...............................................
def _get_values_for_keys(output, keys):
    values = []
    # Get values from JSON response
    for key in keys:
        if type(key) is list or type(key) is tuple:
            key_list = key
            while key_list:
                key = key_list[0]
                key_list = key_list[1:]
                try:
                    output = output[key]
                    if not key_list:
                        val = output
                except Exception:
                    val = None
        else:
            try:
                val = output[key]
            except Exception:
                val = None
        values.append(val)
    return values


# ...............................................
def _get_api_response_vals(url, keys):
    values = []
    try:
        response = requests.get(url)
    except Exception as e:
        errmsg = str(e)
    else:
        try:
            status_code = response.status_code
            reason = response.reason
        except Exception:
            status_code = HTTPStatus.INTERNAL_SERVER_ERROR
            reason = "Unknown API status_code/reason"
        if status_code == HTTPStatus.OK:
            # Parse response
            try:
                output = response.json()
            except Exception:
                output = response.content
                if type(output) is bytes:
                    output = ET.fromstring(str(output))
                try:
                    output = ET.parse(output)
                except Exception as e:
                    errmsg = f"Provider error: Invalid JSON response ({output})"
            # Get values from JSON response
            values = _get_values_for_keys(output, keys)
    return values

# ...............................................
def get_dataset_name_citation(dataset_key):
    """Return title from one dataset record with this key.

    Args:
        dataset_key: GBIF identifier for this dataset

    Returns:
        dataset_name: the name of the dataset.
        citation: the preferred citation for the dataset.

    Raises:
        Exception: on query failure.
    """
    url = f"https://api.gbif.org/v1/dataset/{dataset_key}"
    name, citation = _get_api_response_vals(url, ["title", ["citation", "text"]])
    return name, citation


# ...............................................
def _parse_records(ret_records, keys):
    small_recs = []
    for rec in ret_records:
        values = _get_values_for_keys(rec, keys)
        small_recs.append(values)
    return small_recs

# ...............................................
def _get_records(url, keys):
    small_recs = []
    is_end = True
    try:
        response = requests.get(url)
    except Exception as e:
        errmsg = str(e)
    else:
        try:
            status_code = response.status_code
            reason = response.reason
        except Exception as e:
            status_code = HTTPStatus.INTERNAL_SERVER_ERROR
            reason = str(e)
        if status_code == HTTPStatus.OK:
            # Parse response
            try:
                output = response.json()
            except Exception:
                output = response.content
                if type(output) is bytes:
                    output = ET.fromstring(str(output))
                try:
                    output = ET.parse(output)
                except Exception:
                    reason = f"Provider error: Invalid JSON response ({output})"
            # Last query?
            try:
                is_end = output["endOfRecords"]
            except KeyError:
                print("Missing endOfRecords flag")
            # Get values from JSON response
            try:
                ret_records = output["results"]
            except KeyError:
                reason = "No results returned"
            else:
                small_recs = _parse_records(ret_records, keys)
    return small_recs, is_end


# ----------------------------------------------------
def create_dataframe_from_api(base_url, response_keys, output_columns):
    """Query an API, read the data and write a subset to a table in S3.

    Args:
        base_url: API URL without any key value pairs for the data service
        response_keys: list of keys within the API response to pull values from.  A key
            can be an ordered list of keys nested within several elements of the tree,
            from outermost to innermost.
        output_columns: list of column headings for output lookup table

    Returns:
        dataframe: Pandas dataframe with rows of data for the output_columns
    """
    all_recs = []
    is_end = False
    offset = 0
    limit = 100
    while is_end is False:
        url = f"{base_url}?offset={offset}&limit={limit}"
        small_recs, is_end = _get_records(url, response_keys)
        all_recs.append(small_recs)
        offset += limit
        if offset % 1000 == 0:
            print(f"Offset = {offset}")
    dataframe = pd.DataFrame(all_recs, columns=output_columns)
    print(f"Lookup table contains {dataframe.shape[0]} rows")
    return dataframe


# ----------------------------------------------------
def write_dataframe_to_s3(
        dataframe, bucket, s3_folders, output_fname, region=REGION, encoding="utf-8"):
    """Query an API, read the data and write a subset to a table in S3.

    Args:
        dataframe: Pandas dataframe with rows of data
        bucket: name of the bucket containing the CSV data.
        s3_folders: S3 bucket folders for output lookup table
        output_fname: output table for looking up dataset name and citation
        region: AWS region containing the destination bucket.
        encoding: encoding of the input data

    Postcondition:
        CSV table with output_columns and values for each written to the named S3 object
            in bucket and folders
    """
    output_path = f"{s3_folders}/{output_fname}"
    tmp_filename = f"/tmp/{output_fname}"
    # Output data written as CSV
    dataframe.to_csv(path_or_buf=tmp_filename, sep='\t', header=True, encoding=encoding)
    print(f"Wrote {tmp_filename}")
    upload_to_s3(tmp_filename, bucket, output_path, region=region)
    print(f"Uploaded to s3://{bucket}/{output_path}")


# ----------------------------------------------------
def create_s3_dataset_lookup(bucket, s3_folders, region=REGION, encoding="utf-8"):
    """Query the GBIF Dataset API, write a subset of the response to a table in S3.

    Args:
        bucket: name of the bucket containing the CSV data.
        s3_folders: S3 bucket folders for output lookup table
        region: AWS region containing the destination bucket.
        encoding: encoding of the input data

    Postcondition:
        CSV table with dataset key, pubOrgKey, dataset name, dataset citation written
            to the named S3 object in bucket and folders
    """
    base_url = "https://api.gbif.org/v1/dataset"
    response_keys = ["key", "publishingOrganizationKey", "title", ["citation", "text"]]
    data_date = get_current_datadate_str()
    output_fname = f"dataset_name_{data_date}_"
    output_fname = "dataset_name_2024_02_01_"
    output_columns = ["datasetKey", "publishingOrganizationKey", "title", "citation"]
    lookup_df = create_dataframe_from_api(base_url, response_keys, output_columns)
    write_dataframe_to_s3(
        lookup_df, bucket, s3_folders, output_fname, region=region, encoding=encoding)


# ----------------------------------------------------
def create_s3_organization_lookup(bucket, s3_folders, region=REGION, encoding="utf-8"):
    """Query the GBIF Organization API, write a subset of the response to a table in S3.

    Args:
        bucket: name of the bucket containing the CSV data.
        s3_folders: S3 bucket folders for output lookup table
        region: AWS region to query.
        encoding: encoding of the input data

    Postcondition:
        CSV table with pubOrgKey, pubOrg name written to the named S3 object in
            bucket and folders
    """
    base_url = "https://api.gbif.org/v1/dataset"
    response_keys = ["key", "title"]
    data_date = get_current_datadate_str()
    output_fname = f"organization_name_{data_date}_"
    output_fname = "organization_name_2024_02_01_"
    output_columns = ["publishingOrganizationKey", "title"]
    lookup_df = create_dataframe_from_api(base_url, response_keys, output_columns)
    write_dataframe_to_s3(
        lookup_df, bucket, s3_folders, output_fname, region=region, encoding=encoding)


# .............................................................................
if __name__ == "__main__":
    bucket=PROJ_BUCKET
    region=REGION
    encoding=ENCODING
    s3_folders="summary"

    base_url = "https://api.gbif.org/v1/dataset"
    response_keys = ["key", "publishingOrganizationKey", "title", ["citation", "text"]]
    output_fname = "dataset_name_2024_02_01_"
    output_columns = ["datasetKey", "publishingOrganizationKey", "title", "citation"]

    create_s3_dataset_lookup(
        bucket, s3_folders, region=REGION, encoding="utf-8")
    # create_s3_organization_lookup(
    #     bucket, s3_folders, region=REGION, encoding="utf-8")


"""
from sppy.aws.aws_tools  import *

bucket=PROJ_BUCKET
region=REGION
encoding=ENCODING
s3_folders="summary"

base_url = "https://api.gbif.org/v1/dataset"
response_keys = ["key", "publishingOrganizationKey", "title", ["citation", "text"]]
output_fname = "dataset_name_2024_02_01_"
output_columns = ["datasetKey", "publishingOrganizationKey", "title", "citation"]

create_s3_dataset_lookup(
    bucket, s3_folders, region=REGION, encoding="utf-8")
"""
