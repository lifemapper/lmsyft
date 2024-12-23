"""Lambda function to delete temporary and previous month's tables."""
import boto3
import botocore.session as bc
from botocore.client import Config
from datetime import datetime

import time

PROJECT = "specnet"
TASK = "delete_obsolete"
print(f"*** Loading function {PROJECT} workflow step {TASK} lambda")

# .............................................................................
# Dataload filename postfixes
# .............................................................................
dt = datetime.now()
yr = dt.year
mo = dt.month
prj_datestr = f"{yr}_{mo:02d}_01"
# Last month
prev_yr = yr
prev_mo = mo - 1
if mo == 1:
    prev_mo = 12
    prev_yr = yr - 1
old_prj_datestr = f"{prev_yr}_{prev_mo:02d}_01"

# .............................................................................
# AWS constants
# .............................................................................
REGION = "us-east-1"
AWS_ACCOUNT = "321942852011"
WORKFLOW_ROLE_NAME = f"{PROJECT}_workflow_role"

# S3 locations
S3_BUCKET = f"{PROJECT}-{AWS_ACCOUNT}-{REGION}"
S3_IN_DIR = "input"
S3_OUT_DIR = "output"
S3_LOG_DIR = "log"
S3_SUMMARY_DIR = "summary"

# Redshift
# namespace, workgroup both = 'bison'
db_user = f"IAMR:{WORKFLOW_ROLE_NAME}"
database = "dev"
pub_schema = "public"
external_schema = "redshift_spectrum"
# Wait time for completion of Redshift command
waittime = 5

# .............................................................................
# Initialize Botocore session and clients
# .............................................................................
timeout = 300
session = boto3.session.Session()
bc_session = bc.get_session()
session = boto3.Session(botocore_session=bc_session, region_name=REGION)
# Initialize Redshift client
config = Config(connect_timeout=timeout, read_timeout=timeout)
s3_client = session.client("s3", config=config, region_name=REGION)
rs_client = session.client("redshift-data", config=config)

# .............................................................................
# Data dimension parameters (species x dataset)
# .............................................................................
gbif_tx_fld = "taxonkey"
gbif_sp_fld = "species"
gbif_ds_fld = "datasetkey"
# Fields concatenated to ensure uniqueness
unique_sp_fld = "taxonkey_species"
out_occcount_fld = "occ_count"
out_spcount_fld = "species_count"

ds_counts_tbl = f"dataset_counts_{prj_datestr}"
ds_list_tbl = f"dataset_x_species_list_{prj_datestr}"

# .............................................................................
# Current, temporary, obsolete Redshift tables
# .............................................................................
tmp_prefix = "tmp_"
query_new_stmt = \
    f"SHOW TABLES FROM SCHEMA {database}.{pub_schema} LIKE '%{prj_datestr}';"
query_old_stmt = \
    f"SHOW TABLES FROM SCHEMA {database}.{pub_schema} LIKE '%{old_prj_datestr}';"
query_tmp_stmt = \
    f"SHOW TABLES FROM SCHEMA {database}.{pub_schema} LIKE '{tmp_prefix}%';"

# Queries for obsolete tables to be deleted
QUERY_COMMANDS = (
    ("query_tmp", query_tmp_stmt),
    ("query_old", query_old_stmt),
    ("query_new", query_new_stmt),
)


# --------------------------------------------------------------------------------------
def lambda_handler(event, context):
    """Delete previous month data and current month temporary tables.

    Args:
        event: AWS event triggering this function.
        context: AWS context of the event.

    Returns:
        JSON object

    Raises:
        Exception: on failure to execute Redshift query command.
        Exception: on failure to execute Redshift drop command.
    """
    tmp_tables = []
    old_tables = []
    new_tables = []
    for cmd, stmt in QUERY_COMMANDS:
        if cmd == "query_tmp":
            tbl_lst = tmp_tables
        elif cmd == "query_old":
            tbl_lst = old_tables
        else:
            tbl_lst = new_tables

        # -------------------------------------
        # Submit query request
        try:
            submit_result = rs_client.execute_statement(
                WorkgroupName=PROJECT, Database=database, Sql=stmt)
        except Exception as e:
            raise Exception(e)

        submit_id = submit_result['Id']
        print("*** ......................")
        print(f"*** {cmd.upper()} command submitted with Id {submit_id}")
        print(f"***    {stmt}")

        # -------------------------------------
        # Loop til complete, then get result status
        elapsed_time = 0
        complete = False
        while not complete:
            try:
                describe_result = rs_client.describe_statement(Id=submit_id)
            except Exception as e:
                complete = True
                print(f"Failed to describe_statement {e}")
            else:
                status = describe_result["Status"]
                if status in ("ABORTED", "FAILED", "FINISHED"):
                    complete = True
                    print(f"*** Status - {status} after {elapsed_time} seconds")
                    if status == "FAILED":
                        try:
                            err = describe_result["Error"]
                        except Exception:
                            err = "Unknown Error"
                        print(f"***    FAILED: {err}")
                else:
                    time.sleep(waittime)
                    elapsed_time += waittime

        # -------------------------------------
        # Get list of tables
        time.sleep(waittime * 2)
        try:
            stmt_result = rs_client.get_statement_result(Id=submit_id)
        except Exception as e:
            print(f"!!! No get_statement_result {e}")
            raise

        try:
            records = stmt_result["Records"]
        except Exception as e:
            print(f"!!! Failed to return any records ({e})")
        else:
            print(f"*** Tables from {cmd}:")
            try:
                for rec in records:
                    tbl = rec[2]["stringValue"]
                    tbl_lst.append(tbl)
                    print(f"***    {tbl}")
            except Exception as e:
                print(f"Failed to return records ({e})")

    # -------------------------------------
    # Determine which tables to remove
    print("*** ---------------------------------------")
    tables_to_remove = set(tmp_tables)
    print("*** Tables to remove:")
    print(f"***      {tmp_tables}")
    # Make sure new table exists before removing old table
    for old_tbl in old_tables:
        prefix = old_tbl[:-len(old_prj_datestr)]
        new_tbl = f"{prefix}{prj_datestr}"
        if new_tbl in new_tables:
            tables_to_remove.add(old_tbl)
            print(f"***      {old_tbl}")

    # -------------------------------------
    # Drop each table in list
    for tbl in tables_to_remove:
        drop_stmt = f"DROP TABLE IF EXISTS {pub_schema}.{tbl};"
        try:
            submit_result = rs_client.execute_statement(
                WorkgroupName=PROJECT, Database=database, Sql=drop_stmt)
        except Exception as e:
            raise Exception(e)

        submit_id = submit_result['Id']
        print("*** ......................")
        print(f"*** Drop table {tbl} submitted with Id: {submit_id}")
        print(f"***    {drop_stmt}")

        # -------------------------------------
        # Loop til complete, then get result status
        elapsed_time = 0
        complete = False
        while not complete:
            try:
                describe_result = rs_client.describe_statement(Id=submit_id)
            except Exception as e:
                complete = True
                print(f"*** Failed to describe_statement {e}")
            else:
                status = describe_result["Status"]
                if status in ("ABORTED", "FAILED", "FINISHED"):
                    complete = True
                    print(f"*** Status - {status} after {elapsed_time} seconds")
                    if status == "FAILED":
                        try:
                            err = describe_result["Error"]
                        except Exception:
                            err = "Unknown Error"
                        print(f"***    FAILED: {err}")
                else:
                    time.sleep(waittime)
                    elapsed_time += waittime

    # -------------------------------------
    # Last: List then remove obsolete summary data from S3
    # -------------------------------------
    sum_prefix = f"{S3_SUMMARY_DIR}/"
    # Find old and new versions of data
    old_keys = []
    new_keys = []
    try:
        tr_response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET, Prefix=sum_prefix, MaxKeys=10)
    except Exception as e:
        print(f"*** Error querying for objects in {sum_prefix} ({e})")
    else:
        try:
            contents = tr_response["Contents"]
        except KeyError:
            print(f"!!! No values in {S3_BUCKET}/{sum_prefix}")
        else:
            print("*** Keys found:")
            for rec in contents:
                key = rec["Key"]
                if key.find(old_prj_datestr) > len(sum_prefix):
                    old_keys.append(key)
                    print(f"***      {key}")
                elif key.find(prj_datestr) > len(sum_prefix):
                    new_keys.append(key)
                    print(f"***      {key}")

    # -------------------------------------
    # Determine which objects to remove from S3
    print("*** ---------------------------------------")
    keys_to_delete = set()
    print("*** Keys to remove:")
    # Make sure new table exists before removing old table
    for old_key in old_keys:
        prefix = old_key[:-len(old_prj_datestr)]
        new_key = f"{prefix}{prj_datestr}"
        if new_key in new_keys:
            keys_to_delete.add(old_key)
            print(f"***      {old_key}")

    for old_key in keys_to_delete:
        try:
            _ = s3_client.delete_object(
                Bucket=S3_BUCKET, Key=old_key)
        except Exception as e:
            print(f"!!! Error deleting object {old_key} ({e})")
        else:
            print(f"*** Deleted {old_key} with delete_object.")

    return {
        "statusCode": 200,
        "body": f"Executed {PROJECT} workflow step {TASK} lambda"
    }
