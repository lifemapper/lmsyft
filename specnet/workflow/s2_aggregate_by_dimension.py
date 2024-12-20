"""Lambda function to aggregate counts and lists by region."""
import boto3
import botocore.session as bc
from botocore.client import Config
from datetime import datetime
import time

print("*** Loading function s2_aggregate_region")
PROJECT = "specnet"

# .............................................................................
# Dataload filename postfixes
# .............................................................................
dt = datetime.now()
yr = dt.year
mo = dt.month
prj_datestr = f"{yr}_{mo:02d}_01"

# .............................................................................
# AWS constants
# .............................................................................
REGION = "us-east-1"
AWS_ACCOUNT = "321942852011"
AWS_METADATA_URL = "http://169.254.169.254/latest/"
WORKFLOW_ROLE_NAME = f"{PROJECT}_workflow_role"

# S3 locations
S3_BUCKET = f"{PROJECT}-{AWS_ACCOUNT}-{REGION}"
S3_IN_DIR = "input"
S3_OUT_DIR = "output"
S3_LOG_DIR = "log"
S3_SUMMARY_DIR = "summary"
s3_prj_datestr = prj_datestr.replace('_', '-')
s3_summary_prefix = f"{S3_SUMMARY_DIR}/"
s3_summary_suffix = "_000.parquet"
s3_summary = f"s3://{S3_BUCKET}/{S3_SUMMARY_DIR}"
s3_out = f"s3://{S3_BUCKET}/{S3_OUT_DIR}"

# Redshift
db_user = f"IAMR:{WORKFLOW_ROLE_NAME}"
database = "dev"
pub_schema = "public"
external_schema = "redshift_spectrum"
# Wait time for completion of Redshift command
waittime = 5

# Name the project table with datestr
prj_tbl = f"{pub_schema}.{PROJECT}_{prj_datestr}"

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
# ...............................................
# ...............................................
# Aggregate occurrence, species counts by dimension
ds_counts_stmt = f"""
    CREATE TABLE {pub_schema}.{ds_counts_tbl} AS
        SELECT DISTINCT {gbif_ds_fld},
            COUNT(*) AS {out_occcount_fld},
            COUNT(DISTINCT {gbif_tx_fld}) AS {out_spcount_fld}
        FROM  {prj_tbl} WHERE {gbif_sp_fld} IS NOT NULL
        GROUP BY {gbif_ds_fld};
"""
ds_counts_export_stmt = f"""
    UNLOAD (
        'SELECT * FROM {pub_schema}.{ds_counts_tbl} ORDER BY {gbif_ds_fld}')
        TO '{s3_summary}/{ds_counts_tbl}_'
        IAM_role DEFAULT
        FORMAT AS PARQUET
        PARALLEL OFF;
"""
# ds_counts_export_stmt2 = f"""
#     UNLOAD (
#         'SELECT * FROM {pub_schema}.{ds_counts_tbl} ORDER BY {gbif_ds_fld}')
#         TO '{s3_out}/{ds_counts_tbl}_csv_'
#         IAM_role DEFAULT
#         ALLOWOVERWRITE
#         CSV DELIMITER AS ','
#         HEADER
#         PARALLEL OFF;
# """
# ...............................................
# Records of species, occ_count by dimension
# ...............................................
# Create species lists with counts and RIIS status for state, county, aiannh
ds_list_stmt = f"""
    CREATE TABLE {pub_schema}.{ds_list_tbl} AS
        SELECT DISTINCT {gbif_ds_fld}, {unique_sp_fld}, {gbif_tx_fld}, {gbif_sp_fld},
            COUNT(*) AS {out_occcount_fld}
        FROM  {prj_tbl} WHERE {gbif_ds_fld} IS NOT NULL AND {gbif_sp_fld} IS NOT NULL
        GROUP BY {gbif_ds_fld}, {unique_sp_fld}, {gbif_tx_fld}, {gbif_sp_fld};
"""
ds_list_export_stmt = f"""
    UNLOAD (
        'SELECT * FROM {pub_schema}.{ds_list_tbl} ORDER BY {gbif_ds_fld}, {gbif_sp_fld}')
        TO '{s3_summary}/{ds_list_tbl}_'
        IAM_role DEFAULT
        FORMAT AS PARQUET
        PARALLEL OFF;
"""
# ds_list_export_stmt2 = f"""
#     UNLOAD (
#         'SELECT * FROM {pub_schema}.{ds_list_tbl} ORDER BY {gbif_ds_fld}, {gbif_sp_fld}')
#         TO '{s3_out}/{ds_list_tbl}_csv_'
#         IAM_role DEFAULT
#         ALLOWOVERWRITE
#         CSV DELIMITER AS ','
#         HEADER
#         PARALLEL OFF;
# """
query_tables_stmt = f"SHOW TABLES FROM SCHEMA {database}.{pub_schema};"

REDSHIFT_COMMANDS = [
    ("query_tables", query_tables_stmt, None),
    # Create tables of dataset with species counts, occurrence counts
    ("create_counts_by_dataset", ds_counts_stmt, ds_counts_tbl),
    ("export_dataset_counts", ds_counts_export_stmt, ds_counts_tbl),
    # ("export_dataset_counts_csv", ds_counts_export_stmt2, ds_counts_tbl),
    # Create lists of dataset with species, occurrence counts, then export
    ("create_list_dataset_species", ds_list_stmt, ds_list_tbl),
    ("export_dataset_species", ds_list_export_stmt, ds_list_tbl),
    # ("export_dataset_species_csv", ds_list_export_stmt2, ds_list_tbl),
    ]


# --------------------------------------------------------------------------------------
def lambda_handler(event, context):
    """Aggregate records to species/occurrence counts and species lists by dataset.

    Args:
        event: AWS event triggering this function.
        context: AWS context of the event.

    Returns:
        JSON object

    Raises:
        Exception: on failure to execute Redshift command.
    """
    tables_present = []
    s3objs_present = []
    # -------------------------------------
    # FIRST: List current summary data from S3
    # -------------------------------------
    try:
        tr_response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET, Prefix=s3_summary_prefix, MaxKeys=10)
    except Exception as e:
        print(f"*** Error querying for objects in s3://{S3_BUCKET}/{s3_summary_prefix} ({e})")
    else:
        try:
            contents = tr_response["Contents"]
        except KeyError:
            print(f"*** No values in s3://{S3_BUCKET}/{s3_summary_prefix}")
        else:
            prefix_len = len(s3_summary_prefix)
            suffix_len = len(s3_summary_suffix)
            print(f"*** Contents: {contents}")
            print(f"*** Objects in s3://{S3_BUCKET}/{s3_summary_prefix}")
            for rec in contents:
                print(f"***    {rec}")
                fullkey = rec["Key"]
                if fullkey.find(s3_prj_datestr) > prefix_len:
                    key = fullkey[prefix_len:-suffix_len]
                    s3objs_present.append(key)
                    print(f"***      {key}")

    # -------------------------------------
    # SECOND: Check that current data is in Redshift
    # NEXT: Execute remaining aggregation and export commmands in order
    # -------------------------------------
    # s3objs_present populated above; tables_present populated in first RS command
    for (cmd, stmt, out_obj) in REDSHIFT_COMMANDS:
        if (
                (cmd.startswith("create") and out_obj in tables_present) or
                (cmd.startswith("export") and out_obj in s3objs_present)
        ):
            print("*** ---------------------------------------")
            print(f"*** Skipped command {cmd.upper()}, {out_obj} is present.")
        else:
            # Do not stop after a failure
            # -------------------------------------
            try:
                submit_result = rs_client.execute_statement(
                    WorkgroupName=PROJECT, Database=database, Sql=stmt)
            except Exception:
                raise

            print("*** ---------------------------------------")
            print(f"*** {cmd.upper()} command submitted")
            print(stmt)
            submit_id = submit_result['Id']

            # -------------------------------------
            # Loop til complete
            elapsed_time = 0
            complete = False
            while not complete:
                try:
                    describe_result = rs_client.describe_statement(Id=submit_id)
                except Exception as e:
                    complete = True
                    print(f"!!! Failed to describe_statement {e}")
                else:
                    status = describe_result["Status"]
                    if status in ("ABORTED", "FAILED", "FINISHED"):
                        complete = True
                        print(f"*** Status - {status} after {elapsed_time} seconds")
                        if status in ("ABORTED", "FAILED"):
                            try:
                                err = describe_result["Error"]
                            except Exception:
                                err = "Unknown Error"
                            print(f"!!!    FAILED: {err}")
                    else:
                        time.sleep(waittime)
                        elapsed_time += waittime

            # -------------------------------------
            # First redshift statement
            # -------------------------------------
            if cmd == "query_tables":
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
                    print(f"*** Summaries of {prj_datestr}")
                    try:
                        for rec in records:
                            tbl_name = rec[2]["stringValue"]
                            tables_present.append(tbl_name)
                            print(f"***      {tbl_name}")
                    except Exception as e:
                        raise Exception(f"!!! Unexpected record result {rec}, {e}")

    return {
        "statusCode": 200,
        "body": "Executed s2_aggregate_by_dimension lambda"
    }
