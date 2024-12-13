"""Lambda function to create bison records by subsetting public GBIF data."""
import boto3
import botocore.session as bc
from botocore.client import Config
from datetime import datetime
import time

print("*** Loading function s1_subset_gbif")
PROJECT = "specnet"

# .............................................................................
# Dataload filename postfixes
# .............................................................................
dt = datetime.now()
yr = dt.year
mo = dt.month
prev_yr = yr
prev_mo = mo - 1
if mo == 1:
    prev_mo = 12
    prev_yr = yr - 1
gbif_datestr = f"{yr}-{mo:02d}-01"
prj_datestr = f"{yr}_{mo:02d}_01"
old_prj_datestr = f"{prev_yr}_{prev_mo:02d}_01"

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
# S3 GBIF bucket and file to query
gbif_bucket = f"gbif-open-data-{REGION}"
parquet_key = f"occurrence/{gbif_datestr}/occurrence.parquet"
gbif_odr_data = f"s3://{gbif_bucket}/{parquet_key}/"

# Redshift
db_user = f"IAMR:{WORKFLOW_ROLE_NAME}"
database = "dev"
pub_schema = "public"
external_schema = "redshift_spectrum"
# Wait time for completion of Redshift command
waittime = 5

# Name the Redshift mounted gbif data and project table to create from it
prj_tbl = f"{pub_schema}.{PROJECT}_{prj_datestr}"
old_prj_tbl = f"{pub_schema}.{PROJECT}_{old_prj_datestr}"
mounted_gbif_name = f"{external_schema}.occurrence_{prj_datestr}_parquet"

# .............................................................................
# Initialize Botocore session and clients
# .............................................................................
timeout = 300
session = boto3.session.Session()
bc_session = bc.get_session()
session = boto3.Session(botocore_session=bc_session, region_name=REGION)
# Initialize Redshift client
config = Config(connect_timeout=timeout, read_timeout=timeout)
rs_client = session.client("redshift-data", config=config)

# .............................................................................
# Commands
# .............................................................................
query_tables_stmt = f"SHOW TABLES FROM SCHEMA {database}.{pub_schema};"

drop_schema_stmt = f"DROP SCHEMA IF EXISTS {external_schema};"

create_schema_stmt = f"""
    CREATE EXTERNAL SCHEMA IF NOT EXISTS {external_schema}
        FROM data catalog
        DATABASE '{database}'
        IAM_ROLE DEFAULT
        CREATE external database IF NOT EXISTS;
"""

mount_stmt = f"""
    CREATE EXTERNAL TABLE {mounted_gbif_name} (
        gbifid                              VARCHAR(max),
        datasetkey                          VARCHAR(max),
        occurrenceid                        VARCHAR(max),
        kingdom                             VARCHAR(max),
        phylum                              VARCHAR(max),
        class                               VARCHAR(max),
        _order                              VARCHAR(max),
        family                              VARCHAR(max),
        genus                               VARCHAR(max),
        species                             VARCHAR(max),
        infraspecificepithet                VARCHAR(max),
        taxonrank                           VARCHAR(max),
        scientificname                      VARCHAR(max),
        verbatimscientificname              VARCHAR(max),
        verbatimscientificnameauthorship    VARCHAR(max),
        countrycode                         VARCHAR(max),
        locality                            VARCHAR(max),
        stateprovince                       VARCHAR(max),
        occurrencestatus                    VARCHAR(max),
        individualcount                     INT,
        publishingorgkey                    VARCHAR(max),
        decimallatitude                     DOUBLE PRECISION,
        decimallongitude                    DOUBLE PRECISION,
        coordinateuncertaintyinmeters       DOUBLE PRECISION,
        coordinateprecision                 DOUBLE PRECISION,
        elevation                           DOUBLE PRECISION,
        elevationaccuracy                   DOUBLE PRECISION,
        depth                               DOUBLE PRECISION,
        depthaccuracy                       DOUBLE PRECISION,
        eventdate                           TIMESTAMP,
        day                                 INT,
        month                               INT,
        year                                INT,
        taxonkey                            INT,
        specieskey                          INT,
        basisofrecord                       VARCHAR(max),
        institutioncode                     VARCHAR(max),
        collectioncode                      VARCHAR(max),
        catalognumber                       VARCHAR(max),
        recordnumber                        VARCHAR(max),
        identifiedby                        SUPER,
        dateidentified                      TIMESTAMP,
        license                             VARCHAR(max),
        rightsholder                        VARCHAR(max),
        recordedby                          SUPER,
        typestatus                          SUPER,
        establishmentmeans                  VARCHAR(max),
        lastinterpreted                     TIMESTAMP,
        mediatype                           SUPER,
        issue                               SUPER
    )
    STORED AS PARQUET
    LOCATION '{gbif_odr_data}';
"""

subset_stmt = f"""
    CREATE TABLE {prj_tbl} AS
        SELECT
            gbifid, datasetkey, species, taxonrank, scientificname, countrycode, stateprovince,
            occurrencestatus, publishingorgkey, day, month, year, taxonkey, specieskey,
            basisofrecord, decimallongitude, decimallatitude,
            (taxonkey || ' ' || species) as taxonkey_species,
            ST_Makepoint(decimallongitude, decimallatitude) as geom
        FROM redshift_spectrum.occurrence_{prj_datestr}_parquet
        WHERE decimallatitude IS NOT NULL
          AND decimallongitude IS NOT NULL
          AND occurrencestatus = 'PRESENT'
          AND taxonrank IN
            ('SPECIES', 'SUBSPECIES', 'VARIETY', 'FORM', 'INFRASPECIFIC_NAME', 'INFRASUBSPECIFIC_NAME')
          AND basisofrecord IN ('OCCURRENCE', 'PRESERVED_SPECIMEN');
"""

count_gbif_stmt = f"SELECT COUNT(*) from {mounted_gbif_name};"
count_prj_stmt = f"SELECT COUNT(*) FROM {prj_tbl};"
unmount_stmt = f"DROP TABLE {mounted_gbif_name};"
query_users_stmt = """    
    select usesysid as user_id,
           usename as username,
           usecreatedb as db_create,
           usesuper as is_superuser,
           valuntil as password_expiration
    from pg_user
    order by user_id
    """

REDSHIFT_COMMANDS = [
    ("query_tables", query_tables_stmt),
    ("query_all_users", query_users_stmt),
    ("query_user", "SELECT current_user;"),
    ("schema", create_schema_stmt),
    # 2 secs
    ("mount", mount_stmt),
    # 5 secs
    ("query_mount", count_gbif_stmt),
    # 1 min
    ("subset", subset_stmt),
    # 2 secs
    ("query_subset", count_prj_stmt),
    # 1 secs
    ("unmount", unmount_stmt),
    ("drop_schema", drop_schema_stmt)
]


# --------------------------------------------------------------------------------------
def lambda_handler(event, context):
    """Subset GBIF data to a Redshift table, then add fields to it.

    Args:
        event: AWS event triggering this function.
        context: AWS context of the event.

    Returns:
        JSON object

    Raises:
        Exception: on failure to execute Redshift command.
    """
    success = True
    # -------------------------------------
    # No checks required
    # Mount GBIF, subset to BISON table, add fields, all in Redshift
    # -------------------------------------
    for (cmd, stmt) in REDSHIFT_COMMANDS:
        # Stop after a failure
        if success is False:
            break
        # -------------------------------------
        try:
            submit_result = rs_client.execute_statement(
                WorkgroupName=PROJECT, Database=database, Sql=stmt)
        except Exception as e:
            raise Exception(e)

        print("*** ......................")
        print(f"*** {cmd.upper()} command submitted")
        submit_id = submit_result['Id']

        # -------------------------------------
        # Loop til complete, then get result status
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
                        success = False
                        try:
                            err = describe_result["Error"]
                        except Exception:
                            err = "Unknown Error"
                        print(f"!!!    FAILED: {err}")
                else:
                    time.sleep(waittime)
                    elapsed_time += waittime

        # -------------------------------------
        # IF query, get statement output
        if cmd.startswith("query") and success is True:
            try:
                stmt_result = rs_client.get_statement_result(Id=submit_id)
            except Exception as e:
                print(f"!!! No get_statement_result {e}")
            else:
                try:
                    records = stmt_result["Records"]
                except Exception as e:
                    print(f"!!! Failed to return records ({e})")
                else:
                    if cmd == "query_tables":
                        tables_present = []
                        # tablename is 2nd item in record
                        for rec in records:
                            tables_present.append(rec[2]['stringValue'])
                        msg = f"***     Tables: {tables_present}"
                    elif cmd == "query_mount":
                        msg = f"***     GBIF COUNT = {records[0][0]['longValue']}"
                    elif cmd == "query_subset":
                        msg = f"***     {PROJECT} COUNT = {records[0][0]['longValue']}"
                    else:
                        results = []
                        for rec in records:
                            row = []
                            for fld_dict in rec:
                                val = list(fld_dict.values())[0]
                                row.append(val)
                            results.append(row)
                        msg = f"***     result = {records}"
                    print(msg)
    return {
        "statusCode": 200,
        "body": f"Executed {PROJECT}_s1_subset_gbif lambda"
    }
