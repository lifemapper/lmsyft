"""Constants for Specnet AWS Resources."""
PROJ_NAME = "specnet"
REGION = "us-east-1"
PROJ_BUCKET = f"{PROJ_NAME}-{REGION}"
SUMMARY_FOLDER = "summary"
ENCODING = "utf-8"

INPUT_PATH = "summary"
LOG_PATH = "log"

GBIF_BUCKET = "gbif-open-data-us-east-1/occurrence"
GBIF_ARN = "arn:aws:s3:::gbif-open-data-us-east-1"
GBIF_ODR_FNAME = "occurrence.parquet"

SPOT_TEMPLATE_BASENAME = "launch_template"

KEY_NAME = "aimee-aws-key"
# Allows KU Dyche hall
SECURITY_GROUP_ID = ""
SECRET_NAME = ""

# S3
TRIGGER_PATH = "trigger"
TRIGGER_FILENAME = "go.txt"

# EC2 Spot Instance
# List of instance types at https://aws.amazon.com/ec2/spot/pricing/
INSTANCE_TYPE = "t2.micro"
# INSTANCE_TYPE = "a1.large"

# Log processing progress
LOGINTERVAL = 1000000
LOG_FORMAT = " ".join(["%(asctime)s", "%(levelname)-8s", "%(message)s"])
LOG_DATE_FORMAT = "%d %b %Y %H:%M"
LOGFILE_MAX_BYTES = 52000000
LOGFILE_BACKUP_COUNT = 5

USER_DATA_TOKEN = "###SCRIPT_GOES_HERE###"
