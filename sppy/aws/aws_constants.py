"""Constants for Specnet AWS Resources."""
PROJ_NAME = "specnet"
REGION = "us-east-1"
PROJ_BUCKET = f"{PROJ_NAME}-{REGION}"
SUMMARY_FOLDER = "summary"
ENCODING = "utf-8"

class SummaryTables:
    SUMMARY_TABLES = {
        "dataset_counts": {
            "fname": f"dataset_counts_XXXX_XX_XX_000.parquet",
            "table_format": "Parquet",
            "fields": ["datasetkey", "occ_count", "species_count"],
            "key": "datasetkey"
        },
        "dataset_species_lists": {
            "fname": f"dataset_lists_XXXX_XX_XX_000.parquet",
            "table_format": "Parquet",
            "fields": ["datasetkey", "taxonkey", "species", "occ_count"],
            "key": "datasetkey"
        },
        "dataset_meta": {
            "fname": f"dataset_meta_XXXX_XX_XX.parquet",
            "table_format": "Parquet",
            "fields": [
                "dataset_key", "publishing_organization_key", "title"],
            "key": "dataset_key"
        }
    }

    @classmethod
    def update_summary_tables(cls, datestr):
        tables = {}
        # Update filename in summary tables
        for key, meta in cls.SUMMARY_TABLES.items():
            tbl_meta = {}
            for subkey, val in meta:
                if subkey == "fname":
                    tbl_meta["fname"] = val.replace("XXXX_XX_XX", datestr)
                else:
                    tbl_meta[subkey] = val
            tables[key] = tbl_meta
        return tables


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
