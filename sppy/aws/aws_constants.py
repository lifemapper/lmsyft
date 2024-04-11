"""Constants for Specnet AWS Resources."""
import copy

PROJ_NAME = "specnet"
REGION = "us-east-1"
PROJ_BUCKET = f"{PROJ_NAME}-{REGION}"
SUMMARY_FOLDER = "summary"
ENCODING = "utf-8"

LOCAL_OUTDIR = "/tmp"

class Summaries:
    TABLES = {
        "dataset_counts": {
            "fname": f"dataset_counts_XXXX_XX_XX_000.parquet",
            "table_format": "Parquet",
            "fields": ["datasetkey", "occ_count", "species_count"],
            "key_fld": "datasetkey"
        },
        "dataset_species_lists": {
            "fname": f"dataset_lists_XXXX_XX_XX_000.parquet",
            "table_format": "Parquet",
            "fields": ["datasetkey", "taxonkey", "species", "occ_count"],
            "key_fld": "datasetkey",
            "species_fld": "species",
            "combine_fields": ["taxonkey", "species"],
            "combo_species_fld": "taxonkey_species",
            "value_fld": "occ_count",
        },
        "dataset_meta": {
            "fname": f"dataset_meta_XXXX_XX_XX.parquet",
            "table_format": "Parquet",
            "fields": [
                "dataset_key", "publishing_organization_key", "title"],
            "key_fld": "dataset_key"
        }
    }

    # ...............................................
    @classmethod
    def update_summary_tables(cls, datestr):
        tables = {}
        # Update filename in summary tables
        for key, meta in cls.TABLES.items():
            meta_cpy = copy.deepcopy(meta)
            fname_tmpl = meta["fname"]
            meta_cpy["fname"] = fname_tmpl.replace("XXXX_XX_XX", datestr)
            tables[key] = meta_cpy
        return tables

    # ...............................................
    @classmethod
    def get_table(cls, table_type, datestr):
        # Update filename in summary tables
        try:
            cpy_table = copy.deepcopy(cls.TABLES[table_type])
        except KeyError:
            return None
        fname_tmpl = cpy_table["fname"]
        cpy_table["fname"] = fname_tmpl.replace("XXXX_XX_XX", datestr)
        return cpy_table


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
