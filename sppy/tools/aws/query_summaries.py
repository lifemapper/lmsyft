"""Script to run locally or from an EC2 instance to query stats from S3 datafiles."""
from sppy.tools.aws.aws_constants import (
    BUCKET, INPUT_PATH)
from sppy.tools.aws.aws_tools import (
    create_dataframe_from_s3obj, get_current_datadate_str, get_today_str)

LOCAL_OUTDIR = "/tmp"
# underscores for Redshift data
datestr = get_current_datadate_str()
# date for logfile
todaystr = get_today_str()

count_fname = f"dataset_counts_{datestr}_000.parquet"
list_fname = f"dataset_lists_{datestr}_000.parquet"





# ----------------------------------------------------
# Main
# ----------------------------------------------------
df = create_dataframe_from_s3obj(BUCKET, f"{INPUT_PATH}/{list_fname}")