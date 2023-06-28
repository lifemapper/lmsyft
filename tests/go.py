import sys
import boto3

from pyspark.context import SparkContext
from pyspark.sql import SparkSession

fname = "/mnt/sata8/tank/testgbif/0146234-230224095556074/occurrence.txt"

# spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
# gbif_data = spark.read.csv(fname).cache()
#
# numAs = gbif_data
#
# # Set your source database and table name
# source_database = "gbif"
# source_table = "gbif_extract"

# Set your output S3 path
output_s3_path = "s3://specify-network-dev/gbif_test/gbif_dwc_extract/"

# Set your Glue region
glue_region = "us-east-1"


spark.stop()