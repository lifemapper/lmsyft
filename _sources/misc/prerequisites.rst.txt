Data and Infrastructure Prerequisites
#########################################

Dataset metadata for titles from GUIDs
---------------------------------------------
Download dataset metadata from GBIF to allow adding Dataset titles (and perhaps
Organization metadata) to Analyst outputs.

Download from https://www.gbif.org/dataset/search?type=OCCURRENCE in TSV format and
run `create_s3_dataset_lookup_from_tsv` in the sppy/aws/aws_tools.py module.

This should be done on the same schedule as data refresh - the first of every month.

Create an EC2 instance
---------------------------------------------

Info at `AWS Setup EC2 <..aws/aws-setup#EC2>`_
