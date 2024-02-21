=============================================
AWS strategy
=============================================

Data
---------------------------
* on us-east-1
* Bucket bison-321942852011-us-east-1

gbif_extract: 303237553
gbif_parquet_extract: 301669806

Use python libs **awscli** and **boto3** to connect with AWS

* query (Occurrences only):

  https://www.gbif.org/occurrence/search?basis_of_record=OCCURRENCE&country=US&has_coordinate=true&has_geospatial_issue=false&occurrence_status=present

Test data
...............
* DwCA
* Citation: GBIF.org (09 October 2023) GBIF Occurrence Download  https://doi.org/10.15468/dl.4vqtdy
* 1,421,222 records
* download:


Setup
---------------------------
* install aws-cli

Workflow
---------------------------

* download GBIF data (~350 GB)

  * directly to EC2 instance using wget or script

* upload to S3

  * put-object with AWS CLI v2
    https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3api/put-object.html
  * AWS Python SDK put_object using Boto3
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html#

* pyspark
