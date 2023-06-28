# S3 Athena

* on us-east-1
* Find bucket, specify_athena

gbif_extract: 303237553
gbif_parquet_extract: 301669806

Use python libs **awscli** and **boto3** to connect with AWS

# Setup

* install aws-cli


# Workflow

* download GBIF data (~350 GB)
  * locally
  * directly to EC2 instance using wget or script
* upload to S3
  * put-object with AWS CLI v2
    https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3api/put-object.html
  * AWS Python SDK put_object using Boto3
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html#