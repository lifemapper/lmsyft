#!/bin/bash
echo "Starting script..."
yum update -y
cd /root
echo "retrieving data..."
wget https://api.gbif.org/v1/occurrence/download/request/0042658-230530130749713.zip
unzip 0042658-230530130749713.zip occurrence.txt
aws s3 cp occurrence.txt s3://${DataIngestionBucket}/specifydata/
echo "Copied to S3!"
shutdown now
