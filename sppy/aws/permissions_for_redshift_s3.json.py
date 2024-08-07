"""AWS policies needed for AWS workflow in JSON format."""
# AmazonRedshift-CommandsAccessPolicy-20231129T105842 Customer Managed Policy
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "s3:GetObject",
                "s3:GetBucketAcl",
                "s3:GetBucketCors",
                "s3:GetEncryptionConfiguration",
                "s3:GetBucketLocation",
                "s3:ListBucket",
                "s3:ListAllMyBuckets",
                "s3:ListMultipartUploadParts",
                "s3:ListBucketMultipartUploads",
                "s3:PutObject",
                "s3:PutBucketAcl",
                "s3:PutBucketCors",
                "s3:DeleteObject",
                "s3:AbortMultipartUpload",
                "s3:CreateBucket"
            ],
            "Effect": "Allow",
            "Resource": [
                "arn:aws:s3:::bison-321942852011-us-east-1/*",
                "arn:aws:s3:::bison-321942852011-us-east-1",
                "arn:aws:s3:::specnet-us-east-1/*",
                "arn:aws:s3:::specnet-us-east-1"
            ]
        }
    ]
}
# aimee-glue-some Customer Inline Policy
# For Redshift data creation/manipulation with write to S3.
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Action": [
				"s3:GetBucketLocation",
				"s3:GetObject",
				"s3:ListMultipartUploadParts",
				"s3:ListBucket",
				"s3:ListBucketMultipartUploads"
			],
			"Resource": [
				"*"
			]
		},
		{
			"Effect": "Allow",
			"Action": [
				"glue:CreateDatabase",
				"glue:DeleteDatabase",
				"glue:GetDatabase",
				"glue:GetDatabases",
				"glue:UpdateDatabase",
				"glue:CreateTable",
				"glue:DeleteTable",
				"glue:BatchDeleteTable",
				"glue:UpdateTable",
				"glue:GetTable",
				"glue:GetTables",
				"glue:BatchCreatePartition",
				"glue:CreatePartition",
				"glue:DeletePartition",
				"glue:BatchDeletePartition",
				"glue:UpdatePartition",
				"glue:GetPartition",
				"glue:GetPartitions",
				"glue:BatchGetPartition"
			],
			"Resource": [
				"*"
			]
		}
	]
}