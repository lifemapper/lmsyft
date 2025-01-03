Create lambda function to initiate processing
--------------------------------------------

Lambda functions can execute tasks in a workflow.  They can be initiated manually, or
automatically with the Amazon EventBridge by either a trigger condition (through Rules)
or on a timetable (through Schedules).

Lambda functions can only execute for up to 15 minutes at a time, but can initiate
longer running processes in another service, such as launch an EC2 instance to run a
workflow task.

Short tasks that require specific or in-development code, may be run in a Docker
container on an EC2 instance.  The Docker image can be built with code pulled from a
repository or the pre-built image pulled from a Docker Hub.

Lambda functions log directly to AWS Cloudwatch - make sure the role executing
the lambda function has Create/Write permissions to AWS Cloudwatch.  All print
statements will go to the log.  More information is in the

The Specify Network workflow has one step that runs on an EC2 instance.  It launches an
EC2 instance from a template which contains a startup script in the `userdata` for the
EC2 instance.  The startup script builds a Docker image from the latest code in the
BISON Github repository, then executes a task defined in that code.  The step is
the last of the workflow: **calc_stats**, in the specnet/workflow directory.

Edit the Configuration for lambda function
--------------------------------------------

* General: Timeout to an appropriate value for this task.
* Permissions: Execution role to the Workflow role (specnet_workflow_role)
* Asynchronous Invokation: Set Maximum age of event and Retry attempts.
* By default, Lambda functions produce a Cloudwatch Logs stream.

Lambda to query Redshift
--------------------------------------------

https://repost.aws/knowledge-center/redshift-lambda-function-queries

https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift-data/client/execute_statement.html

* Connect to a serverless workgroup (bison), namespace (bison), database name (dev)

* When connecting to a serverless workgroup, specify the workgroup name and database
  name. The database user name is derived from the IAM identity. For example,
  arn:iam::123456789012:user:foo has the database user name IAM:foo. Also, permission
  to call the redshift-serverless:GetCredentials operation is required.
* need redshift:GetClusterCredentialsWithIAM permission for temporary authentication
  with a role

Lambda to start EC2 for task
--------------------------------------------

Lambda functions must be single-function tasks that run in less than 15 minutes.
For complex or long-running tasks we start an EC2 instance containing bison code
and execute it in a docker container.

For each task, the lambda function should create a Spot EC2 instance with a template
containing userdata that will either 1) pull the Github repo, then build the docker
image, or 2) pull a docker image directly.

Creating a heatmap, PAM, and calculating statistics, does not take long, but uses
multiple project modules and requires a larger EC2 instance (t4g.small) than less
memory-intensive functions.
