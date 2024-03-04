Authentication
####################

For programmatic access to S3
*******************************
Configure AWS credentials either through
    * environment variables
    * AWS CLI configuration (for command line tools), or
    * using an IAM role attached to your instance if running on AWS infrastructure.


Redshift
###############################

Overview
*******************************

* Redshift allows you to work with very large datasets in parallel very quickly.
* Redshift acts as a database application, and can connect to databases created in
  Redshift, Glue Data Catalogs, and mount tabular data in S3
* The default new database is "dev", and it contains the "public" schema. The
  schema contains Tables, Views, Functions, and Stored Procedures.
* To mount S3 data, you must create an external schema in the database, and define
  the new data, including all of its fields and its S3 location.  These functions are
  included in the script rs_subset_gbif.sql
* After mounting a table, you can filter the data into a new table in your public
  schema, then drop the table in the external schema (the original S3 data).
* We currently filter out data with missing latitude or longitude, taxonomic ranks above
  species level, and records with a basis of record that is not observation, occurrence,
  or preserved specimen.  This brings the full dataset from about 2.6 billion down to
  2.3 billion.

Create an IAM role for the Redshift/S3 interaction
*******************************

* Create a Role (Redshift-S3) for service Redshift to read/write to S3

    * Add a policy allowing read and write access to the specnet S3 bucket
    * Step 1: Trusted entity type = AWS service, Use Case = Redshift - Customizable.
        * TODO: change to Redshift - Scheduler when we automate the workflow
    * Step 2: Add permissions
        * AmazonRedshiftAllCommandsFullAccess (AWS managed)
        * AmazonS3FullAccess (AWS managed)


Create a new workgroup (and namespace)
*******************************

* In the Redshift dashbord, choose the button **Create workspace** to create a new
  workgroup and namespace.  The resulting form shows 3 steps.

    * Step 1, define the Workgroup name, Capacity, and Network and Security.
      Choose a name, i.e. **specnet**, and keep the defaults for the Capacity, VPC, and
      Subnets
    * Step 2, set up a namespace.  Create a new one, i.e. **specnet** (we are using
      the same name for the worksgroup and namespace).  Retain the first database name
      (dev) and leave the Admin user credentials as the default (unchecked Customize
      box).  Check the the default Associated IAM role or create a new role.
      Leave Encryption and security settings unchanged.
        * Make sure that the Associated IAM role has permission to access the bucket
          you will write to (use Redshift-S3 created above)
        * Make new Redshift-S3 Role the default for Redshift operations in this
          namespace
    * Step 3, review and create workspace.  This will take some time.

Connect to new namespace in Query Editor
*******************************

* Choose **Query editor v2** in the Redshift dashboard left-side menu
* Choose the new workgroup "Serverless: specnet" in the resource list

    * From the resulting dialog, choose "Other ways to connect" and "Federated user"
      then click the button "Create connection"
    * The connection will become active, and the new "dev" database will
      appear, as well as any other data catalogs your user account has access to.
    * In the top of the right pane, click the + sign to open a new tab for writing
      and executing commands.
    * Paste in the contents of rs_create_sps_functions.sql to create functions and
      stored procedures to be used in this workspace.


Configure for data acquisition and analyses
*******************************

* Create a bucket to hold relevant data
* Create output folders for tables to be written from rs_summarize_data.sql
* Make sure that Redshift namespace/workgroup has permission to write to the S3 bucket

    *


Create a Security Group for the region
###############################

* Create a security group for the instance (and all other instances in region)
  * Must be tied to the region of instance
  * aimee.stewart_SG_useast1
  * inbound: SSH from campus, HTTP/HTTPS from all


Create AWS Elastic Compute Cloud (EC2) instance
###############################
* Create from AMI (or not for new config)
* Use the security group created for this region
* Default user for ubuntu instance is `ubuntu`
* (opt) Request an Elastic IP and assign DNS to it
  * Register FQDN (GoDaddy) to IP for public access

Enable SSH access to EC2
###############################

AWS access: keypair
***************************************

* Create a keypair for SSH access (tied to region) on EC2 launch
* One chance only: Download the private key (.pem file for Linux and OSX) to local machine
* Set file permissions to 400


Set up local/client
***************************************

* Copy SSH private key to each machine used for AWS access
* Extend the SSH timeout vim ~/.ssh/config

    ```
    Host *
        ServerAliveInterval 20
    ```

    ```commandline
    ssh -i ~/.ssh/aws_rsa.pem ubuntu@xxx.xxx.xx.xx
    ```

Connect and set EC2 SSH service timeout
***************************************

* Extend the SSH timeout (in AMI or instance?) in new config file under ssh config dir:

```commandline
$ sudo vim /etc/ssh/sshd_config.d/sp_network.conf
```

    ```text
    ClientAliveInterval 1200
    ClientAliveCountMax 3
    ```

* Reload SSH with new configuration

    ```commandline
    $ sudo systemctl reload sshd
    ```

Install software on EC2
###############################

Base software
***************************************

* update apt
* install AWS client, awscli
* install apache for getting/managing certificates
* install certbot for Let's Encrypt certificates

    ```commandline
    $ sudo apt update
    $ sudo apt install awscli
    $ sudo apt install apache2
    $ sudo apt install certbot
    $ sudo apt install plocate
    ```

Docker
***************************************

Follow instructions at https://docs.docker.com/engine/install/ubuntu/

* Set up the repository:

    ```commandline
    $ sudo apt-get update
    $ sudo apt-get install ca-certificates curl gnupg
    ```

* Add Docker GPG key

    ```commandline
    $ sudo install -m 0755 -d /etc/apt/keyrings
    $ curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    $ sudo chmod a+r /etc/apt/keyrings/docker.gpg
    ```

* Set up the docker repository

    ```commandline
    $ echo \
      "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
      "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" | \
      sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    ```

* Update apt and install Docker Engine, containerd, and Docker Compose.

    ```commandline
    $ sudo apt-get update
    $ sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
    ```

Add the Github repo to EC2 instance
###########################################

Generate a local ssh key
***************************************

    ```commandline
    $ ssh-keygen -t ed25519 -C "<your_email@address>"
    $ eval "$(ssh-agent -s)"
    $ ssh-add ~/.ssh/id_ed25519
    ```

Add the ssh key to Github
***************************************


* In the Github website, login, and navigate to your user profile
* Select **SSH and GPG keys** from the left vertical menu
* Choose **New SSH key**
* In a terminal window, copy the key to the clipboard

    ```commandline
    $ cat ~/.ssh/id_ed25519.pub
    ```

* In the resulting text window, add your public key, and tie with your EC2 instance
  with a memorable name

Clone the repository to the EC2 instance
***************************************

    ```commandline
    git clone git@github.com:specifysystems/sp_network
    ```

Enable S3 access from local machine and EC2
###############################

Configure AWS credentials and defaults
***************************************

Using aws_cli
=====================

    ```commandline
    -- written to ~/.aws/config
    aws configure set default.region us-east-1;
    aws configure set default.output json;

    -- Configure AWS; written to ~/.aws/credentials
    aws configure set aws_access_key_id "";
    aws configure set aws_secret_access_key "";

    ```

or setting environment variables in ~/.bashrc
=====================

    ```commandline
    # AWS credentials and defaults
    export AWS_DEFAULT_REGION=region
    export AWS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
    export AWS_ACCESS_KEY_ID=xxx
    export AWS_SECRET_ACCESS_KEY=xxx

    ```

Test access locally with
###############################

    ```commandline
    $ aws s3 ls
    $ aws ec2 describe-instances
    ```

Error: SSL
***************************************

```
SSL validation failed for https://ec2.us-east-1.amazonaws.com/
[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer
certificate (_ssl.c:1002)

    ```commandline
    $ aws s3 ls --no-verify-ssl
    $ aws ec2 describe-instances --no-verify-ssl
    ```

* Set up to work with Secret containing security key


Workflow for Specify Network Analyst pre-computations
###############################

* Read https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/plan-spot-fleet.html
* work with: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/work-with-spot-fleets.html
* create request (console): https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/work-with-spot-fleets.html#create-spot-fleet
* Local
  * Create an EC2 instance launch template
  * Create a Spot EC2 instance
    * with create_fleet, prerequisites:
      https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/manage-ec2-fleet.html#ec2-fleet-prerequisites
    * send "UserData" with scripts on instantiation
* On new Spot EC2 instance
  * UserData Script will run on startup
    * Download from GBIF
    * Trim data and save as parquet format on Spot instance
    * Upload data to S3, delete on Spot

* template of common software configuration
