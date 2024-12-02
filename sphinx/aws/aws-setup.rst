AWS Setup
####################

Security
**********************

Create a Security Group for the region
===========================================================

* Test this group!
* Create a security group for the instance (and all other instances in region)

  * Must be tied to the region of instance
  * inbound: SSH from campus (use VPN if elsewhere), HTTP/HTTPS from all

* or use launch-wizard-1 security group (created by some EC2 instance creation in 2023)

  * inbound rules IPv4:

    * Custom TCP 8000
    * Custom TCP 8080
    * SSH 22
    * HTTP 80
    * HTTPS 443

  * outbound rules IPv4, IPv6:

    * All traffic all ports


Create an IAM role for the EC2/S3 interaction (specnet_ec2_s3_role)
===========================================================

* Create a Role for EC2 instance access to S3

  1. First, create Policy allowing FullAccess to SpecifyNetwork bucket
     (specnet_S3bucket_FullAccess)

     * Make sure to add each S3 bucket subfolder - permissions are not recursive.

  2. Trusted entity type = AWS service, Use Case = S3.

  3. Add permissions

    * specnet_S3bucket_FullAccess

  4. Save and name role (specnet_ec2_s3_role)

Create an IAM role for the EC2/S3/Redshift interaction (specnet_ec2_s3_role)
===========================================================

* Create a Role for EC2 instance access to Redshift and S3

  1. Trusted entity type = AWS service, Use Case = Redshift - Customizable.

    * TODO: change to Redshift - Scheduler when we automate the workflow

  3. Add permissions

    * AmazonRedshiftAllCommandsFullAccess (AWS managed)
    * AmazonS3FullAccess (AWS managed)
    * specnet_S3bucket_FullAccess

  4. Save and name role (specnet_ec2_s3_role)


EC2
******

EC2 instance creation
===========================================================

Creation Settings
--------------------
* Future - create and save an AMI or template for consistent reproduction
* via Console, without launch template:

  * Ubuntu Server 24.04 LTS, SSD Volume Type (free tier eligible), Arm architecture
  * Instance type t4g.small

    * Build fails with t2.micro or t3.micro with 1gb RAM
    * t4g.small is 2gb RAM

  * Security Group: launch-wizard-1
  * 30 Gb General Purpose SSD (gp3)
  * For dev, Spot instance (in Advanced options)
  * Modify IAM role - to role created for s3 access (i.e. specnet_ec2_s3_role)
  * Use the security group created for this region (currently launch-wizard-1)
  * Assign your key pair to this instance

    * If you do not have a keypair, create one for SSH access (tied to region) on initial
      EC2 launch
    * One chance only: Download the private key (.pem file for Linux and OSX) to local
      machine
    * Set file permissions to 400

  * Launch
  * Test by SSH-ing to the instance with the Public IPv4 DNS address, with efault user
    (for ubuntu instance) `ubuntu`::

    ssh  -i .ssh/<aws_keyname>.pem  ubuntu@<ec2-xxx-xxx-xxx-xxx.compute-x.amazonaws.com>


Elastic IP
==============================================

* If needed (not re-using an existing IP), create an Elastic IP for the EC2 instance
* assign a DNS name to its FQDN in 3rd party (spcoco: GoDaddy)
* in console, assign EC2 instance to the Elastic IP

Install software on EC2
===========================================================

Baseline
------------
* update apt
* install apache for getting/managing certificates
* install certbot for Let's Encrypt certificates::

    sudo apt update
    sudo apt install apache2 certbot plocate unzip

AWS Client tools
--------------------

* Make sure awscli dependencies are satisified; currently glibc, groff, and less
* Use instructions to install the awscli package:
  https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html.
* Make sure to use the instructions with the right architecture (x86 vs Arm)
* Troubleshooting:

  * The AWS cli depends on boto3, so both must be up to date.  In my testing, awscli
    1.27.118 (with requirement botocore==1.29.118) and boto3 1.28.1, failed on
    S3 Select access.
  * I upgraded awscli (sudo apt install awscli), then upgraded boto3
    (pip install --upgrade boto3) , which installed 1.34.60.  Success

Configure programmatic access to S3
----------------------------------------

Configure AWS credentials either through

* (preferred) Using an IAM role attached to your instance if running on AWS
  infrastructure.
* Not recommended:

  * Environment variables
  * AWS CLI configuration (for command line tools),
    https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html

* Test access with local file test.txt (for S3 resources allowed in IAM role)::

  aws s3 ls s3://specnet-us-east-1
  aws s3 cp test.txt s3://specnet-us-east-1/summary/
  aws s3 rm s3://specnet-us-east-1/summary/test.txt



Allow docker containers to use host credentials
------------------------------------------------
* Extend the hop limit for getting metadata about permissions to 2
  host --> dockercontainer --> metadata
  https://specifydev.slack.com/archives/DQSAVMMHN/p1717706137817839

* SSH to the ec2 instance, then run ::

    aws ec2 modify-instance-metadata-options \
        --instance-id i-082e751b94e476987 \
        --http-put-response-hop-limit 2 \
        --http-endpoint enabled

Docker
-----------

Follow instructions at https://docs.docker.com/engine/install/ubuntu/

* Install dependencies if needed::

    sudo apt-get update
    sudo apt-get install ca-certificates curl gnupg

* Add Docker GPG key::

    sudo install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    sudo chmod a+r /etc/apt/keyrings/docker.gpg

* Set up the docker repository::

    echo \
      "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
      "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" | \
      sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

* Update apt for Docker repo, install Docker Engine, containerd, and Docker Compose::

    sudo apt-get update
    sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin


Add the Specify Network software via Github
-----------------------------------------------------

* Generate a local ssh key::

    $ ssh-keygen -t ed25519 -C "<your_email@address>"
    $ eval "$(ssh-agent -s)"
    $ ssh-add ~/.ssh/id_ed25519

* Add the ssh key to Github

  * In the Github website, login, and navigate to your profile Settings
  * Select **SSH and GPG keys** from the left vertical menu
  * Choose **New SSH key**
  * In a terminal window, copy the key to the clipboard::

    $ cat ~/.ssh/id_ed25519.pub

* In the resulting text window, add your public key, and tie with your EC2 instance
  with a memorable name

* Clone the repository to the EC2 instance::

    git clone git@github.com:specifysystems/sp_network




Redshift
***********************************

Overview
=================

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
* Role/Permissions: Attach the Role AmazonRedshift-CommandsAccessRole-20231129T105842
  to the Redshift Namespace


Create (or use existing) Role
==================================

* Need Glue permissions


Create a new workgroup (and namespace)
=============================================
* In the Redshift dashboard, choose the button **Create workspace** to create a new
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
=============================================

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


Configure S3/Redshift for data acquisition and analyses
=====================================================================

* Create a bucket to hold relevant data
* Create output folders for tables to be written from rs_summarize_data.sql
* Make sure that Redshift namespace/workgroup has permission to write to the S3 bucket



Local client
***************************************

Configuration
========================

* Copy SSH private key to each machine used for AWS access
* Extend the SSH timeout in local ssh client config file ~/.ssh/config::

    Host *
        ServerAliveInterval 20


* then login with private key::

    ssh -i ~/.ssh/<your_aws_key>.pem ubuntu@xxx.xxx.xx.xx


Connect and set EC2 SSH service timeout
===========================================

* Extend the SSH timeout (in AMI or instance?) in new config file (<proj_name>.conf)
  under ssh config dir (/etc/ssh/sshd_config.d)::

    ClientAliveInterval 1200
    ClientAliveCountMax 3

* Reload SSH with new configuration::

    $ sudo systemctl reload sshd

Enable S3 access from local machine
===========================================================

* Configure AWS credentials and defaults

  * Using aws_cli::

    -- written to ~/.aws/config
    aws configure set default.region region;
    aws configure set default.output json;

    -- Configure AWS; written to ~/.aws/credentials
    aws configure set aws_access_key_id "";
    aws configure set aws_secret_access_key "";

 * or setting environment variables in ~/.bashrc::

    # AWS credentials and defaults
    export AWS_DEFAULT_REGION=region
    export AWS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
    export AWS_ACCESS_KEY_ID=xxx
    export AWS_SECRET_ACCESS_KEY=xxx

* Test access locally with::

    $ aws s3 ls s3://specnet-us-east-1
    $ aws ec2 describe-instances


Troubleshooting
***************************************

Error: SSL
==================
First time:

Error message ::

    SSL validation failed for https://ec2.us-east-1.amazonaws.com/
    [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer
    certificate (_ssl.c:1002)

Test with::

    $ aws s3 ls --no-verify-ssl
    $ aws ec2 describe-instances --no-verify-ssl

Fix: Set up to work with Secret containing security key

Second time (in python code):
>>> response = requests.get(url)
Traceback (most recent call last):
  File "/home/astewart/git/sp_network/venv/lib/python3.8/site-packages/urllib3/connectionpool.py", line 703, in urlopen
    httplib_response = self._make_request(
  File "/home/astewart/git/sp_network/venv/lib/python3.8/site-packages/urllib3/connectionpool.py", line 386, in _make_request
    self._validate_conn(conn)
  File "/home/astewart/git/sp_network/venv/lib/python3.8/site-packages/urllib3/connectionpool.py", line 1042, in _validate_conn
    conn.connect()
  File "/home/astewart/git/sp_network/venv/lib/python3.8/site-packages/urllib3/connection.py", line 419, in connect
    self.sock = ssl_wrap_socket(
  File "/home/astewart/git/sp_network/venv/lib/python3.8/site-packages/urllib3/util/ssl_.py", line 449, in ssl_wrap_socket
    ssl_sock = _ssl_wrap_socket_impl(
  File "/home/astewart/git/sp_network/venv/lib/python3.8/site-packages/urllib3/util/ssl_.py", line 493, in _ssl_wrap_socket_impl
    return ssl_context.wrap_socket(sock, server_hostname=server_hostname)
  File "/usr/lib/python3.8/ssl.py", line 500, in wrap_socket
    return self.sslsocket_class._create(
  File "/usr/lib/python3.8/ssl.py", line 1069, in _create
    self.do_handshake()
  File "/usr/lib/python3.8/ssl.py", line 1338, in do_handshake
    self._sslobj.do_handshake()
ssl.SSLCertVerificationError: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:1131)


https://stackoverflow.com/questions/51925384/unable-to-get-local-issuer-certificate-when-using-requests

pip install certifi

import certifi
certifi.where()

Error accessing AWS console and/or CLI
===========================================================
You need permissions

Signature not yet current: 20240624T205810Z is still later than 20240624T205725Z (20240624T205225Z + 5 min.)

Solution:
-----------------
Make sure that the local time is correct and is syncing regularly from time.ku.edu.

* Check systemd_timesyncd.service::

    $ sudo systemctl status systemd-timesyncd
    ● systemd-timesyncd.service - Network Time Synchronization
         Loaded: loaded (/lib/systemd/system/systemd-timesyncd.service; enabled; vendor preset: enabled)
         Active: active (running) since Mon 2024-05-13 11:22:01 CDT; 1 months 12 days ago
           Docs: man:systemd-timesyncd.service(8)
       Main PID: 1049 (systemd-timesyn)
         Status: "Idle."
          Tasks: 2 (limit: 154130)
         Memory: 1.4M
         CGroup: /system.slice/systemd-timesyncd.service
                 └─1049 /lib/systemd/systemd-timesyncd

    Jun 25 13:01:19 murderbot systemd-timesyncd[1049]: Timed out waiting for reply from 185.125.190.57:123 (ntp.ubuntu.com).
    Jun 25 13:01:30 murderbot systemd-timesyncd[1049]: Timed out waiting for reply from 91.189.91.157:123 (ntp.ubuntu.com).
    Jun 25 13:35:48 murderbot systemd-timesyncd[1049]: Timed out waiting for reply from 185.125.190.57:123 (ntp.ubuntu.com).
    Jun 25 13:35:58 murderbot systemd-timesyncd[1049]: Timed out waiting for reply from 185.125.190.56:123 (ntp.ubuntu.com).
    Jun 25 13:36:09 murderbot systemd-timesyncd[1049]: Timed out waiting for reply from 185.125.190.58:123 (ntp.ubuntu.com).
    Jun 25 13:36:19 murderbot systemd-timesyncd[1049]: Timed out waiting for reply from 91.189.91.157:123 (ntp.ubuntu.com).
    Jun 25 14:10:37 murderbot systemd-timesyncd[1049]: Timed out waiting for reply from 91.189.91.157:123 (ntp.ubuntu.com).
    Jun 25 14:10:48 murderbot systemd-timesyncd[1049]: Timed out waiting for reply from 185.125.190.57:123 (ntp.ubuntu.com).
    Jun 25 14:10:58 murderbot systemd-timesyncd[1049]: Timed out waiting for reply from 185.125.190.58:123 (ntp.ubuntu.com).
    Jun 25 14:11:08 murderbot systemd-timesyncd[1049]: Timed out waiting for reply from 185.125.190.56:123 (ntp.ubuntu.com).

* Update the reference server in /etc/systemd/timesyncd.conf to point to time.ku.edu.
  Change the NTP value, and leave others as defaults, uncomment if necessary.::

    [Time]
    NTP=time.ku.edu
    FallbackNTP=ntp.ubuntu.com
    RootDistanceMaxSec=5
    PollIntervalMinSec=32
    PollIntervalMaxSec=2048

* Restart systemd_timesyncd.service::

    $ sudo systemctl restart systemd-timesyncd


Workflow for Specify Network Analyst pre-computations
===========================================================

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
