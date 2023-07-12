# Create a Security Group for the region

* Create a security group for the instance (and all other instances in region)
  * Must be tied to the region of instance
  * aimee.stewart_SG_useast1
  * inbound: SSH from campus, HTTP/HTTPS from all

# Create AWS Elastic Compute Cloud (EC2) instance

* Create from AMI (or not for new config)
* Use the security group created for this region
* Default user for ubuntu instance is `ubuntu`
* (opt) Request an Elastic IP and assign DNS to it
  * Register FQDN (GoDaddy) to IP for public access

# Enable SSH access to EC2 

## AWS access: keypair

* Create a keypair for SSH access (tied to region) on EC2 launch
* One chance only: Download the private key (.pem file for Linux and OSX) to local machine
* Set file permissions to 400


## Set up local/client 

* Copy SSH private key to each machine used for AWS access
* Extend the SSH timeout vim ~/.ssh/config

```
Host *
    ServerAliveInterval 20
```

```commandline
ssh -i ~/.ssh/aws_rsa.pem ubuntu@xxx.xxx.xx.xx
```

## Connect and set EC2 SSH service timeout

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

# Install software on EC2

## Base software 

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

## Docker

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

# Add the Github repo to EC2 instance

## Generate a local ssh key 

```commandline
$ ssh-keygen -t ed25519 -C "aimee.stewart@ku.edu"
$ eval "$(ssh-agent -s)"
$ ssh-add ~/.ssh/id_ed25519
```

## Add the ssh key to Github

* In the Github website, login, and navigate to your user profile
* Select **SSH and GPG keys** from the left vertical menu
* Choose **New SSH key**
* In a terminal window, copy the key to the clipboard

```commandline
$ cat ~/.ssh/id_ed25519.pub
```
* In the resulting text window, add your public key, and tie with your EC2 instance 
  with a memorable name 

## Clone the repository to the EC2 instance

```commandline
git clone git@github.com:specifysystems/sp_network 
```

# Enable S3 access from local machine and EC2

## Configure AWS credentials and defaults

### Using aws_cli
```commandline
# written to ~/.aws/config
aws configure set default.region us-east-1;
aws configure set default.output json;
# Configure AWS; written to ~/.aws/credentials
aws configure set aws_access_key_id "";
aws configure set aws_secret_access_key "";

```

### or setting environment variables in ~/.bashrc
```commandline
# AWS credentials and defaults
export AWS_DEFAULT_REGION=us-east-1
export AWS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
export AWS_ACCESS_KEY_ID=xxx
export AWS_SECRET_ACCESS_KEY=xxx

```
# Test access locally with 

```commandline
$ aws s3 ls
$ aws ec2 describe-instances
```

## Error: SSL
```
SSL validation failed for https://ec2.us-east-1.amazonaws.com/ 
[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer 
certificate (_ssl.c:1002)
```

```commandline
$ aws s3 ls --no-verify-ssl
$ aws ec2 describe-instances --no-verify-ssl
```

# Workflow for Specify Network Analyst pre-computations

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

