# Create Amazon Machine Image (AMI) 

* template of common software configuration

## Setup default software installs

* update apt
* install AWS client, awscli
* install apache for getting/managing certificates
* install certbot for Let's Encrypt certificates

```commandline
$ sudo apt update
$ sudo apt install awscli
$ sudo apt install apache2
$ sudo apt install certbot
```

# Security Group

* Create a security group for the instance (and all other instances in region)
  * Must be tied to the region of instance
  * aimee.stewart_SG_useast2
  * inbound: SSH from campus, HTTP/HTTPS from all

# Create AWS Elastic Compute Cloud (EC2) instance

* Create from AMI (or not for new config)
* Use the security group created for this region
* Default user for ubuntu instance is `ubuntu`

# SSH 

## AWS access: keypair

* Create a keypair for SSH access (tied to region)
* One chance only: Download the private key (.pem file for Linux and OSX) to local machine
* Set file permissions to 600

## SSH service

* Extend the SSH timeout (in AMI or instance?) by editing config file:

```commandline
$ sudo vim /etc/ssh/sshd_config
```

```text
ClientAliveInterval 1200
ClientAliveCountMax 3
```

* Reload SSH with new configuration 

```commandline
$ sudo systemctl reload sshd
```

# Set up static IP

* Elastic IP address (broker-dev.spcoco.org == 52.15.115.249)
* Register FQDN (spcoco.org in GoDaddy) to IP for public access



# Set up client machines 

## SSH

* Copy SSH private key to each machine used for AWS access
* Extend the SSH timeout vim ~/.ssh/config

```
Host *
    ServerAliveInterval 20
```