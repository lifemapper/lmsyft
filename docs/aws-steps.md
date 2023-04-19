# Create a Security Group for the region

* Create a security group for the instance (and all other instances in region)
  * Must be tied to the region of instance
  * aimee.stewart_SG_useast2
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

## Connect and set SSH service timeout

```commandline
ssh -i ~/.ssh/aws_rsa.pem ubuntu@xxx.xxx.xx.xx
```

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

# Install base software

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

# Set up client machines 



## SSH

* Copy SSH private key to each machine used for AWS access
* Extend the SSH timeout vim ~/.ssh/config

```
Host *
    ServerAliveInterval 20
```

# Later: Create Amazon Machine Image (AMI) 

* template of common software configuration

