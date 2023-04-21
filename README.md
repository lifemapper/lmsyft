# Welcome to the Specify Network Broker and Analyst services!

The Specify Network consists of tools and services to serve the Specify Collections
Consortium.  There are currently several elements in production, and some still in
development.

This work has been supported by NSF Awards NSF BIO-1458422, OCI-1234983.

## Specify Network Broker
The Specify Broker searches for information related to an occurrence object
information from other data services, such as GBIF, iDigBio, OpenTreeOfLife, ITIS,
WoRMS, and more.  It presents the related digital object elements in a frontend,
primarily accessed through the Specify application.

The Specify Broker houses objects and common tools used within a Broker installation
that may also be useful for outside contributors and the community as a whole.

Any community contributed tool through the
[sp_network repository](https://github.com/specifysystems/sp_network/) should
use these objects to ensure that new contributions are compatible with the
Lifemapper backend.

## Specify Network Analyst (in development)

The Specify Network Analyst is in development, and will be a collection of specimen-based analytics
assessing the composition of collection holdings and available species information.

These data are used to compare and assess collections against and among the collective
holdings globally distributed data.  The analytics are then returned to the
contributing institutions and others to assist those collections in prioritizing
collecting and digitization efforts, institutional loans, mergers, deaccessions, and
more, to improve, the overall quality of the collection.  This information can also be
used by the community as a whole to identify gaps in species knowlege or redundancies.

The Analyst presents this information in multivariate-, but subsettable, space
to provide as much value and feedback to the community as possible.

# Specify Network Deployment 


## SSL

### Local self-signed certificates

To run the containers, generate `fullchain.pem` and `privkey.pem` (certificate
and the private key) using Let's Encrypt and link these files in `./sp_network/config/`.

While in development, you can generate self-signed certificates then link them in
~/git/sp_network/config/ directory for this project:

```zsh
$ mkdir ~/certificates

openssl req \
  -x509 -sha256 -nodes -newkey rsa:2048 -days 365 \
  -keyout ~/certificates/privkey.pem \
  -out ~/certificates/fullchain.pem

$ cd ~/git/sp_network/config
$ ln -s ~/certificates/privkey.pem
$ ln -s ~/certificates/fullchain.pem
```

To run either the production or the development containers with HTTPS
support, generate `fullchain.pem` and `privkey.pem` (certificate and the private
key) using Let's Encrypt, link these files in the `./config/` directory.
Full instructions in the docs/aws-steps.rst page, under `Set up TLS/SSL` 

Modify the `FQDN` environment variable in `.env.conf` as needed.

###  TLS/SSL using Certificate Authority (CA)

* Make sure that DNS has propogated for domain for SSL
* Stop apache service 
* request a certificate for the domain

```commandline
ubuntu@ip-172-31-22-32:~$ sudo systemctl stop apache2
ubuntu@ip-172-31-22-32:~$ sudo certbot certonly
Saving debug log to /var/log/letsencrypt/letsencrypt.log

How would you like to authenticate with the ACME CA?
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
1: Spin up a temporary webserver (standalone)
2: Place files in webroot directory (webroot)
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Select the appropriate number [1-2] then [enter] (press 'c' to cancel): 1
Enter email address (used for urgent renewal and security notices)
 (Enter 'c' to cancel): aimee.stewart@ku.edu

- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Please read the Terms of Service at
https://letsencrypt.org/documents/LE-SA-v1.3-September-21-2022.pdf. You must
agree in order to register with the ACME server. Do you agree?
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
(Y)es/(N)o: Y

- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Would you be willing, once your first certificate is successfully issued, to
share your email address with the Electronic Frontier Foundation, a founding
partner of the Let's Encrypt project and the non-profit organization that
develops Certbot? We'd like to send you email about our work encrypting the web,
EFF news, campaigns, and ways to support digital freedom.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
(Y)es/(N)o: N
Account registered.
Please enter the domain name(s) you would like on your certificate (comma and/or
space separated) (Enter 'c' to cancel): broker-dev.spcoco.org
Requesting a certificate for broker-dev.spcoco.org

Successfully received certificate.
Certificate is saved at: /etc/letsencrypt/live/broker-dev.spcoco.org/fullchain.pem
Key is saved at:         /etc/letsencrypt/live/broker-dev.spcoco.org/privkey.pem
This certificate expires on 2023-07-16.
These files will be updated when the certificate renews.
Certbot has set up a scheduled task to automatically renew this certificate in the background.

- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
If you like Certbot, please consider supporting our work by:
 * Donating to ISRG / Let's Encrypt:   https://letsencrypt.org/donate
 * Donating to EFF:                    https://eff.org/donate-le
```

* copy the newly created directory with certificates to the home directory
* change the owner so that they can be used in Docker containers

```commandline
$ cd
$ mkdir certificates
$ sudo cp -rp /etc/letsencrypt/archive/broker-dev.spcoco.org/*  ~/certificates
$ sudo chown ubuntu:ubuntu ~/certificates/*
```

### Renew Certbot SSL certificates

SSL certificates are served from the base VM, and need apache to be renewed.
These are administered by Letsencrypt using Certbot and are only valid for 90 days at
a time. When it is time for a renewal (approx every 60 days), bring the docker
containers down, and start apache. Renew the certificates, then stop apache,
and bring the containers up again.

```zsh
certbot certificates
docker compose stop
systemctl start httpd
certbot renew
systemctl stop httpd
docker compose up -d
```

### SSL through Amazon?

* Create Elastic IP address for EC2 instance
* Request a public certificate through Certificate Manager (ACM)
  * Choose DNS validation
  * Add tags sp_network, dev or prod, others


## Install 

### Install dependencies

Certbot: 

```commandline
$ sudo apt update
$ sudo apt install certbot
```

### Install Docker

Add docker repository, then use apt to install Docker: 
https://docs.docker.com/engine/install/ubuntu/

### Install repo from Github

* generate an SSH key for communicating with Github
* Add SSH key to agent on local machine

```commandline
$ ssh-keygen -t rsa -b 4096 -C "aimee.stewart@ku.edu"
$ eval "$(ssh-agent -s)"
$ ssh-add ~/.ssh/id_rsa
$ cat .ssh/id_rsa.pub 
```
* Add the SSH to Github by printing to console, copying, adding in Github profile
* clone the repository

```commandline
$ cat .ssh/id_rsa.pub 
$ # <copy to profile in github website>
$ cd ~/git
$ git clone git@github.com:specifysystems/sp_network.git
$ git checkout <branch>
```

### Install certificates into config directory

* Link the certificates in the repo config directory 

```commandline
$ cd ~/git/sp_network
$ cd config 
$ ln -s ~/certificates/fullchain1.pem
$ ln -s ~/certificates/privkey1.pem
```

## Testing

On a development server, check the following URL endpoints:

* Index page: https://localhost

* Broker:
  * https://localhost/broker/api/v1/
    * https://localhost/broker/api/v1/badge/
    * https://localhost/broker/api/v1/name/
    * https://localhost/broker/api/v1/occ/
    * https://localhost/broker/api/v1/frontend/
  
  * https://localhost/broker/api/v1/badge/gbif
  * https://localhost/broker/api/v1/occ/?occid=a7156437-55ec-4c6f-89de-938f9361753d
  * https://localhost/broker/api/v1/name/Harengula%20jaguana
  * https://localhost/broker/api/v1/frontend/?occid=a7156437-55ec-4c6f-89de-938f9361753d
  
For local testing in a development environment, tests in the tests directory
require the lmtest module available at https://github.com/lifemapper/lmtest.

Environment variables set in the Docker containers from the .env.conf file are
necessary to test containers from the host machine.

**Temp solution:** Export these variables to the local environment in the python
virtual environment activation script (bin/activate) script.

```zsh
export SECRET_KEY="dev"
export WORKING_DIRECTORY="scratch-path"
```

**Specify Network** homepage is now available at https://localhost/ and http://localhost.

**Broker** (aka back-end):

   * https://localhost/broker/api/v1/name?namestr=Notemigonus%20crysoleucas%20(Mitchill,%201814)
   * https://localhost/broker/api/v1/occ?occid=01493b05-4310-4f28-9d81-ad20860311f3

**Webpack** is watching for front-end file changes and rebuilds the bundle when
needed.

**Flask** is watching for back-end file changes and restarts the server when
needed.


# Docker manipulation

## Run the containers (production)

```zsh
docker compose up -d
```

Specify Network is now available at [https://localhost/](https://localhost:443)


## Run the containers (development)

```zsh
docker compose -f docker-compose.yml -f docker-compose.development.yml up
```

Flask has hot-reload enabled.


## Rebuild/restart

To delete all containers, images, networks and volumes, stop any running
containers:

```zsh
docker compose stop
```

And run this command (which ignores running container):

```zsh
docker system prune --all --volumes
```

Then rebuild/restart:

```zsh
docker compose up -d
```

## Examine container

To examine containers at a shell prompt:

```zsh
docker exec -it sp_network-nginx-1 /bin/sh
```

Error port in use:
"Error starting userland proxy: listen tcp4 0.0.0.0:443: bind: address already in use"

See what else is using the port.  In my case apache was started on reboot.  Bring down
all docker containers, shut down httpd, bring up docker.

```zsh
lsof -i -P -n | grep 443
docker compose down
systemctl stop httpd
docker compose  up -d
```

# Dev Environment 

* Create a virtual environment and install python libs there

```commandline
$ cd ~/git/sp_network 
$ python3 -m venv venv
$ . venv/bin/activate
$ pip install -r requirements.txt
```


## Configure Debugger in local IDE

[Instructions for PyCharm]
(https://kartoza.com/en/blog/using-docker-compose-based-python-interpreter-in-pycharm/)

## Debug

To run flask in debug mode, first setup Flask environment, then start the flask 
application.

```zsh
export FLASK_ENV=development
export FLASK_APP=flask_app.broker.routes
flask run
```

`broker` container is running `debugpy` on port `5003`.

w/ SSL:
https://localhost:5003/broker/api/v1/name?namestr=Notemigonus%20crysoleucas%20(Mitchill,%201814)
https://localhost:5003/broker/api/v1/occ?occid=01493b05-4310-4f28-9d81-ad20860311f3

## Troubleshooting

### pip errors with SSL

  * add trusted-host option at command line

```commandline
pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org ~/git/lmpy
```
  * for processes that call pip, create a pip configuration file , then export as
    PIP_CONFIG_FILE environment variable in .bashrc

```commandline
# ~/pip.conf
[install]
trusted-host = pypi.python.org
               pypi.org
               files.pythonhosted.org

# ~/.bashrc
export PIP_CONFIG_FILE ~/pip.conf
```

###  pre-commit errors with self-signed certificate
  * turn off verification (but this leaves you open to man-in-the-middle attacks)

```commandline
git config --global http.sslVerify false
```

  * turn on again with

```commandline
git config --global http.sslVerify true

```

### pre-commit build errors

* Errors installing toml, Poetry, dependencies of isort.
  * Updated .pre-commit-config.yaml isort version to latest,
     https://github.com/PyCQA/isort, fixed build

# Misc

## Process DWCAs

You can setup a cron job to process pending DWCAs.

See `./cron/process_dwcas_cron.in`.

Note, you many need to modify `sp_cache-1` to reflect your container name.
