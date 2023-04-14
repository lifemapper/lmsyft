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
[lmtrex repository](https://github.com/lifemapper/lmtrex/) should
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

The Syftorium presents this information in multivariate-, but subsettable, space
to provide as much value and feedback to the community as possible.

# Specify Network Deployment 

## Local Deployment

To run the containers, generate `fullchain.pem` and `privkey.pem` (certificate
and the private key) using Let's Encrypt and link these files in the (currently
separate config directories) `./specify_cache/lmtrex/config/` and `./specify_cache/config/`.

While in development, you can generate self-signed certificates then link them in
./specify_cache/lmtrex/config/ directory for this project:

```zsh
mkdir ~/self-signed-certificates

openssl req \
  -x509 -sha256 -nodes -newkey rsa:2048 -days 365 \
  -keyout ~/self-signed-certificates/privkey.pem \
  -out ~/self-signed-certificates/fullchain.pem

cd ./specify_cache/config
ln -s ~/self-signed-certificates/privkey.pem
ln -s ~/self-signed-certificates/fullchain.pem
```

To run the production container, or the development container with HTTPs
support, generate `fullchain.pem` and `privkey.pem` (certificate and the private
key) using Let's Encrypt and put these files into the `./config/`
directory.

### Production

Modify the `FQDN` environment variable in `.env.conf` as needed.

Run the containers:

```zsh
docker compose up -d
```

lmsyft is now available at [https://localhost/](https://localhost:443)

### Development

Run the containers:

```zsh
docker compose -f docker-compose.yml -f docker-compose.development.yml up
```

Flask has hot-reload enabled.

### Testing

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
export SOLR_SERVER="https://localhost"
export SOLR_PORT=8983
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

## AWS Deployment

### Create and setup an EC2 instance
* Create in AWS dashboard, including SSH keypari
* update apt
* install AWS client, awscli

```commandline
$ sudo apt update
$ sudo apt install awscli
```

### Extend the SSH timeout

* SSH Client: vim ~/.ssh/config

```
Host *
    ServerAliveInterval 20
```

* SSH Server: sudo vim /etc/ssh/sshd_config

```
ClientAliveInterval 1200
ClientAliveCountMax 3
```

* Then run `sudo systemctl reload sshd` 
* Copy SSH private key to each machine used for AWS access


### Setup SSL
* Find IP address of EC2 instance
* Request a public certificate through Certificate Manager (ACM)
  * Choose DNS validation
  * Add tags specify_network, dev or prod, others
* Go to DNS hosting service (GoDaddy) and add FQDN with IP address 
  * Takes ~30 min for DNS to propogate
  * Takes several hours for Amazon to validate and issue certificate


# Docker manipulation

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

And rebuild/restart:

```zsh
docker compose up -d
```

## Examine container

To examine containers at a shell prompt:

```zsh
docker exec -it specify_cache-nginx-1 /bin/sh
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

## Setup
* Create a virtual environment and install python libs there

```commandline
python3 -m venv venv
. venv/bin/activate
pip install -r requirements.txt
```

## Configure Debugger

Debugger configuration is IDE dependent. [Instructions for
PyCharm](https://kartoza.com/en/blog/using-docker-compose-based-python-interpreter-in-pycharm/)

`broker` container is running `debugpy` on port `5003`.

## Debug

To run flask in debug mode, first setup virtual environment for python at the
top level of the repo, activate, then add dependencies from requirements.txt:

```zsh
cd ~/git/specify_network
python3 -m venv venv
. venv/bin/activate
pip3 install -r requirements.txt
```

then start the flask application

```zsh
export FLASK_ENV=development
export FLASK_APP=flask_app.broker.routes
flask run
```

## Test through flask

no SSL:
http://localhost:5003/broker/api/v1/name?namestr=Notemigonus%20crysoleucas%20(Mitchill,%201814)
http://localhost:5003/broker/api/v1/occ?occid=01493b05-4310-4f28-9d81-ad20860311f3

## Local SSL certificates

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


# Local development setup


## Troubleshooting

## pip errors with SSL

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

# at terminal
$ source ~/.bashrc
```

* pre-commit errors with self-signed certificate
  * turn off verification (but this leaves you open to man-in-the-middle attacks)

```commandline
git config --global http.sslVerify false

```

  * turn on again with

```commandline
git config --global http.sslVerify true

```

## pre-commit build errors

* remove cache, then re-install pre-commit

```commandline
$ rm -rf ~/.cache/pre-commit
$ pre-commit run
```

* still errors installing toml, Poetry, dependencies of isort.
  * Updated .pre-commit-config.yaml isort version to latest,
     https://github.com/PyCQA/isort, fixed build
