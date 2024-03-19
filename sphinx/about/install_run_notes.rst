Install/Run Notes
#########################

Contains
============

* Specify Network API services

  * Tools/classes for broker, including

    * Flask application for individual API endpoints and frontend
    * classes for Provider API connectors
    * standardized API service output (s2n)

  * Tools/classes for analyst, including

    * AWS scripts and
    * Classes for use on EC2 or other AWS resources

      * geotools for geospatial intersection/annotations
      * aggregation, summary tools for writing tabular summaries

Deployment
===================================

SSL
-----------------------------------

Local self-signed certificates
.........................................
To run the containers, generate `fullchain.pem` and `privkey.pem` (certificate
and the private key) using Let's Encrypt and link these files in `./sp_network/config/`.

While in development, generate self-signed certificates then link them in
~/git/sp_network/config/ directory for this project::

  $ mkdir ~/certificates

  openssl req \
  -x509 -sha256 -nodes -newkey rsa:2048 -days 365 \
  -keyout ~/certificates/privkey.pem \
  -out ~/certificates/fullchain.pem

  $ cd ~/git/sp_network/config
  $ ln -s ~/certificates/privkey.pem
  $ ln -s ~/certificates/fullchain.pem

To run either the production or the development containers with HTTPS
support, generate `fullchain.pem` and `privkey.pem` (certificate and the private
key) using Let's Encrypt, link these files in the `./config/` directory.
Full instructions in the docs/aws-steps.rst page, under `Set up TLS/SSL`

Modify the `FQDN` environment variable in `.env.conf` as needed.

TLS/SSL using Certificate Authority (CA)
..................................................

* Make sure that DNS has propogated for domain for SSL
* Stop apache service
* request a certificate for the domain

::

    ubuntu@ip-172-31-86-62:~$ sudo systemctl stop apache2
    ubuntu@ip-172-31-86-62:~$ sudo certbot certonly -v
    Saving debug log to /var/log/letsencrypt/letsencrypt.log

    How would you like to authenticate with the ACME CA?
    - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    1: Spin up a temporary webserver (standalone)
    2: Place files in webroot directory (webroot)
    - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    Select the appropriate number [1-2] then [enter] (press 'c' to cancel): 1
    Plugins selected: Authenticator standalone, Installer None
    Please enter the domain name(s) you would like on your certificate (comma and/or
    space separated) (Enter 'c' to cancel): broker-dev.spcoco.org analyst-dev.spcoco.org
    Requesting a certificate for broker-dev.spcoco.org and analyst-dev.spcoco.org
    Performing the following challenges:
    http-01 challenge for broker-dev.spcoco.org
    Waiting for verification...
    Cleaning up challenges

    Successfully received certificate.
    Certificate is saved at: /etc/letsencrypt/live/broker-dev.spcoco.org/fullchain.pem
    Key is saved at:         /etc/letsencrypt/live/broker-dev.spcoco.org/privkey.pem
    This certificate expires on 2023-10-18.
    These files will be updated when the certificate renews.
    Certbot has set up a scheduled task to automatically renew this certificate in the background.

    - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    If you like Certbot, please consider supporting our work by:
     * Donating to ISRG / Let's Encrypt:   https://letsencrypt.org/donate
     * Donating to EFF:                    https://eff.org/donate-le
    - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    ubuntu@ip-172-31-86-62:~$


* as superuser, link the newly created fullchain.pem and privkey.pem files from the
  letsencrypt live to the project/config directory
* change the owner so that they can be used in Docker containers

::

    $ sudo su -
    # cp -p /etc/letsencrypt/live/dev.spcoco.org/* /home/ubuntu/certificates/
    # chown ubuntu:ubuntu /home/ubuntu/certificates/*
    # exit
    $ cd ~/git/sp_network/config
    $ ln -s ~/certificates/fullchain.pem
    $ ln -s ~/certificates/privkey.pem

Renew Certbot SSL certificates
.........................................

SSL certificates are served from the instance (AWS EC2), and need port 80 to be renewed.
These are administered by Letsencrypt using Certbot and are only valid for 90 days at
a time. When it is time for a renewal (approx every 60 days), bring the docker
containers down. Renew the certificates, then bring the containers up again.

Amazon EC2 containers do not need apache running, certbot runs its own temp web server.

Test with https://broker.spcoco.org/api/v1/frontend/?occid=01493b05-4310-4f28-9d81-ad20860311f3

::

    $ sudo certbot certificates
    $ sudo docker compose stop
    $ sudo su -
    # certbot renew
    # cp -p /etc/letsencrypt/live/dev.spcoco.org/* /home/ubuntu/certificates/
    # chown ubuntu:ubuntu /home/ubuntu/certificates/*
    # exit
    $ ls -lahtr ~/git/sp_network/config
    <check symlinks - should still be valid>
    $ sudo docker system prune --all --volumes
    $ sudo docker compose up -d

TODO: SSL through Amazon
.........................................

* Create Elastic IP address for EC2 instance
* Request a public certificate through Certificate Manager (ACM)
  * Choose DNS validation
  * Add tags sp_network, dev or prod, others


Install
======================================

Install dependencies
---------------------------------------

Certbot::

    $ sudo apt update
    $ sudo apt install certbot


Install Docker
---------------------------------------

Add docker repository, then use apt to install Docker:
https://docs.docker.com/engine/install/ubuntu/

Install repo from Github
---------------------------------------

* generate an SSH key for communicating with Github
* Add SSH key to agent on local machine

::

    $ ssh-keygen -t rsa -b 4096 -C "aimee.stewart@ku.edu"
    $ eval "$(ssh-agent -s)"
    $ ssh-add ~/.ssh/id_rsa
    $ cat .ssh/id_rsa.pub

* Add the SSH to Github by printing to console, copying, adding in Github profile
* clone the repository

::
    $ cat .ssh/id_rsa.pub
    $ # <copy to profile in github website>
    $ cd ~/git
    $ git clone git@github.com:specifysystems/sp_network.git
    $ git checkout <branch>

Install certificates into config directory
-------------------------------------------------------

* Link the certificates in the repo config directory

::
    $ cd ~/git/sp_network
    $ cd config
    $ ln -s ~/certificates/fullchain1.pem
    $ ln -s ~/certificates/privkey1.pem

Direct Docker to correct FQDN
------------------------------------

Edit FQDN value in env.conf (or .env.analyst.conf and .env.broker.conf) to actual FQDN


Testing
---------------------------------------
On a development server, check the following URL endpoints:

* Index page: https://localhost

* Broker:

  * https://localhost/api/v1/

    * https://localhost/api/v1/badge/
    * https://localhost/api/v1/name/
    * https://localhost/api/v1/occ/
    * https://localhost/api/v1/frontend/

  * https://localhost/api/v1/badge/gbif?icon_status=active
  * https://localhost/api/v1/occ/?occid=a7156437-55ec-4c6f-89de-938f9361753d
  * https://localhost/api/v1/name/Harengula%20jaguana
  * https://localhost/api/v1/frontend/?occid=a7156437-55ec-4c6f-89de-938f9361753d

For local testing in a development environment, tests in the tests directory
require the lmtest module available at https://github.com/lifemapper/lmtest.

Environment variables set in the Docker containers from the .env.broker.conf and
.env.broker.conf files are necessary to inform the host machine/container of its FQDN.

**Temp solution:** Export these variables to the local environment in the python
virtual environment activation script (bin/activate) script::

    export SECRET_KEY="dev"
    export WORKING_DIRECTORY="scratch-path"


**Specify Network** homepage is now available at https://localhost/ and http://localhost.

**Broker** (aka back-end):

   * https://localhost/api/v1/name?namestr=Notemigonus%20crysoleucas%20(Mitchill,%201814)
   * https://localhost/api/v1/occ?occid=01493b05-4310-4f28-9d81-ad20860311f3

**Webpack** is watching for front-end file changes and rebuilds the bundle when
needed.

**Flask** is watching for back-end file changes and restarts the server when
needed.

Troubleshooting
===========================================

For webserver errors, check logs of nginx container::

    $ sudo docker logs --tail 1000 sp_network-nginx-1
    $ sudo docker logs --tail 1000 sp_network-broker-1


Error: "... cannot import name 'url_quote' from 'werkzeug.urls'" in broker container
Fix: Add Werkzeug==2.2.2 to requirements.txt to ensure it does not use 3.0+
Then stop/rebuild/start::

    $ sudo docker compose stop
    $ sudo docker system prune --all --volumes
    $ sudo docker compose up -d

Docker manipulation
=================================

Edit the docker environment files
-------------------------------------------

* Add the container domain name to the files .env.broker.conf and .env.analyst.conf
* Change the FQDN value to the fully qualified domain name of the server.

  * If this is a local testing deployment, it will be "localhost"
  * For a development or production server it will be the FQDN with correct subdomain
    for each container, i.e FQDN=broker.spcoco.org in .env.broker.conf and
    FQDN=analyst.spcoco.org in .env.analyst.conf

Run the containers (production)
-------------------------------------------

Start the containers with the Docker composition file::

    sudo docker compose -f docker-compose.yml up -d

Specify Network is now available at [https://localhost/](https://localhost:443)


Run the containers (development)
-------------------------------------------

Note that the development compose file, docker-compose.development.yml, is referenced
first on the command line.  It has elements that override those defined in the
general compose file, docker-compose.yml::

    sudo docker compose -f docker-compose.development.yml -f docker-compose.yml  up

Flask has hot-reload enabled.


Rebuild/restart
-------------------------------------------

To delete all containers, images, networks and volumes, stop any running
containers::

    sudo docker compose stop


And run this command (which ignores running container)::

    sudo docker system prune --all --volumes

Then rebuild/restart::

    sudo docker compose up -d

Examine container
-------------------------------------------

To examine containers at a shell prompt::

    sudo docker exec -it sp_network-nginx-1 /bin/sh

Error port in use:
"Error starting userland proxy: listen tcp4 0.0.0.0:443: bind: address already in use"

See what else is using the port.  In my case apache was started on reboot.  Bring down
all docker containers, shut down httpd, bring up docker.

::
    lsof -i -P -n | grep 443
    sudo docker compose down
    sudo systemctl stop httpd
    sudo docker compose  up -d


Dev Environment
==========================

* Create a virtual environment and install python libs there::

    $ cd ~/git/sp_network
    $ python3 -m venv venv
    $ . venv/bin/activate
    $ pip install -r requirements.txt


Configure Debugger in local IDE
========================================

[Instructions for PyCharm]
(https://kartoza.com/en/blog/using-docker-compose-based-python-interpreter-in-pycharm/)

Debug
-------------------------------------------

To run flask in debug mode, first set up Flask environment, then start the flask
application (in this case, main in flask_app.broker.routes.py).  Only one resource
(aka broker or analyst) at a time can be tested in this way.
Reset the FLASK_APP variable to test an alternate resource::

    export FLASK_ENV=development
    export FLASK_APP=flask_app.broker.routes:app
    # or
    # export FLASK_APP=flask_app.analyst.routes:app
    flask run

* `broker` container is running `debugpy` on localhost, port `5000`
* ** the broker frontend can NOT be tested this way, as it depends on a docker volume

* Test with http, no https!!

  http://localhost:5000/api/v1/name?namestr=Notemigonus%20crysoleucas%20(Mitchill,%201814)
  http://localhost:5000/api/v1/occ?occid=01493b05-4310-4f28-9d81-ad20860311f3

Troubleshooting
======================================

pip errors with SSL
-------------------------------------------

* add trusted-host option at command line::

    pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org ~/git/lmpy

* for processes that call pip, create a pip configuration file , then export as
    PIP_CONFIG_FILE environment variable in .bashrc::

    # ~/pip.conf
    [install]
    trusted-host = pypi.python.org
                   pypi.org
                   files.pythonhosted.org

    # ~/.bashrc
    export PIP_CONFIG_FILE ~/pip.conf

pre-commit errors with self-signed certificate
---------------------------------------------------------

* turn off verification (but this leaves you open to man-in-the-middle attacks)::

    git config --global http.sslVerify false

  * turn on again with::

    git config --global http.sslVerify true


pre-commit build errors
--------------------------------------

* Errors installing toml, Poetry, dependencies of isort.
  * Updated .pre-commit-config.yaml isort version to latest,
     https://github.com/PyCQA/isort, fixed build

AWS setup
===================================

* Add raw GBIF data to S3




Dependencies:
==============

Schema openapi3==1.1.0


TODO:
============

Add swagger doc generation for APIs