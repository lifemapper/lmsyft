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

Install
======================================

Install dependencies
---------------------------------------

AWS Client, Certbot::

    $ sudo apt update
    $ sudo apt install awscli, certbot


Install Docker
---------------------------------------

Add docker repository, then use apt to install Docker:
https://docs.docker.com/engine/install/ubuntu/

Install/Update repo from Github
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

DNS
----------------------

If this is a development or production server with an actual domain, first point the
DNS record (through whatever service is managing the domain, GoDaddy in the case of
spcoco.org) to the static IP address for the server.

For AWS, create (or modify) an Elastic IP address to point to the EC2 instance.

If replacing an EC2 instance, disassociate the Elastic IP address from the old EC2
instance, and associate it with the new instance.

SSL
-----------------------------------
:ref:`Specify Network SSL certificates`


Direct Docker to correct FQDN
------------------------------------

Edit FQDN value in .env.analyst.conf and .env.broker.conf (referenced by the docker
compose file) and server_name in config/nginx.conf to actual FQDN.


Docker
=================================

More info at :ref:`Docker`

AWS Config
================

Boto3 getting Error "botocore.exceptions.NoCredentialsError

Create credentials file on host EC2 instance

Test
===========================
On a development server, check the following URL endpoints:

* Broker:

  * https://localhost.broker
  * https://localhost.broker/api/v1/

  * https://localhost.broker/api/v1/badge/
  * https://localhost.broker/api/v1/name/
  * https://localhost.broker/api/v1/occ/
  * https://localhost.broker/api/v1/frontend/

  * https://localhost.broker/api/v1/badge/gbif?icon_status=active
  * https://localhost.broker/api/v1/occ/?occid=a7156437-55ec-4c6f-89de-938f9361753d
  * https://localhost.broker/api/v1/name/Harengula%20jaguana
  * https://localhost.broker/api/v1/frontend/?occid=a7156437-55ec-4c6f-89de-938f9361753d

* Analyst:

  * https://localhost.analyst
  * https://localhost.analyst/api/v1/

  * https://localhost.analyst/api/v1/count/
  * https://localhost.analyst/api/v1/rank/

  * http://localhost.analyst/api/v1/count/?dataset_key=0000e36f-d0e9-46b0-aa23-cc1980f00515
  * http://localhost.analyst/api/v1/rank/?by_species=true

For local testing in a development environment, tests in the tests directory
require the lmtest module available at https://github.com/lifemapper/lmtest.

Environment variables set in the Docker containers from the .env.broker.conf and
.env.broker.conf files are necessary to inform the host machine/container of its FQDN.

**Temp solution:** Export these variables to the local environment in the python
virtual environment activation script (bin/activate) script::

    export SECRET_KEY="dev"
    export WORKING_DIRECTORY="scratch-path"


**Specify Network** homepage is now available at https://localhost/

**Broker** (aka back-end):

   * https://localhost/api/v1/name?namestr=Notemigonus%20crysoleucas%20(Mitchill,%201814)
   * https://localhost/api/v1/occ?occid=01493b05-4310-4f28-9d81-ad20860311f3

**Webpack** is watching for front-end file changes and rebuilds the bundle when
needed.

**Flask** is watching for back-end file changes and restarts the server when
needed.

Dev Environment
==========================

* Base system libraries::

    sudo apt get update
    sudo apt install awscli, certbot, apt install python3.10-venv

* Create a virtual environment and install python libs there::

    $ cd ~/git/sp_network
    $ python3 -m venv venv
    $ . venv/bin/activate
    $ pip install -r requirements.txt


Configure Debugger
========================================

Pycharm
------------------
[Instructions for PyCharm]
(https://kartoza.com/en/blog/using-docker-compose-based-python-interpreter-in-pycharm/)

Flask
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


For webserver errors
-----------------------

Check logs of nginx container::

    $ sudo docker logs --tail 1000 sp_network-nginx-1
    $ sudo docker logs --tail 1000 sp_network-broker-1


Import error from werkzeug.urls
--------------------------------------

Error: "... cannot import name 'url_quote' from 'werkzeug.urls'" in broker container
Fix: Add Werkzeug==2.2.2 to requirements.txt to ensure it does not use 3.0+
Then stop/rebuild/start::

    $ sudo docker compose stop
    $ sudo docker system prune --all --volumes
    $ sudo docker compose up -d


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


Dependencies:
==============

Schema openapi3==1.1.0


TODO:
============

Add swagger doc generation for APIs