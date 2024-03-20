Specify Network SSL certificates
######################################


SSL certificates are served from the host machine, and are administered by
Letsencrypt using Certbot.  They are only valid for 90 days at a time.

TODO: move administration to AWS, and script renewal if necessary

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


Install certificates into config directory
-------------------------------------------------------

* Create a ~/certificates directory to hold certificate files
* as superuser, copy the newly created fullchain.pem and privkey.pem files from the
  letsencrypt live
* change the owner so that they can be used in Docker containers
* Link the certificates in the repo config directory

::

    $ cd
    $ mkdir certificates
    $ sudo su -
    # cp -p /etc/letsencrypt/live/<fqdn>/* /home/ubuntu/certificates/
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
containers down.  Prune the volumes so the new containers and volumes will be created
with the updated certificates.  Renew the certificates, then bring the containers up.

Amazon EC2 containers do not need apache running, certbot runs its own temp web server.

Test with https://broker.spcoco.org/api/v1/frontend/?occid=01493b05-4310-4f28-9d81-ad20860311f3

::

    $ sudo certbot certificates
    $ sudo docker compose stop
    $ sudo su -
    # certbot renew
    # cp -p /etc/letsencrypt/live/spcoco.org/* /home/ubuntu/certificates/
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
