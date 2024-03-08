Specify Network SSL certificates
######################################


SSL certificates are served from the host machine, and are administered by
Letsencrypt using Certbot.  They are only valid for 90 days at a time.

TODO: move administration to AWS, and script renewal if necessary


Renewal procedure
=============================================

* Change to superuser, then check the validity of your certificates::

    sudo su -
    certbot certificates

* When it is time for a renewal (approx every 60 days), move to the Specify Network
  project directory where Docker was started, and stop the Docker containers::

    cd /home/ubuntu/git/sp_network
    docker compose stop

* Renew the certificates::

    certbot renew

* Move to /etc/letsencrypt/archive/<FQDN> and find the most recent
  certificate names in the directory (certX.pem, chainX.pem, fullchainX.pem,
  privkeyX.pem, where X is an integer)::

    cd /etc/letsencrypt/archive/spcoco.org/
    ls -lahtr

* Copy the new certificates to /home/ubuntu/certificates, changing
  the name to cert.pem, chain.pem, fullchain.pem, privkey.pem (no X integer).  Then
  change the owner from root, to the username (ubuntu)::

    cp cert4.pem /home/ubuntu/certificates/cert.pem
    cp chain4.pem /home/ubuntu/certificates/chain.pem
    cp fullchain4.pem /home/ubuntu/certificates/fullchain.pem
    cp privkey4.pem /home/ubuntu/certificates/privkey.pem

* Move to the directory with the certificates and change the
  owner to ubuntu, then exit superuser::

    cd /home/ubuntu/certificates
    chown ubuntu:ubuntu *
    exit

* Move to the config directory and create symbolic links to the new fullchain.pem
  and privkey.pem files::

    cd /home/ubuntu/git/sp_network/config
    ln -s /home/ubuntu/certificates/fullchain.pem
    ln -s /home/ubuntu/certificates/privkey.pem

* Then restart the containers::

    sudo docker compose up -d
