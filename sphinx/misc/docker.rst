Docker
##############################

Standard manipulation
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
    # or
    sudo docker compose -f docker-compose.development.yml -f docker-compose.yml  up

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


Troubleshooting
=================================

Out of Space Problem
------------------

Running `certbot certificates` failed because the EC2 instance running Docker
containers for Specify Network development shows disk full::

    root@ip-172-31-86-62:~# df -h
    Filesystem      Size  Used Avail Use% Mounted on
    /dev/root       7.6G  7.6G     0 100% /
    tmpfs           483M     0  483M   0% /dev/shm
    tmpfs           194M   21M  173M  11% /run
    tmpfs           5.0M     0  5.0M   0% /run/lock
    /dev/xvda15     105M  6.1M   99M   6% /boot/efi
    overlay         7.6G  7.6G     0 100% /var/lib/docker/overlay2/82d82cc5eb13260207b94443934c7318af651ea96a5fcd88c579f23224ba099d/merged
    overlay         7.6G  7.6G     0 100% /var/lib/docker/overlay2/cb0d78289131b3925e21d7eff2d03c79fe432eeba2d69a33c6134db40dc3caf3/merged
    overlay         7.6G  7.6G     0 100% /var/lib/docker/overlay2/3bd6d12b36e746f9c74227b6ac9d928a3179d8b604a9dea4fd88625eab84be1f/merged
    tmpfs            97M  4.0K   97M   1% /run/user/1000

The disk is small, but the culprit is /var/lib/docker/overlay2

Some strategies at:
https://forums.docker.com/t/some-way-to-clean-up-identify-contents-of-var-lib-docker-overlay/30604/19

Solution:
-------------------

* The instance was created with a volume of an 8gb default size.
* Stop the instance
* Modify the volume.
* Restart the EC2 instance - ok while the volume is in the optimizing state.
* If the instance does not recognize the extended volume immediately::

    ubuntu@ip-172-31-91-57:~$ df -h
    Filesystem      Size  Used Avail Use% Mounted on
    /dev/root       7.6G  7.6G     0 100% /
    tmpfs           475M     0  475M   0% /dev/shm
    tmpfs           190M   11M  180M   6% /run
    tmpfs           5.0M     0  5.0M   0% /run/lock
    /dev/xvda15     105M  6.1M   99M   6% /boot/efi
    tmpfs            95M  4.0K   95M   1% /run/user/1000
    ubuntu@ip-172-31-91-57:~$ sudo lsblk
    sudo: unable to resolve host ip-172-31-91-57: Temporary failure in name resolution
    NAME     MAJ:MIN RM  SIZE RO TYPE MOUNTPOINTS
    loop0      7:0    0 24.9M  1 loop /snap/amazon-ssm-agent/7628
    loop1      7:1    0 25.2M  1 loop /snap/amazon-ssm-agent/7983
    loop2      7:2    0 55.7M  1 loop /snap/core18/2796
    loop3      7:3    0 55.7M  1 loop /snap/core18/2812
    loop4      7:4    0 63.9M  1 loop /snap/core20/2105
    loop5      7:5    0 63.9M  1 loop /snap/core20/2182
    loop6      7:6    0   87M  1 loop /snap/lxd/27037
    loop7      7:7    0   87M  1 loop /snap/lxd/27428
    loop8      7:8    0 40.4M  1 loop /snap/snapd/20671
    loop9      7:9    0 39.1M  1 loop /snap/snapd/21184
    xvda     202:0    0   30G  0 disk
    ├─xvda1  202:1    0  7.9G  0 part /
    ├─xvda14 202:14   0    4M  0 part
    └─xvda15 202:15   0  106M  0 part /boot/efi

* extend the filesystem:
  https://docs.aws.amazon.com/ebs/latest/userguide/recognize-expanded-volume-linux.html
* In this case we want to extend xvda1, so::

    $ sudo growpart /dev/xvda 1
    sudo: unable to resolve host ip-172-31-91-57: Temporary failure in name resolution
    mkdir: cannot create directory ‘/tmp/growpart.1496’: No space left on device
    FAILED: failed to make temp dir

* We must free up space to allow extension::

    $ sudo docker system prune --all --volumes
    sudo: unable to resolve host ip-172-31-91-57: Temporary failure in name resolution
    WARNING! This will remove:
      - all stopped containers
      - all networks not used by at least one container
      - all volumes not used by at least one container
      - all images without at least one container associated to them
      - all build cache

    Are you sure you want to continue? [y/N] y
    Deleted Containers:
    24768ca767d37f248eff173f13556007468330298329200d533dfa9ca011e409
    809709d6f8bfa8575009a0d07df16ee78852e9ab3735aa19561ac0dbc0313123
    64591ed14ecae60721ea367af650683f738636167162f6ed577063582c210aa9

    Deleted Networks:
    sp_network_nginx

    Deleted Images:
    untagged: nginx:alpine
    untagged: nginx@sha256:a59278fd22a9d411121e190b8cec8aa57b306aa3332459197777583beb728f59
    deleted: sha256:529b5644c430c06553d2e8082c6713fe19a4169c9dc2369cbb960081f52924ff
    ...
    deleted: sha256:e74dab46dbca98b4be75dfbda3608cd857914b750ecd251c4f1bdbb4ef623c8c

    Total reclaimed space: 1.536GB

* Extend filesystem::

    $ sudo growpart /dev/xvda 1
    sudo: unable to resolve host ip-172-31-91-57: Temporary failure in name resolution
    CHANGED: partition=1 start=227328 old: size=16549855 end=16777183 new: size=62687199 end=62914527
    $ df -h
    Filesystem      Size  Used Avail Use% Mounted on
    /dev/root       7.6G  5.7G  2.0G  75% /
    tmpfs           475M     0  475M   0% /dev/shm
    tmpfs           190M   18M  173M  10% /run
    tmpfs           5.0M     0  5.0M   0% /run/lock
    /dev/xvda15     105M  6.1M   99M   6% /boot/efi
    tmpfs            95M  4.0K   95M   1% /run/user/1000


* Stop apache2 if running
* Rebuild the docker containers
