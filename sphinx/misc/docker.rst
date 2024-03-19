Docker Troubleshooting
##############################

Out of space error
************************

Problem
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


Research
------------------

The disk is small, but the culprit is /var/lib/docker/overlay2

Some strategies at:
https://forums.docker.com/t/some-way-to-clean-up-identify-contents-of-var-lib-docker-overlay/30604/19

Actual disk usage is correctly reported here (unlike some of the use cases above), so
for now, clean it all out by stopping, pruning, removing images, killing the overlay2
directory, recreating the overlay2 directory, changing permissions, then rebuilding
and restarting the docker image::

    $ sudo docker compose stop
    $ sudo docker system prune --all --volumes
    $ sudo docker image ls
    REPOSITORY   TAG       IMAGE ID       CREATED        SIZE
    <none>       <none>    e6bf776fc762   2 months ago   1.43GB
    <none>       <none>    0ece9b23b9b3   2 months ago   108MB
    <none>       <none>    23e4dc1f7809   2 months ago   108MB
    <none>       <none>    529b5644c430   4 months ago   42.6MB

    $ sudo docker image rm <each image id>
    $ sudo du -skh /var/lib/docker/overlay2
    1.2G	/var/lib/docker/overlay2

    $ sudo rm -rf  /var/lib/docker/overlay2
    $ df -h
    Filesystem      Size  Used Avail Use% Mounted on
    /dev/root       7.6G  4.9G  2.8G  65% /
    tmpfs           483M     0  483M   0% /dev/shm
    tmpfs           194M  884K  193M   1% /run
    tmpfs           5.0M     0  5.0M   0% /run/lock
    /dev/xvda15     105M  6.1M   99M   6% /boot/efi
    tmpfs            97M  4.0K   97M   1% /run/user/1000

    $ sudo mkdir  /var/lib/docker/overlay2
    $ sudo ls -lahtr /var/lib/docker/overlay2
    total 8.0K
    drwx--x--- 12 root root 4.0K Mar 19 20:20 ..
    drwxr-xr-x  2 root root 4.0K Mar 19 20:20 .

    $ sudo chmod 710 /var/lib/docker/overlay2
    total 8.0K
    drwx--x--- 12 root root 4.0K Mar 19 20:20 ..
    drwx--x---  2 root root 4.0K Mar 19 20:20 .




Then uninstall docker (previously installed from docker repository noted in
about/install_run_notes), update apt repositories, re-install, reboot::

    $ sudo apt list docker --installed
    Listing... Done
    docker/jammy 1.5-2 all
    $ sudo apt-get update
    $ sudo apt remove docker
    ...
    $ sudo apt install docker
    ...
    $ sudo shutdown -r now

Apparently, ubuntu comes with a docker install, not removed by apt::

    $ dpkg -l | grep -i docker
    ii  docker-buildx-plugin               0.10.4-1~ubuntu.22.04~jammy             amd64        Docker Buildx cli plugin.
    ii  docker-ce                          5:23.0.4-1~ubuntu.22.04~jammy           amd64        Docker: the open-source application container engine
    ii  docker-ce-cli                      5:23.0.4-1~ubuntu.22.04~jammy           amd64        Docker CLI: the open-source application container engine
    ii  docker-ce-rootless-extras          5:23.0.4-1~ubuntu.22.04~jammy           amd64        Rootless support for Docker.
    ii  docker-compose-plugin              2.17.2-1~ubuntu.22.04~jammy             amd64        Docker Compose (V2) plugin for the Docker CLI.
    ii  wmdocker                           1.5-2                                   amd64        System tray for KDE3/GNOME2 docklet applications

    $ sudo sudo apt-get purge -y docker-buildx-plugin docker-ce docker-ce-cli docker-compose-plugin docker-ce-rootless-extras
    $ sudo apt-get autoremove -y --purge docker-buildx-plugin docker-ce docker-ce-cli docker-compose-plugin docker-ce-rootless-extras
    $ sudo rm -rf /var/lib/docker
    $ sudo groupdel docker
    $ sudo rm -rf /var/run/docker.sock

Then rebuild/restart docker::

    $ sudo docker compose -f docker-compose.development.yml -f docker-compose.yml  up
