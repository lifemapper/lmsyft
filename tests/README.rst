==============================
Testing
==============================

Post data to Specify Cache: collection and specimen
Retrieve data through the Resolver: specimen with identifier


-----------------------------------
Development machine
-----------------------------------

Find the container names, then login to an interactive command prompt for the "runner"::

    (venv) astewart@badenov:~/git/specify_cache$ docker ps -a
    CONTAINER ID   IMAGE                     COMMAND                  CREATED      STATUS      PORTS                                                                      NAMES
    2020da80238b   nginx:1.21.3-alpine       "/docker-entrypoint.…"   2 days ago   Up 2 days   0.0.0.0:80->80/tcp, :::80->80/tcp, 0.0.0.0:443->443/tcp, :::443->443/tcp   specify_cache-nginx-1
    9fe5357b7953   solr:8.10.1-slim          "bash -c 'precreate-…"   2 days ago   Up 2 days   0.0.0.0:8983->8983/tcp, :::8983->8983/tcp                                  specify_cache-solr-1
    3dc835650bf4   specify_cache_runner      "/bin/sh -c 'venv/bi…"   2 days ago   Up 2 days   0.0.0.0:5001->5001/tcp, :::5001->5001/tcp                                  specify_cache-runner-1
    137c26bcb39c   specify_cache_resolver    "/bin/sh -c 'venv/bi…"   2 days ago   Up 2 days                                                                              specify_cache-resolver-1
    ec67f330dce0   specify_cache_sp_cache    "/bin/sh -c 'venv/bi…"   2 days ago   Up 2 days   0.0.0.0:5002->5002/tcp, :::5002->5002/tcp                                  specify_cache-sp_cache-1
    af625d18b984   specify_cache_back-end    "/bin/sh -c 'venv/bi…"   2 days ago   Up 2 days   0.0.0.0:5003->5003/tcp, :::5003->5003/tcp                                  specify_cache-back-end-1
    4c5a5fc1bb8c   specify_cache_front-end   "docker-entrypoint.s…"   2 days ago   Up 2 days                                                                              specify_cache-front-end-1

    (venv) astewart@badenov:~/git/specify_cache$ docker exec -it specify_cache-runner-1  /bin/sh
    ~ $

Go to the /home/specify directory, activate the venv, start python::

    ~ $ cd /home/specify
    ~ $ . venv/bin/activate
    ~ $ python



-----------------------------------
Data sources
-----------------------------------
IPT
KUI, collection ID: 8f79c802-a58c-447f-99aa-1d6a0790825a
* http://ipt.nhm.ku.edu:8080/ipt/resource?r=kubi_ichthyology


KUIT, collection ID: 56caf05f-1364-4f24-85f6-0c82520c2792
* http://ipt.nhm.ku.edu:8080/ipt/resource?r=kubi_ichthyology_tissue

Specify 7 implementations of those using the extensions that currently only go to GGBN
* https://ichthyology.specify.ku.edu/export/rss/)
