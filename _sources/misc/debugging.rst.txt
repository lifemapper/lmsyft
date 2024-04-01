Debugging Flask and Docker instances
###########################################################

IDE debugging of functions
=============================================

* Set up python virtual environment for the project
* Connect IDE to venv python interpreter

Local debugging of flask app
=============================================

* Choose to run the Analyst or Broker with FLASK_APP environment variable
* Run flask at command prompt

```zsh
export FLASK_ENV=development
export FLASK_APP=flask_app.broker.routes
export FLASK_APP=flask_app.analyst.routes
flask run
```
* With either Analyst or Broker, the development port will be 5000.  Connect to
  http://127.0.0.1:5000 in browser,

  * Broker
    * http://127.0.0.1:5000/api/v1/name/Acer%20opalus%20Miller?is_accepted=True&gbif_count=False&
    * http://127.0.0.1:5000/api/v1/occ/?occid=db8cc0df-1ed3-11e3-bfac-90b11c41863e&provider=gbif
    * http://127.0.0.1:5000/api/v1/badge/?provider=mopho

  * Analyst:
    http://127.0.0.1:5000/api/v1/count/?dataset_key=0000e36f-d0e9-46b0-aa23-cc1980f00515
    http://127.0.0.1:5000/api/v1/rank/?by_species=true

* Flask will auto-update on file save.
* Refresh browser after changes
* The frontend endpoint cannot be tested this way, as it depends on frontend
  **webpack-output** and **static-files** to be mounted as docker volumes.


Local debugging of Docker
=============================================

More info in about/install_run_notes


Run Docker containers (development)
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

    sudo docker compose -f docker-compose.development.yml -f docker-compose.yml  up

Examine container
-------------------------------------------

To examine containers at a shell prompt::

    sudo docker exec -it sp_network-nginx-1 /bin/sh
