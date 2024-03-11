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
flask run
```
* With either Analyst or Broker, the development port will be 5000

  * Connect to http://127.0.0.1:5000 in browser,
    i.e. http://127.0.0.1:5000/api/v1/name/Acer%20opalus%20Miller?is_accepted=True&gbif_count=False&

* Flask will auto-update on file save.
* Refresh browser after changes

