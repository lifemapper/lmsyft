## Occurrence Broker 

### Manually check specify_cache 

* Implemented in solr, collection is specimen_records
* Schema is in lmysft/solr_cores/specimen_records/conf/schema.xml
* Go to development or production machine (currently Syftorium, notyeti-195 and joe-125) and query solr directly
  ::
  
  curl http://localhost:8983/solr/specimen_records/select?q=identifier:2c1becd5-e641-4e83-b3f5-76a55206539a


### Local debugging of flask app

* Set up python virtual environment for the project
* Connect IDE to venv python interpreter
* Run flask at command prompt
* Assuming that the project directory is 

```zsh
export PROJDIR=~/git/lmtrex
cd $PROJDIR/lmtrex/flask_app/broker/

export PYTHONPATH=$PROJECT_DIR
export FLASK_ENV=development
export FLASK_APP=routes
flask run
```

* Connect to localhost in browser.  
* Flask will auto-update on file save.
* Refresh browser after changes