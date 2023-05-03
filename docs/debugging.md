# Debugging Flask and Docker instances

## IDE debugging of functions

* Set up python virtual environment for the project
* Connect IDE to venv python interpreter

## Local debugging of flask app

* Run flask at command prompt

```zsh
export FLASK_ENV=development
export FLASK_APP=flask_app.broker.routes
flask run
```

* Connect to localhost in browser.
* Flask will auto-update on file save.
* Refresh browser after changes
