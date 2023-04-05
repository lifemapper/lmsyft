"""Root Flask application for Syftorium."""
import os

from flask import Flask

try:
    SECRET_KEY = os.environ['SECRET_KEY']
except:
    SECRET_KEY = "dev"

# .....................................................................................
def create_app(blueprint, test_config=None):
    """Create a Flask application.

    Args:
        blueprint: Flask Blueprint to use for this app
        test_config (dict): Testing configuration parameters.

    Returns:
        Flask: Flask application.
    """
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY=SECRET_KEY,
        # DATABASE=os.path.join(app.instance_path, 'flaskr.sqlite'),
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    app.register_blueprint(blueprint)

    return app
