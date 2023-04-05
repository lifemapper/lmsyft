"""Create Broker Flask App."""

from flask_app.application import create_app
from flask_app.broker.routes import bp

app = create_app(bp)
app.config['JSON_SORT_KEYS'] = False
