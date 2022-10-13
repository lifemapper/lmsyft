"""Entrypoint for Resolver Flask App."""

from flask.cli import FlaskGroup

from lmtrex.flask_app.broker.routes import app

cli = FlaskGroup(app)

if __name__ == '__main__':
    cli()
