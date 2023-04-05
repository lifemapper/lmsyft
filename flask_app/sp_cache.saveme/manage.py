"""Entrypoint for Resolver Flask App."""

from flask.cli import FlaskGroup

from flask_app.sp_cache import app
from flask_app.sp_cache.process_dwca import main as process_dwca

cli = FlaskGroup(app)


@cli.command('process_dwcas')
def process_dwcas():
    """Forward the call to the CLI command process_dwcas."""
    process_dwca()


if __name__ == '__main__':
    cli()
