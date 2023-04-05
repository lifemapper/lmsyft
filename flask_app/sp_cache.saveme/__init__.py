"""Create Resolver Flask App."""

from flask_app.application import create_app
from flask_app.sp_cache.routes import bp

app = create_app(bp)
