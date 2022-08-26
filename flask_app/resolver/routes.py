"""Flask route definitions for the resolver."""
import csv
from flask import Blueprint, request
import io
import json
from werkzeug.exceptions import NotFound, UnsupportedMediaType

import flask_app.resolver.solr_controller as controller
from flask_app.resolver.config import ARK_PATTERN
from flask_app.resolver.models import Ark

bp = Blueprint('resolve', __name__, url_prefix='/resolve')


# .....................................................................................
@bp.route('/', methods=['GET', 'POST'])
def resolver_endpoint():
    """Handle requests to the base resolver endpoint.

    Raises:
        UnsupportedMediaType: Raised if the content-type of the
            request data is something other than text/csv or application/json.

    Returns:
        dict: A dictionary of metadata about the collection holdings on GET.
        tuple: A tuple of empty string and HTTP status 204 for new POSTs.
    """
    if request.method.lower() == 'get':
        return controller.get_endpoint_metadata()
    else:
        content_type = request.headers.get('Content-Type', default='text/csv').lower()
        if content_type == 'text/csv':
            csv_data = io.StringIO(request.data)
            reader = csv.DictReader(csv_data)
            records = [Ark(record) for record in reader]
        elif content_type == 'application/json':
            records = [Ark(record) for record in request.json]
        else:
            raise UnsupportedMediaType(
                'Cannot ingest {} records this time.'.format(content_type)
            )
        controller.post_identifiers(records)
        return (json.dumps({'pattern': ARK_PATTERN}), 204)


# .....................................................................................
@bp.route('/<string:identifier>', methods=['GET'])
def resolver_get(identifier):
    """Get a record from the resolver collection.

    Args:
        identifier (str): An identifier to look for in the index.

    Raises:
        NotFound: Raised if the requested record is not found.

    Returns:
        dict: A dictionary of metadata for the requested record.
    """
    record = controller.get_identifier(identifier)
    if record is None:
        raise NotFound('The requested identifier was not found: {}'.format(identifier))
    return record
