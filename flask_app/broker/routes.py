from flask import Flask, Blueprint, request, render_template
import os

from flask_app.broker.constants import (
    TEMPLATE_DIR, STATIC_DIR, SCHEMA_DIR, SCHEMA_FNAME, S2nEndpoint)
from flask_app.broker.badge import BadgeSvc
from flask_app.broker.frontend import FrontendSvc
from flask_app.broker.name import NameSvc
from flask_app.broker.occ import OccurrenceSvc
from flask_app.broker.stats import StatsSvc

# downloadable from <baseurl>/static/schema/open_api.yaml

bp = Blueprint(
    'broker', __name__, url_prefix="/broker/api/v1", template_folder=TEMPLATE_DIR,
    static_folder=STATIC_DIR, static_url_path='/static')


# .....................................................................................
@bp.route('/', methods=['GET'])
def broker_status():
    """Get services available from broker.

    Returns:
        dict: A dictionary of status information for the server.
    """
    endpoints = S2nEndpoint.get_endpoints()
    system_status = 'In Development'
    return {
        'num_services': len(endpoints),
        'endpoints': endpoints,
        'status': system_status
    }


# ..........................
@bp.route('/schema')
def display_raw_schema():
    """Show the schema XML."""
    fname = os.path.join(SCHEMA_DIR, SCHEMA_FNAME)
    with open(fname, 'r') as f:
        schema = f.read()
    return schema


# ..........................
@bp.route('/swaggerui')
def swagger_ui():
    """Show the swagger UI to the schema."""
    return render_template('swagger_ui.html')


# .....................................................................................
@bp.route("/badge/")
def badge_endpoint():
    """Show the providers/icons available for the badge service."""
    response = BadgeSvc.get_endpoint()
    return response


# .....................................................................................
@bp.route('/badge/<string:provider>', methods=['GET'])
def badge_get(provider):
    """Get an occurrence record from available providers.

    Args:
        provider (str): An provider code for which to return an icon.

    Returns:
        dict: An image file as binary or an attachment.
    """
    # response = OccurrenceSvc.get_occurrence_records(occid='identifier')
    # provider = request.args.get('provider', default = None, type = str)
    icon_status = request.args.get('icon_status', default = 'active', type = str)
    stream = request.args.get('stream', default = 'True', type = str)
    response = BadgeSvc.get_icon(
        provider=provider, icon_status=icon_status, stream=stream, app_path=app.root_path)
    return response


# .....................................................................................
@bp.route("/name/")
def name_endpoint():
    """Show the providers available for the name service."""
    name_arg = request.args.get('namestr', default = None, type = str)
    provider = request.args.get('provider', default = None, type = str)
    is_accepted = request.args.get('is_accepted', default = 'True', type = str)
    gbif_parse = request.args.get('gbif_parse', default = 'True', type = str)
    gbif_count = request.args.get('gbif_count', default = 'True', type = str)
    kingdom = request.args.get('kingdom', default = None, type = str)
    if name_arg is None:
        response = NameSvc.get_endpoint()
    else:
        response = NameSvc.get_name_records(
            namestr=name_arg, provider=provider, is_accepted=is_accepted,
            gbif_parse=gbif_parse, gbif_count=gbif_count)

    return response

# .....................................................................................
@bp.route('/name/<string:namestr>', methods=['GET'])
def name_get(namestr):
    """Get a taxonomic name record from available providers.

    Args:
        namestr (str): A scientific name to search for among taxonomic providers.

    Returns:
        dict: A dictionary of metadata for the requested record.
    """
    # response = OccurrenceSvc.get_occurrence_records(occid='identifier')
    provider = request.args.get('provider', default = None, type = str)
    is_accepted = request.args.get('is_accepted', default = 'True', type = str)
    gbif_parse = request.args.get('gbif_parse', default = 'True', type = str)
    gbif_count = request.args.get('gbif_count', default = 'True', type = str)
    kingdom = request.args.get('kingdom', default = None, type = str)
    response = NameSvc.get_name_records(
        namestr=namestr, provider=provider, is_accepted=is_accepted,
        gbif_parse=gbif_parse, gbif_count=gbif_count)
    return response

# .....................................................................................
@bp.route("/occ/")
def occ_endpoint():
    """Show the providers available for the occurrence service."""
    occ_arg = request.args.get('occid', default = None, type = str)
    provider = request.args.get('provider', default = None, type = str)
    gbif_dataset_key = request.args.get('gbif_dataset_key', default = None, type = str)
    count_only = request.args.get('count_only', default = 'False', type = str)
    if occ_arg is None and gbif_dataset_key is None:
        response = OccurrenceSvc.get_endpoint()
    else:
        response = OccurrenceSvc.get_occurrence_records(
            occid=occ_arg, provider=provider, gbif_dataset_key=gbif_dataset_key,
            count_only=count_only)
    return response

# .....................................................................................
@bp.route('/occ/<string:identifier>', methods=['GET'])
def occ_get(identifier):
    """Get an occurrence record from available providers.

    Args:
        identifier (str): An occurrence identifier to search from occurrence providers.

    Returns:
        dict: A dictionary of metadata for the requested record.
    """
    provider = request.args.get('provider', default = None, type = str)
    gbif_dataset_key = request.args.get('gbif_dataset_key', default = None, type = str)
    count_only = request.args.get('count_only', default = 'False', type = str)
    response = OccurrenceSvc.get_occurrence_records(
        occid=identifier, provider=provider, gbif_dataset_key=gbif_dataset_key,
        count_only=count_only)
    return response


# .....................................................................................
@bp.route("/stats/")
def stats_get():
    response = StatsSvc.get_stats()
    return response


# .....................................................................................
@bp.route("/frontend/")
def frontend_get():
    occid = request.args.get('occid', default = None, type = str)
    namestr = request.args.get('namestr', default = None, type = str)
    response = FrontendSvc.get_frontend(occid=occid, namestr=namestr)
    return response


# .....................................................................................
# .....................................................................................
if __name__ == "__main__":
    bp.run(debug=True)
