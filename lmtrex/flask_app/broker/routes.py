from flask import Flask, jsonify, request, render_template, url_for
import json
import os

from lmtrex.common.lmconstants import (
    TEMPLATE_DIR, STATIC_DIR, SCHEMA_DIR, SCHEMA_FNAME)
from lmtrex.flask_app.broker.address import AddressSvc
from lmtrex.flask_app.broker.badge import BadgeSvc
from lmtrex.flask_app.broker.frontend import FrontendSvc
from lmtrex.flask_app.broker.map import MapSvc
from lmtrex.flask_app.broker.name import NameSvc
from lmtrex.flask_app.broker.occ import OccurrenceSvc
from lmtrex.flask_app.broker.resolve import ResolveSvc
from lmtrex.flask_app.broker.stats import StatsSvc

# downloadable from <baseurl>/static/schema/open_api.yaml

app = Flask(
    __name__, template_folder=TEMPLATE_DIR, static_folder=STATIC_DIR, static_url_path='/static')
app.config['JSON_SORT_KEYS'] = False


# .....................................................................................
@app.route('/')
def index():
    return render_template('index.html')

# # ..........................
# @app.route('/api/v1/schema')
# def download_schema():
#     return "<a href={}>API schema</a>".format(
#         url_for('static', filename='{}'.format(SCHEMA_FILE)))

# ..........................
@app.route('/api/v1/schema')
def display_raw_schema():
    fname = os.path.join(SCHEMA_DIR, SCHEMA_FNAME)
    # fname = '/Users/aimeestewart/git/lmtrex/lmtrex/static/schema/open_api.yaml'
    with open(fname, 'r') as f:
        schema = f.read()
    return schema

# ..........................
@app.route('/api/v1/swaggerui')
def swagger_ui():
    return render_template('swagger_ui.html')

# .....................................................................................
@app.route("/api/v1/address")
def address_endpoint():
    response = AddressSvc.get_endpoint()
    return response

# .....................................................................................
@app.route("/api/v1/badge/")
def badge_endpoint():
    response = BadgeSvc.get_endpoint()
    return response

# .....................................................................................
@app.route('/api/v1/badge/<string:provider>', methods=['GET'])
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
@app.route("/api/v1/map/")
def map_endpoint():
    name_arg = request.args.get('namestr', default = None, type = str)
    provider = request.args.get('provider', default = None, type = str)
    is_accepted = request.args.get('is_accepted', default = 'True', type = str)
    gbif_parse = request.args.get('gbif_parse', default = 'True', type = str)
    scenariocode = request.args.get('scenariocode', default = None, type = str)
    color = request.args.get('color', default = 'red', type = str)
    if name_arg is None:
        response = MapSvc.get_endpoint()
    else:
        response = NameSvc.get_name_records(
            namestr=name_arg, provider=provider, is_accepted=is_accepted, gbif_parse=gbif_parse, 
            scenariocode=scenariocode, color=color)
    return response

# .....................................................................................
@app.route('/api/v1/map/<string:namestr>', methods=['GET'])
def map_get(namestr):
    """Get an map layer records from available providers.

    Args:
        namestr (str): A scientific name to search for among map providers.

    Returns:
        dict: A dictionary of metadata for the requested record.
    """
    provider = request.args.get('provider', default = None, type = str)
    is_accepted = request.args.get('is_accepted', default = 'True', type = str)
    gbif_parse = request.args.get('gbif_parse', default = 'True', type = str)
    scenariocode = request.args.get('scenariocode', default = None, type = str)
    color = request.args.get('color', default = 'red', type = str)
    response = MapSvc.get_map_meta(
        namestr=namestr, provider=provider, is_accepted=is_accepted, gbif_parse=gbif_parse, 
        scenariocode=scenariocode, color=color)
    return response

# .....................................................................................
@app.route("/api/v1/name/")
def name_endpoint():
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
@app.route('/api/v1/name/<string:namestr>', methods=['GET'])
def name_get(namestr):
    """Get an taxonomic name record from available providers.

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
        namestr=namestr, provider=provider, is_accepted=is_accepted, gbif_parse=gbif_parse, 
        gbif_count=gbif_count)
    return response

# .....................................................................................
@app.route("/api/v1/occ/")
def occ_endpoint():
    occ_arg = request.args.get('occid', default = None, type = str)
    provider = request.args.get('provider', default = None, type = str)
    gbif_dataset_key = request.args.get('gbif_dataset_key', default = None, type = str)
    count_only = request.args.get('count_only', default = 'False', type = str)
    if occ_arg is None and gbif_dataset_key is None:
        response = OccurrenceSvc.get_endpoint()
    else:
        response = OccurrenceSvc.get_occurrence_records(
            occid=occ_arg, provider=provider, gbif_dataset_key=gbif_dataset_key, count_only=count_only)
    return response

# .....................................................................................
@app.route('/api/v1/occ/<string:identifier>', methods=['GET'])
def occ_get(identifier):
    """Get an occurrence record from available providers.

    Args:
        identifier (str): An occurrence identifier to search for among occurrence providers.

    Returns:
        dict: A dictionary of metadata for the requested record.
    """
    provider = request.args.get('provider', default = None, type = str)
    gbif_dataset_key = request.args.get('gbif_dataset_key', default = None, type = str)
    count_only = request.args.get('count_only', default = 'False', type = str)
    response = OccurrenceSvc.get_occurrence_records(
        occid=identifier, provider=provider, gbif_dataset_key=gbif_dataset_key, count_only=count_only)
    return response

# .....................................................................................
@app.route("/api/v1/resolve/")
def resolve_endpoint():
    response = ResolveSvc.get_endpoint()
    return response

# .....................................................................................
@app.route('/api/v1/resolve/<string:identifier>', methods=['GET'])
def resolve_get(identifier):
    """Get a Specify GUID resolution record from the Specify Resolver.
    Args:
        identifier (str): An occurrence identifier to search for among the Specify Cache of 
        registered Specify records.

    Returns:
        dict: A dictionary of metadata including a direct URL for the requested record.
    """
    # response = OccurrenceSvc.get_occurrence_records(occid='identifier')
    occ_arg = request.args.get('occid', default = None, type = str)
    if occ_arg is not None:
        identifier = occ_arg
    response = ResolveSvc.get_guid_resolution(occid=identifier)
    return response

# .....................................................................................
@app.route("/api/v1/stats/")
def stats_get():
    response = StatsSvc.get_stats()
    return response

# .....................................................................................
@app.route("/api/v1/frontend/")
def frontend_get():
    occid = request.args.get('occid', default = None, type = str)
    namestr = request.args.get('namestr', default = None, type = str)
    response = FrontendSvc.get_frontend(occid=occid, namestr=namestr)
    return response

# .....................................................................................
# .....................................................................................
if __name__ == "__main__":
    app.run(debug=True)
