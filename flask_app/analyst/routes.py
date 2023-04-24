"""URL Routes for the Specify Network API services."""
from flask import Blueprint, render_template
import os

from flask_app.analyst.count import CountSvc
from flask_app.common.constants import (
    STATIC_DIR, TEMPLATE_DIR, SCHEMA_DIR, SCHEMA_FNAME)
from flask_app.common.s2n_type import APIEndpoint

# downloadable from <baseurl>/static/schema/open_api.yaml

bp = Blueprint(
    "analyst", __name__, url_prefix=APIEndpoint.analyst_root(),
    template_folder=TEMPLATE_DIR, static_folder=STATIC_DIR, static_url_path="/static")


# .....................................................................................
@bp.route("/", methods=["GET"])
def analyst_status():
    """Get services available from broker.

    Returns:
        dict: A dictionary of status information for the server.
    """
    endpoints = APIEndpoint.get_analyst_endpoints()
    system_status = "In Development"
    return {
        "num_services": len(endpoints),
        "endpoints": endpoints,
        "status": system_status
    }


# ..........................
@bp.route("/schema")
def display_raw_schema():
    """Show the schema XML.

    Returns:
        schema: the schema for the Specify Network.
    """
    fname = os.path.join(SCHEMA_DIR, SCHEMA_FNAME)
    with open(fname, "r") as f:
        schema = f.read()
    return schema


# ..........................
@bp.route("/swaggerui")
def swagger_ui():
    """Show the swagger UI to the schema.

    Returns:
        a webpage UI of the Specify Network schema.
    """
    return render_template("swagger_ui.html")


# .....................................................................................
@bp.route("/stats/")
def counts_get():
    """Get the available statistics.

    Returns:
        response: A flask_app.analyst API response object containing the count
            API response.
    """
    response = CountSvc.get_stats()
    return response


# .....................................................................................
# .....................................................................................
if __name__ == "__main__":
    bp.run(debug=True)
