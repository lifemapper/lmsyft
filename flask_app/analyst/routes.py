"""URL Routes for the Specify Network API services."""
from flask import Blueprint, Flask, render_template, request

from flask_app.analyst.compare import CompareSvc
from flask_app.analyst.describe import DescribeSvc
from flask_app.analyst.rank import RankSvc
from flask_app.common.constants import (
    STATIC_DIR, TEMPLATE_DIR)
from flask_app.common.s2n_type import APIEndpoint

analyst_blueprint = Blueprint(
    "analyst", __name__, template_folder=TEMPLATE_DIR, static_folder=STATIC_DIR,
    static_url_path="/static")

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False
app.register_blueprint(analyst_blueprint)


# .....................................................................................
@app.route('/')
def index():
    """Render template for the base URL.

    Returns:
        Rendered template for a browser response.
    """
    return render_template("analyst.index.html")


# .....................................................................................
@app.route("/api/v1/", methods=["GET"])
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


# # ..........................
# @app.route("/api/v1/schema")
# def display_raw_schema():
#     """Show the schema XML.
#
#     Returns:
#         schema: the schema for the Specify Network.
#     """
#     fname = os.path.join(SCHEMA_DIR, SCHEMA_ANALYST_FNAME)
#     with open(fname, "r") as f:
#         schema = f.read()
#     return schema
#
#
# # ..........................
# @app.route("/api/v1/swaggerui")
# def swagger_ui():
#     """Show the swagger UI to the schema.
#
#     Returns:
#         a webpage UI of the Specify Network schema.
#     """
#     return render_template("swagger_ui.html")


# .....................................................................................
@app.route("/api/v1/describe/")
def describe_endpoint():
    """Get the statistics for descriptive measures of some dimension of occurrence data.

    Returns:
        response: A flask_app.analyst API response object containing the dataset
            API response.
    """
    type_arg = request.args.get("summary_type", default=None, type=str)
    key_arg = request.args.get("summary_key", default=None, type=str)
    if type_arg is None:
        response = DescribeSvc.get_endpoint()
    else:
        response = DescribeSvc.get_measures(summary_type=type_arg, summary_key=key_arg)
    return response


# .....................................................................................
@app.route("/api/v1/compare/")
def compare_endpoint():
    """Comare the statistics for descriptive measures of some dimension of occurrence data.

    Returns:
        response: A flask_app.analyst API response object containing the dataset
            API response.
    """
    type_arg = request.args.get("summary_type", default=None, type=str)
    key_arg = request.args.get("summary_key", default=None, type=str)
    if type_arg is None:
        response = CompareSvc.get_endpoint()
    else:
        response = CompareSvc.compare_measures(
            summary_type=type_arg, summary_key=key_arg)
    return response


# .....................................................................................
@app.route("/api/v1/rank/")
def rank_endpoint():
    """Get the available counts.

    Returns:
        response: A flask_app.analyst API response object containing the count
            API response.
    """
    summary_type_arg = request.args.get("summary_type", default=None, type=str)
    rank_by_arg = request.args.get("rank_by", default=None, type=str)
    order_arg = request.args.get("order", default=None, type=str)
    limit_arg = request.args.get("limit", default=10, type=int)
    print(
        f"*** type_arg={summary_type_arg}, rank_by_arg={rank_by_arg}, order_arg={order_arg}, "
        f"limit_arg={limit_arg} ***")
    # if coll_arg is None and org_arg is None:
    if summary_type_arg is None:
        response = RankSvc.get_endpoint()
    else:
        response = RankSvc.rank_counts(
            summary_type_arg, rank_by_arg, order=order_arg, limit=limit_arg)
    return response


# .....................................................................................
# .....................................................................................
if __name__ == "__main__":
    app.run(debug=True)
