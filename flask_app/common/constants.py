"""Constants common to the Specify Network Broker and Analyst API services."""
URL_ESCAPES = [[" ", r"\%20"], [",", r"\%2C"]]

# Used in broker, so relative to the flask_app/broker or analyst directories
STATIC_DIR = "../../sppy/frontend/static"
ICON_DIR = f"{STATIC_DIR}/icon"
SCHEMA_DIR = f"{STATIC_DIR}/schema"

TEMPLATE_DIR = "../templates"
SCHEMA_ANALYST_FNAME = "open_api.analyst.yaml"
SCHEMA_BROKER_FNAME = "open_api.broker.yaml"
