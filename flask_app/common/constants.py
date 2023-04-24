"""Constants common to the Specify Network Broker and Analyst API services."""
URL_ESCAPES = [[" ", r"\%20"], [",", r"\%2C"]]
ENCODING = "utf-8"

STATIC_DIR = "../../sppy/frontend/static"
ICON_DIR = "{}/icon".format(STATIC_DIR)

TEMPLATE_DIR = "../templates"
SCHEMA_DIR = "{}/schema".format(STATIC_DIR)
SCHEMA_FNAME = "open_api.yaml"
