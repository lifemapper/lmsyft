"""Tests for the Broker flask application."""
from open_api_tools.test.full_test import full_test
from open_api_tools.common.load_schema import load_schema
import os

from flask_app.common.constants import SCHEMA_BROKER_FNAME, STATIC_DIR

schema = load_schema(os.path.join(STATIC_DIR, SCHEMA_BROKER_FNAME))


# Error message schema is defined in open_api_tools.validate.index
def after_error_occurred(*error_message):
    """Print a response on error.

    Args:
        error_message: message to print
    """
    print(error_message)


full_test(
    schema=schema,
    max_urls_per_endpoint=50,
    failed_request_limit=10,
    after_error_occurred=after_error_occurred,
)

"""
https://broker-dev.spcoco.org/api/v1/occ/?provider=gbif&gbif_dataset_key=e635240a-3cb1-4d26-ab87-57d8c7afdfdb
"""
