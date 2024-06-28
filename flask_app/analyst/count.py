"""Class for the Specify Network Name API service."""
from http import HTTPStatus
from werkzeug.exceptions import BadRequest

from flask_app.common.s2n_type import APIService, AnalystOutput
from flask_app.analyst.base import _AnalystService

from sppy.aws.aws_constants import PROJ_BUCKET
from sppy.tools.provider.spnet import SpNetAnalyses
from sppy.tools.s2n.utils import (combine_errinfo, get_traceback, prettify_object)


# .............................................................................
class CountSvc(_AnalystService):
    """Specify Network API service for retrieving taxonomic information."""
    SERVICE_TYPE = APIService.Count
    ORDERED_FIELDNAMES = []

    # ...............................................
    @classmethod
    def get_counts(cls, dataset_key=None, pub_org_key=None):
        """Return occurrence and species counts for dataset/organization identifiers.

        Args:
            dataset_key: URL parameter for unique GBIF identifier of dataset.
            pub_org_key: URL parameter for unique GBIF identifier of
                publishingOrganization.

        Returns:
            full_output (flask_app.common.s2n_type.AnalystOutput): including records
                as a list of one list (CSV) or dictionary (JSON) of a record
                containing dataset_key,  occurrence count, and species count.
        """
        if dataset_key is None and pub_org_key is None:
            return cls.get_endpoint()

        records = []
        try:
            good_params, errinfo = cls._standardize_params(
                dataset_key=dataset_key, pub_org_key=pub_org_key)

        except BadRequest as e:
            errinfo = {"error": [e.description]}

        else:
            # Query dataset counts
            if good_params["dataset_key"] is not None:
                try:
                    records, errors = cls._get_simple_dataset_counts(
                        good_params["dataset_key"])
                except Exception:
                    errors = {"error": [get_traceback()]}

                # Combine errors from success or failure
                errinfo = combine_errinfo(errinfo, errors)

        # Assemble
        full_out = AnalystOutput(
            cls.SERVICE_TYPE["name"], description=cls.SERVICE_TYPE["description"],
            output=records, errors=errinfo)

        return full_out.response

# ...............................................
    @classmethod
    def _get_simple_dataset_counts(cls, dataset_key):
        """Get counts for datasetKey.

        Args:
            dataset_key: unique GBIF identifier for dataset of interest.

        Returns:
            a flask_app.analyst.s2n_type.AnalystOutput object with optional records as a
            list of records corresponding to occurrence and counts for the dataset.
        """
        records = []
        errors = {}
        spnet = SpNetAnalyses(PROJ_BUCKET)
        try:
            records = spnet.get_simple_dataset_counts(dataset_key)
        except Exception:
            traceback = get_traceback()
            errors["error"] = [HTTPStatus.INTERNAL_SERVER_ERROR, traceback]

        return records, errors


# .............................................................................
if __name__ == "__main__":
    dataset_key = "0000e36f-d0e9-46b0-aa23-cc1980f00515"

    svc = CountSvc()
    response = svc.get_endpoint()
    print(prettify_object(response))
    response = svc.get_counts(dataset_key=dataset_key, pub_org_key=None)
    print(prettify_object(response))

"""
from flask_app.analyst.count import *

dataset_key = "0000e36f-d0e9-46b0-aa23-cc1980f00515"
dataset_key = "0000e36f-d0e9-46b0-aa23-cc1980f00515"

svc = CountSvc()
response = svc.get_endpoint()
print(prettify_object(response))
response = svc.get_counts(dataset_key=dataset_key, pub_org_key=None)
print(prettify_object(response))

# ...............................................
from sppy.tools.provider.spnet import *
bucket = PROJ_BUCKET
arn = f"arn:aws:s3:::{bucket}"
uri = f"s3://{bucket}/summary/"

spnet = SpNetAnalyses(PROJ_BUCKET)
records = spnet.get_simple_dataset_counts(dataset_key)

self = spnet

table = self._summary_tables["dataset_counts"]
query_str = (
    f"SELECT * FROM s3object s WHERE s.{table['key_fld']} = '{dataset_key}'"
)
format="JSON"
records = self._query_summary_table(table, query_str, format)

# ...................................................
rec_table = table


meta_table = self._summary_tables["dataset_meta"]
meta_fields = meta_table["fields"]
meta_key_fld = meta_table["key_fld"]
meta_key_idx = meta_fields.index(meta_key_fld)
meta_fields.pop(meta_key_idx)
qry_flds = ", ".join(meta_fields)

# Record info
rec_fields = rec_table["fields"]
rec_key_fld = rec_table["key_fld"]
rec_key_idx = rec_fields.index(rec_key_fld)

rec = records[0]

# Get datasetkey by field or position
if format == "JSON":
    val = rec[rec_key_fld]
else:
    val = rec[rec_key_idx]

query_str = (
    f"SELECT {qry_flds} FROM s3object s WHERE s.{meta_key_fld} = '{val}'"
)
# Returns empty list or list of 1 record
meta_recs = self._query_summary_table(meta_table, query_str, format)
try:
    meta = meta_recs[0]
except IndexError:
    # Add placeholders for empty values, no entries for dictionary
    if format == "CSV":
        rec.extend(["" for _ in qry_flds])
else:
    if format == "JSON":
        rec.update(meta)
    else:
        rec.extend(meta)

"""
