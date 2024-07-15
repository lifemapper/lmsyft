"""Class for the Specify Network Name API service."""
from http import HTTPStatus
from werkzeug.exceptions import BadRequest

from flask_app.common.s2n_type import APIService, AnalystOutput
from flask_app.analyst.base import _AnalystService

from sppy.tools.util.utils import (
    add_errinfo, combine_errinfo, get_traceback, prettify_object)


# .............................................................................
class CompareSvc(_AnalystService):
    """Specify Network API service for retrieving taxonomic information."""
    SERVICE_TYPE = APIService.Compare
    ORDERED_FIELDNAMES = []

    # ...............................................
    @classmethod
    def compare_measures(cls, summary_type=None, summary_key=None):
        """Compare descriptive measurements for one identifier against all others.

        Args:
            summary_type: data dimension for summary, ("species" or "dataset")
            summary_key: unique identifier for the data dimension being examined.

        Returns:
            full_output (flask_app.common.s2n_type.AnalystOutput): including a
                dictionary (JSON) of a record containing keywords with values.
        """
        if summary_type is None and summary_key is None:
            return cls.get_endpoint()

        stat_dict = {}
        try:
            good_params, errinfo = cls._standardize_params(
                summary_type=summary_type, summary_key=summary_key)
        except BadRequest as e:
            errinfo = {"error": [e.description]}
        except Exception:
            errinfo = {"error": [get_traceback()]}

        else:
            # Query dataset counts
            if good_params["summary_type"] is not None:
                try:
                    stat_dict, errors = cls._get_comparative_measures(
                        good_params["summary_type"], good_params["summary_key"])
                except Exception:
                    errors = {"error": [get_traceback()]}

                # Combine errors from success or failure
                errinfo = combine_errinfo(errinfo, errors)

        # Assemble
        full_out = AnalystOutput(
            cls.SERVICE_TYPE["name"], description=cls.SERVICE_TYPE["description"],
            output=stat_dict, errors=errinfo)

        return full_out.response

# ...............................................
    @classmethod
    def _get_comparative_measures(cls, summary_type, summary_key):
        key_txt = f"{summary_type.capitalize()} Statistics"
        spnet_mtx, errinfo = cls._init_sparse_matrix()
        if spnet_mtx is not None:
            # Compare dataset
            if summary_type == "dataset":
                # Only request single dataset if summary_key is present
                if summary_key is not None:
                    try:
                        one_stat_dict = spnet_mtx.get_column_stats(summary_key)
                    except IndexError:
                        errinfo = {
                            "error": [f"Key {summary_key} does not exist in {summary_type}"]
                        }
                    except Exception:
                        errinfo = {
                            "error": [HTTPStatus.INTERNAL_SERVER_ERROR, get_traceback()]
                        }
                # Get aggregated stats for all datasets
                try:
                    all_stat_dict = spnet_mtx.get_all_column_stats()
                except Exception:
                    errinfo = {
                        "error": [HTTPStatus.INTERNAL_SERVER_ERROR, get_traceback()]
                    }
            # other valid option is "species"
            else:
                # Only request single species if summary_key is present
                if summary_key is not None:
                    try:
                        one_stat_dict = spnet_mtx.get_row_stats(summary_key)
                    except IndexError:
                        errinfo = {
                            "error": [f"Key {summary_key} does not exist in {summary_type}"]
                        }
                    except Exception:
                        errinfo = add_errinfo(
                            errinfo, "error",
                            [HTTPStatus.INTERNAL_SERVER_ERROR, get_traceback()])
                # Get aggregated stats for all species
                try:
                    all_stat_dict = spnet_mtx.get_all_row_stats()
                except Exception:
                    errinfo = {
                        "error": [HTTPStatus.INTERNAL_SERVER_ERROR, get_traceback()]
                    }

        out_dict = {f"Total {key_txt}":  all_stat_dict}
        if summary_key is not None:
            out_dict[f"Individual {key_txt}"] = one_stat_dict
        return out_dict, errinfo


# .............................................................................
if __name__ == "__main__":
    dataset_key = "0000e36f-d0e9-46b0-aa23-cc1980f00515"

    svc = CompareSvc()
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
