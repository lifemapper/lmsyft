"""Class for the Specify Network Name API service."""
from http import HTTPStatus
from werkzeug.exceptions import BadRequest

from flask_app.common.s2n_type import APIService, AnalystOutput
from flask_app.analyst.base import _AnalystService

from sppy.common.util import (
    add_errinfo, combine_errinfo, get_traceback, prettify_object
)


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
                if good_params["summary_key"] is not None:
                    try:
                        stat_dict, errors = cls._get_comparative_measures(
                            good_params["summary_type"], good_params["summary_key"])
                    except Exception:
                        errors = {"error": [get_traceback()]}
                else:
                    errors = {
                        "error":
                            ["Parameter `summary_key` is required for compare API."]
                    }
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
        out_dict = {}
        spnet_mtx, errinfo = cls._init_sparse_matrix()
        if spnet_mtx is not None:
            one_stat_dict = None
            all_stat_dict = None
            # Compare dataset
            if summary_type == "dataset":
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
                else:
                    # Get aggregated stats for all datasets
                    try:
                        all_stat_dict = spnet_mtx.get_all_column_stats()
                    except Exception:
                        errinfo = {
                            "error": [HTTPStatus.INTERNAL_SERVER_ERROR, get_traceback()]
                        }
            # other valid option is "species"
            else:
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
                else:
                    # Get aggregated stats for all species
                    try:
                        all_stat_dict = spnet_mtx.get_all_row_stats()
                    except Exception:
                        errinfo = {
                            "error": [HTTPStatus.INTERNAL_SERVER_ERROR, get_traceback()]
                        }
            if one_stat_dict is not None and all_stat_dict is not None:
                key_txt = f"{summary_type.capitalize()} Statistics"
                out_dict[f"Individual {key_txt}"] = one_stat_dict
                out_dict[f"Total {key_txt}"] = all_stat_dict
        return out_dict, errinfo


# .............................................................................
if __name__ == "__main__":
    dataset_key = "0000e36f-d0e9-46b0-aa23-cc1980f00515"
    dataset_key = "3e2d26d9-2776-4bec-bdc7-bab3842ffb6b"
    species_key = "11378306 Phaneroptera laticerca"
    svc = CompareSvc()
    response = svc.get_endpoint()
    print(prettify_object(response))
    response = svc.compare_measures(summary_type="dataset", summary_key=dataset_key)
    print(prettify_object(response))

    response = svc.compare_measures(summary_type="species")
    print(prettify_object(response))

    response = svc.compare_measures(summary_type="species", summary_key=species_key)
    print(prettify_object(response))
"""
from flask_app.analyst.count import *

"""
