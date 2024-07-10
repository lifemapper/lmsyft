"""Class for the Specify Network Analyst `summary` API service."""
from http import HTTPStatus
import os
from werkzeug.exceptions import BadRequest

from flask_app.common.s2n_type import APIService, AnalystOutput
from flask_app.analyst.base import _AnalystService, INPUT_DATA_PATH, LOCAL_PATH

from sppy.tools.s2n.constants import (Summaries, SUMMARY_TABLE_TYPES)
from sppy.tools.s2n.summary_matrix import SummaryMatrix
from sppy.aws.aws_constants import PROJ_BUCKET, SUMMARY_FOLDER
from sppy.aws.aws_tools import get_current_datadate_str, download_from_s3
from sppy.tools.util.utils import (
    add_errinfo, combine_errinfo, get_traceback, prettify_object)


# .............................................................................
class SummarySvc(_AnalystService):
    """Specify Network API service for retrieving taxonomic information."""
    SERVICE_TYPE = APIService.Summary
    ORDERED_FIELDNAMES = []

    # ...............................................
    @classmethod
    def get_measurements(
            cls, summary_type=None, summary_key=None):
        """Return occurrence counts and other data measurements for an identifier.

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
            try:
                stat_dict, errors = cls._get_all_measurements(
                    good_params["summary_type"], good_params["summary_key"])
            except Exception:
                errinfo = add_errinfo(errinfo, "error", get_traceback())
            else:
                errinfo = combine_errinfo(errinfo, errors)

        # Assemble
        full_out = AnalystOutput(
            cls.SERVICE_TYPE["name"], description=cls.SERVICE_TYPE["description"],
            output=stat_dict, errors=errinfo)

        return full_out.response

    # ...............................................
    @classmethod
    def _init_summary_matrix(cls, summary_type):
        errinfo = {}
        mtx = None
        data_datestr = get_current_datadate_str()
        if summary_type == "dataset":
            mtx_table_type = SUMMARY_TABLE_TYPES.DATASET_SPECIES_SUMMARY
        else:
            mtx_table_type = SUMMARY_TABLE_TYPES.SPECIES_DATASET_SUMMARY

        table = Summaries.get_table(mtx_table_type, data_datestr)
        zip_fname = f"{table['fname']}.zip"
        zip_filename = os.path.join(INPUT_DATA_PATH, zip_fname)

        if not os.path.exists(zip_filename):
            errinfo["error"] = [f"Missing input data file {zip_filename}"]
            # TODO: download this as part of AWS workflow
            _ = download_from_s3(
                PROJ_BUCKET, SUMMARY_FOLDER, zip_fname, local_path=LOCAL_PATH,
                overwrite=False)

        else:
            # Will only extract if matrix and metadata files do not exist yet
            try:
                dataframe, _table_type, _data_datestr = \
                    SummaryMatrix.uncompress_zipped_data(
                        zip_filename, local_path=LOCAL_PATH, overwrite=False)
            except Exception as e:
                errinfo = add_errinfo(errinfo, "error", str(e))
            # Create
            else:
                sp_mtx = SummaryMatrix(
                    dataframe, mtx_table_type, data_datestr, category=row_categ,
                    column_category=col_categ, logger=None)
        return sp_mtx, errinfo

    # ...............................................
    @classmethod
    def _get_all_measurements(cls, summary_type, summary_key):
        stat_dict = {}
        spnet_mtx, errinfo = cls._init_summary_matrix(summary_type)
        if spnet_mtx is not None:
            if summary_key is not None:
                try:
                    stat_dict = spnet_mtx.get_column_stats(
                        dataset_key, agg_type=agg_type)
                except Exception:
                    errinfo = {
                        "error": [HTTPStatus.INTERNAL_SERVER_ERROR, get_traceback()]
                    }

            if stat_dict is not None:
                stat_dict[f"{summary_type.capitalize()} Statistics"] = stat_dict

        return stat_dict, errinfo

# .............................................................................
if __name__ == "__main__":
    dataset_key = "3e2d26d9-2776-4bec-bdc7-bab3842ffb6b"
    species_key = "11378306 Phaneroptera laticerca"
    agg_type = None

    svc = SummarySvc()
    response = svc.get_endpoint()
    print(prettify_object(response))

    response = svc.get_measurements(summary_type="dataset", summary_key=dataset_key)
    print(prettify_object(response))

    response = svc.get_measurements(summary_type="species", summary_key=species_key)
    print(prettify_object(response))
"""
from flask_app.analyst.dataset import *

dataset_key = "3e2d26d9-2776-4bec-bdc7-bab3842ffb6b"
species_key = "11378306 Phaneroptera laticerca"

svc = SummarySvc()
response = svc.get_endpoint()
print(prettify_object(response))

response = svc.get_measurements(summary_type="dataset", summary_key=dataset_key)
print(prettify_object(response))

response = svc.get_measurements(summary_type="species", summary_key=species_key)
print(prettify_object(response))

"""
