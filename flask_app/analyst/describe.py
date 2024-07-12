"""Class for the Specify Network Name API service."""
from http import HTTPStatus
import os
from werkzeug.exceptions import BadRequest

from flask_app.common.s2n_type import APIService, AnalystOutput
from flask_app.analyst.base import _AnalystService, INPUT_DATA_PATH

from sppy.aws.aws_constants import PROJ_BUCKET, SUMMARY_FOLDER
from sppy.aws.aws_tools import download_from_s3, get_current_datadate_str

from sppy.tools.s2n.constants import (Summaries, SUMMARY_TABLE_TYPES)
from sppy.tools.s2n.sparse_matrix import SparseMatrix
from sppy.tools.util.utils import (
    add_errinfo, combine_errinfo, get_traceback, prettify_object)


# .............................................................................
class DescribeSvc(_AnalystService):
    """Specify Network API service for information about aggregated occurrence data."""
    SERVICE_TYPE = APIService.Describe
    ORDERED_FIELDNAMES = []

    # ...............................................
    @classmethod
    def get_measures(cls, summary_type=None, summary_key=None):
        """Return descriptive measurements for one or all dataset/species.

        Args:
            summary_type: data dimension for summary, ("species" or "dataset")
            summary_key: unique identifier for the data dimension being examined.  If
                None, return stats for all identifiers.

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
            if good_params["summary_type"] is not None:
                try:
                    stat_dict, errors = cls._get_measures(
                        good_params["summary_type"], good_params["summary_key"])
                except Exception:
                    errinfo = add_errinfo(errinfo, "error", get_traceback())
                else:
                    errinfo = combine_errinfo(errinfo, errors)
            else:
                options = cls.SERVICE_TYPE["params"]["summary_type"]["options"]
                errinfo = {
                    "error": [f"Must provide summary_type key with value in {options}"]}

        # Assemble
        full_out = AnalystOutput(
            cls.SERVICE_TYPE["name"], description=cls.SERVICE_TYPE["description"],
            output=stat_dict, errors=errinfo)

        return full_out.response

    # ...............................................
    @classmethod
    def _init_sparse_matrix(cls):
        errinfo = {}
        sp_mtx = None
        data_datestr = get_current_datadate_str()
        mtx_table_type = SUMMARY_TABLE_TYPES.SPECIES_DATASET_MATRIX
        table = Summaries.get_table(mtx_table_type, data_datestr)
        zip_fname = f"{table['fname']}.zip"
        zip_filename = os.path.join(INPUT_DATA_PATH, zip_fname)
        # Download if necessary
        if not os.path.exists(zip_filename):
            errinfo["info"] = [f"Download input file {zip_filename}"]
            # Download if file does not exist
            _zip_filename = download_from_s3(
                PROJ_BUCKET, SUMMARY_FOLDER, zip_fname, local_path=INPUT_DATA_PATH,
                overwrite=False)

        # Extract if matrix and metadata files do not exist, create objects
        try:
            sparse_coo, row_categ, col_categ, table_type, _data_datestr = \
                SparseMatrix.uncompress_zipped_data(
                    zip_filename, local_path=INPUT_DATA_PATH, overwrite=False)
        except Exception as e:
            errinfo = add_errinfo(errinfo, "error", str(e))
        # Create
        else:
            sp_mtx = SparseMatrix(
                sparse_coo, mtx_table_type, data_datestr, row_category=row_categ,
                column_category=col_categ, logger=None)
        return sp_mtx, errinfo

    # ...............................................
    @classmethod
    def _get_measures(cls, summary_type, summary_key):
        stat_dict = {}
        spnet_mtx, errinfo = cls._init_sparse_matrix()
        if spnet_mtx is not None:
            if summary_type == "dataset":
                try:
                    stat_dict = spnet_mtx.get_column_stats(summary_key)
                except IndexError:
                    errinfo = {
                        "error": [f"Key {summary_key} does not exist in {summary_type}"]
                    }
                except Exception:
                    errinfo = {
                        "error": [HTTPStatus.INTERNAL_SERVER_ERROR, get_traceback()]
                    }
            # other valid option is "species"
            else:
                try:
                    stat_dict = spnet_mtx.get_row_stats(summary_key)
                except IndexError:
                    errinfo = {
                        "error": [f"Key {summary_key} does not exist in {summary_type}"]
                    }
                except Exception:
                    errinfo = add_errinfo(
                        errinfo, "error",
                        [HTTPStatus.INTERNAL_SERVER_ERROR, get_traceback()])

        out_dict = {f"{summary_type.capitalize()} Statistics":  stat_dict}
        return out_dict, errinfo

# .............................................................................
if __name__ == "__main__":
    dataset_key = "3e2d26d9-2776-4bec-bdc7-bab3842ffb6b"
    species_key = "8277078 Carcharodus alceae"

    print("**** Endpoint ****")
    svc = DescribeSvc()
    response = svc.get_endpoint()
    print(prettify_object(response))

    print("**** dataset_key ****")
    response = svc.get_measures(summary_type="dataset", summary_key=dataset_key)
    print(prettify_object(response))

    print("**** species_key ****")
    response = svc.get_measures(summary_type="species", summary_key=species_key)
    print(prettify_object(response))

    print("**** all datasets ****")
    response = svc.get_measures(summary_type="dataset")
    print(prettify_object(response))

    print("**** all species ****")
    response = svc.get_measures(summary_type="species")
    print(prettify_object(response))

    print("**** no type ****")
    response = svc.get_measures(summary_key=dataset_key)
    print(prettify_object(response))

    print("**** wrong type ****")
    response = svc.get_measures(summary_type="dataset", summary_key=species_key)
    print(prettify_object(response))
"""
from flask_app.analyst.describe import *

dataset_key = "3e2d26d9-2776-4bec-bdc7-bab3842ffb6b"
key_species = "11378306 Phaneroptera laticerca"

svc = DescribeSvc()
response = svc.get_endpoint()
print(prettify_object(response))

response = svc.get_measures(summary_type="dataset", summary_key=dataset_key)
print(prettify_object(response))

response = svc.get_measures(summary_type="species", summary_key=key_species)
print(prettify_object(response))

response = svc.get_measures(summary_type="dataset")
print(prettify_object(response))

response = svc.get_measures(summary_type="species")
print(prettify_object(response))
"""
