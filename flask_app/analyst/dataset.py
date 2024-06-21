"""Class for the Specify Network Name API service."""
from http import HTTPStatus
from logging import ERROR, INFO, WARNING
import os
from werkzeug.exceptions import BadRequest

from flask_app.common.s2n_type import APIService, AnalystOutput
from flask_app.analyst.base import _AnalystService

from sppy.aws.aws_constants import (
    PROJ_BUCKET, Summaries, SUMMARY_FOLDER, SUMMARY_TABLE_TYPES
)
from sppy.aws.aggregate_matrix import SparseMatrix
from sppy.aws.aws_tools import (
    download_from_s3, get_current_datadate_str, get_today_str
)
from sppy.tools.s2n.utils import (
    add_errinfo, combine_errinfo, get_traceback, prettify_object)
from sppy.tools.util.logtools import Logger

LOCAL_PATH = os.environ["WORKING_DIRECTORY"]
INPUT_DATA_PATH = os.environ["AWS_DATA_DIRECTORY"]
LOG_PATH = os.path.join(LOCAL_PATH, "log")

# .............................................................................
class DatasetSvc(_AnalystService):
    """Specify Network API service for retrieving taxonomic information."""
    SERVICE_TYPE = APIService.Dataset
    ORDERED_FIELDNAMES = []

    # ...............................................
    @classmethod
    def get_dataset_counts(
            cls, dataset_key=None, species_key=None, aggregate_by=None, stat_type=None):
        """Return occurrence and species counts for dataset/species identifiers.

        Args:
            dataset_key: URL parameter for unique GBIF identifier of dataset.
            species_key: URL parameter for unique GBIF identifier of accepted taxon
                concatenated with the species name.
            aggregate_by: URL parameter for measuring counts of occurrences, datasets,
                or species.
            stat_type: URL parameter for "describe" or "compare" indicating whether to
                describe the
                  * total for dataset or species count or
                  * min and max count and dataset or species for occurrence count or
                compare the above to the min/max/mean/median for all datasets

        Returns:
            full_output (flask_app.common.s2n_type.AnalystOutput): including a
                dictionary (JSON) of a record containing keywords with values.
        """
        if dataset_key is None and species_key is None:
            return cls.get_endpoint()

        stat_dict = {}
        try:
            good_params, errinfo = cls._standardize_params(
                dataset_key=dataset_key, species_key=species_key,
                aggregate_by=aggregate_by, stat_type=stat_type)
        except BadRequest as e:
            errinfo = {"error": [e.description]}
        except Exception as e:
            errinfo = {"error": [get_traceback()]}

        else:
            try:
                stat_dict, errors = cls._get_speciesxdataset_counts(
                    good_params["dataset_key"], good_params["species_key"],
                    good_params["aggregate_by"], good_params["stat_type"])
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
    def _init_sparse_matrix(cls):
        errinfo = {}
        sp_mtx = None
        data_datestr = get_current_datadate_str()
        mtx_table_type = SUMMARY_TABLE_TYPES.SPECIES_DATASET_MATRIX
        table = Summaries.get_table(mtx_table_type, data_datestr)
        zip_fname = f"{table['fname']}.zip"
        zip_filename = os.path.join(INPUT_DATA_PATH, zip_fname)
        if not os.path.exists(zip_filename):
            errinfo["error"] = [f"Missing input data file {zip_filename}"]
        # Download matrix if file does not exist
        # try:
        #     zip_filename = download_from_s3(
        #         PROJ_BUCKET, SUMMARY_FOLDER, zip_fname, local_path=LOCAL_PATH,
        #         overwrite=False)
        # except Exception as e:
        #     errinfo["error"] = [str(e)]
        else:
            # Only extract if files do not exist
            try:
                sparse_coo, row_categ, col_categ, table_type, _data_datestr = \
                    SparseMatrix.uncompress_zipped_sparsematrix(
                        zip_filename, local_path=LOCAL_PATH, overwrite=False)
            except Exception as e:
                errinfo = add_errinfo(errinfo, "error", e.description)
            # Create
            else:
                script_name = os.path.splitext(os.path.basename(__file__))[0]
                todaystr = get_today_str()
                log_name = f"{script_name}_{todaystr}"
                # Create logger if we get this far
                logger = Logger(
                    log_name, log_path=LOG_PATH, log_console=True, log_level=INFO)
                sp_mtx = SparseMatrix(
                    sparse_coo, mtx_table_type, data_datestr, row_category=row_categ,
                    column_category=col_categ, logger=logger)
        return sp_mtx, errinfo

    # ...............................................
    @classmethod
    def _get_speciesxdataset_counts(
            cls, dataset_key, species_key, aggregate_by, stat_type):
        stat_dict = {}
        ds_stat_dict = sp_stat_dict = None
        spnet_mtx, errinfo = cls._init_sparse_matrix()
        if spnet_mtx is not None:
            if aggregate_by is None:
                agg_type = aggregate_by
            elif aggregate_by == "occurrences":
                agg_type = "value"
            elif aggregate_by in ("species", "dataset"):
                agg_type = "axis"

            if dataset_key is not None:
                if stat_type == "describe":
                    try:
                        ds_stat_dict = spnet_mtx.get_column_stats(
                            dataset_key, agg_type=agg_type)
                    except Exception:
                        errinfo = {
                            "error": [HTTPStatus.INTERNAL_SERVER_ERROR, get_traceback()]
                        }
                else:
                    # if compare
                    try:
                        ds_stat_dict = spnet_mtx.compare_column_to_others(
                            dataset_key, agg_type=agg_type)
                    except Exception:
                        errinfo = add_errinfo(
                            errinfo, "error",
                            [HTTPStatus.INTERNAL_SERVER_ERROR, get_traceback()])

            if species_key is not None:
                if stat_type == "describe":
                    try:
                        sp_stat_dict = spnet_mtx.get_row_stats(species_key)
                    except Exception:
                        errinfo = add_errinfo(
                            errinfo, "error",
                            [HTTPStatus.INTERNAL_SERVER_ERROR, get_traceback()])
                else:
                    # if compare
                    try:
                        sp_stat_dict = spnet_mtx.compare_row_to_others(
                            species_key, agg_type=agg_type)
                    except Exception:
                        errinfo = add_errinfo(
                            errinfo, "error",
                            [HTTPStatus.INTERNAL_SERVER_ERROR, get_traceback()])

            if ds_stat_dict is not None:
                stat_dict["Dataset Statistics"] = ds_stat_dict
            if sp_stat_dict is not None:
                stat_dict["Species Statistics"] = sp_stat_dict

        return stat_dict, errinfo


# .............................................................................
if __name__ == "__main__":
    dataset_key = "3e2d26d9-2776-4bec-bdc7-bab3842ffb6b"
    key_species = "11378306 Phaneroptera laticerca"
    agg_type = None

    svc = DatasetSvc()
    response = svc.get_endpoint()
    print(prettify_object(response))

    response = svc.get_dataset_counts(
        dataset_key=dataset_key, species_key=key_species, stat_type="compare",
        aggregate_by=agg_type)
    print(prettify_object(response))

"""
from flask_app.analyst.dataset import *

dataset_key = "3e2d26d9-2776-4bec-bdc7-bab3842ffb6b"
key_species = "11378306 Phaneroptera laticerca"
agg_type = None

svc = DatasetSvc()
response = svc.get_endpoint()
print(prettify_object(response))

response = svc.get_dataset_counts(
    dataset_key=dataset_key, species_key=key_species, stat_type="compare",
    aggregate_by=agg_type)
print(prettify_object(response))

# ...................................

data_datestr = get_current_datadate_str()
mtx_table_type = SUMMARY_TABLE_TYPES.SPECIES_DATASET_MATRIX
local_path = "/tmp"
table = Summaries.get_table(mtx_table_type, data_datestr)
zip_fname = f"{table['fname']}.zip"
# Only download if file does not exist
zip_filename = download_from_s3(
    PROJ_BUCKET, SUMMARY_FOLDER, zip_fname, local_path=local_path,
    overwrite=False)
# Only extract if files do not exist
sparse_coo, row_categ, col_categ, table_type, _data_datestr = \
    SparseMatrix.uncompress_zipped_sparsematrix(
        zip_filename, local_path=local_path, overwrite=False)
# Create
sp_mtx = SparseMatrix(
    sparse_coo, mtx_table_type, data_datestr, row_category=row_categ,
    column_category=col_categ, logger=None)

"""
