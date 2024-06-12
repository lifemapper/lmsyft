"""Class for the Specify Network Name API service."""
from http import HTTPStatus
from werkzeug.exceptions import BadRequest

from flask_app.common.s2n_type import APIService, AnalystOutput
from flask_app.analyst.base import _AnalystService

from sppy.aws.aws_constants import (
    PROJ_BUCKET, Summaries, SUMMARY_FOLDER, SUMMARY_TABLE_TYPES, REGION
)
from sppy.aws.aggregate_matrix import SparseMatrix
from sppy.aws.aws_tools import (
    download_from_s3, get_current_datadate_str
)
from sppy.tools.s2n.utils import (combine_errinfo, get_traceback, prettify_object)


# .............................................................................
class DatasetSvc(_AnalystService):
    """Specify Network API service for retrieving taxonomic information."""
    SERVICE_TYPE = APIService.Dataset
    ORDERED_FIELDNAMES = []

    # ...............................................
    @classmethod
    def get_dataset_counts(cls, dataset_key=None, aggregate_by=None, stat_type=None):
        """Return occurrence and species counts for dataset/organization identifiers.

        Args:
            dataset_key: URL parameter for unique GBIF identifier of dataset.
            aggregate_by: URL parameter for measuring counts of occurrences or species.
            stat_type: URL parameter for "describe" or "compare" indicating whether to
                describe the
                  * total for species count or
                  * min and max count and species for occurrence count or
                compare the above to the min/max/mean/median for all datasets

        Returns:
            full_output (flask_app.common.s2n_type.AnalystOutput): including a
                dictionary (JSON) of a record containing keywords with values.
        """
        if dataset_key is None and aggregate_by is None:
            return cls.get_endpoint()

        records = []
        try:
            good_params, errinfo = cls._standardize_params(
                dataset_key=dataset_key, aggregate_by=aggregate_by, stat_type=stat_type)

        except BadRequest as e:
            errinfo = {"error": [e.description]}

        else:
            try:
                records, errors = cls._get_dataset_counts(
                    good_params["dataset_key"], good_params["aggregate_by"],
                    good_params["stat_type"])
            except Exception:
                errors = {"error": [get_traceback()]}

            # Combine errors from success or failure
            errinfo = combine_errinfo(errinfo, errors)

        # Assemble
        full_out = AnalystOutput(
            cls.SERVICE_TYPE["name"], description=cls.SERVICE_TYPE["description"],
            records=records, errors=errinfo)

        return full_out.response

    # ...............................................
    @classmethod
    def _init_sparse_matrix(cls):
        records = []
        errors = {}
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
        sparse_coo, row_categ, col_categ, table_type = \
            SparseMatrix.uncompress_zipped_sparsematrix(
                zip_filename, local_path=local_path, overwrite=False)
        # Create
        sp_mtx = SparseMatrix(
            sparse_coo, mtx_table_type, row_category=row_categ,
            column_category=col_categ, logger=None)
        return sp_mtx

    # ...............................................
    @classmethod
    def _get_dataset_counts(cls, dataset_key, aggregate_by, stat_type):
        records = []
        errors = {}
        spnet_mtx = cls._init_sparse_matrix()
        if aggregate_by == "occurrences":
            agg_type = "value"
        elif aggregate_by == "species":
            agg_type = "axis"

        if stat_type == "describe":
            try:
                stats = spnet_mtx.get_column_stats(dataset_key, agg_type=agg_type)
            except Exception:
                traceback = get_traceback()
                errors["error"] = [HTTPStatus.INTERNAL_SERVER_ERROR, traceback]
        else:
            # if compare
            try:
                records = spnet_mtx.get_column_stats(dataset_key, agg_type=agg_type)
            except Exception:
                traceback = get_traceback()
                errors["error"] = [HTTPStatus.INTERNAL_SERVER_ERROR, traceback]

        return records, errors


# .............................................................................
if __name__ == "__main__":
    dataset_key = "0000e36f-d0e9-46b0-aa23-cc1980f00515"

    svc = DatasetSvc()
    response = svc.get_endpoint()
    AnalystOutput.print_output(response, do_print_rec=True)
    # print(response)
    response = svc.get_dataset_counts(
        dataset_key=dataset_key, aggregate_by="species", stat_type=None)
    print(prettify_object(response))
    # AnalystOutput.print_output(response, do_print_rec=True)
    # print(response)
