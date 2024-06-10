"""Class for the Specify Network Name API service."""
from http import HTTPStatus
from werkzeug.exceptions import BadRequest

from flask_app.common.s2n_type import APIService, AnalystOutput
from flask_app.analyst.base import _AnalystService

from sppy.aws.aws_constants import PROJ_BUCKET
from sppy.aws.aggregate_matrix import SparseMatrix
from sppy.tools.provider.spnet import SpNetAnalyses
from sppy.tools.s2n.utils import (combine_errinfo, get_traceback, prettify_object)


# .............................................................................
class DatasetSvc(_AnalystService):
    """Specify Network API service for retrieving taxonomic information."""
    SERVICE_TYPE = APIService.Dataset
    ORDERED_FIELDNAMES = []

    # ...............................................
    @classmethod
    def get_dataset_counts(cls, dataset_key=None, count_by=None, out_stats=None):
        """Return occurrence and species counts for dataset/organization identifiers.

        Args:
            dataset_key: URL parameter for unique GBIF identifier of dataset.
            count_by: URL parameter for counts of "occurrence" or "species".
            out_stats: URL parameter for "describe" or "compare" indicating whether to
                describe the
                  * total for species count or
                  * min and max count and species for occurrence count or
                compare the above to the min/max/mean/median for all datasets

        Returns:
            full_output (flask_app.common.s2n_type.AnalystOutput): including a
                dictionary (JSON) of a record containing keywords with values.
        """
        if dataset_key is None and count_by is None:
            return cls.get_endpoint()

        records = []
        try:
            good_params, errinfo = cls._standardize_params(
                dataset_key=dataset_key, count_by=count_by, out_stats=out_stats)

        except BadRequest as e:
            errinfo = {"error": [e.description]}

        else:
            try:
                records, errors = cls._get_dataset_counts(
                    good_params["dataset_key"], good_params["count_by"],
                    good_params["out_stats"])
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
    def _init_sparse_matrix(cls, dataset_key, count_by, out_stats):
        records = []
        errors = {}
        spnet_query = SparseMatrix.init_from_s3(PROJ_BUCKET)

    # ...............................................
    @classmethod
    def _get_dataset_counts(cls, dataset_key, count_by, out_stats):
        records = []
        errors = {}
        spnet_query = SparseMatrix(PROJ_BUCKET)
        try:
            records = spnet_query.get_dataset_counts(dataset_key, count_by, out_stats)
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
    response = svc.get_counts(
        dataset_key=dataset_key, count_by="species", out_stats="describe")
    print(prettify_object(response))
    # AnalystOutput.print_output(response, do_print_rec=True)
    # print(response)
