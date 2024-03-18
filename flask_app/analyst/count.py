"""Class for the Specify Network Name API service."""
from http import HTTPStatus
from werkzeug.exceptions import BadRequest

from flask_app.common.s2n_type import APIService, AnalystOutput
from flask_app.analyst.base import _AnalystService

from sppy.aws.aws_constants import PROJ_BUCKET
from sppy.tools.provider.awss3 import S3Query
from sppy.tools.s2n.utils import (combine_errinfo, get_traceback)


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

        allrecs = []
        try:
            good_params, errinfo = cls._standardize_params(
                dataset_key=dataset_key, pub_org_key=pub_org_key)

        except BadRequest as e:
            errinfo = {"error": e.description}

        else:
            # Query dataset counts
            if good_params["dataset_key"] is not None:
                try:
                    records, errors = cls._get_dataset_counts(
                        good_params["dataset_key"])
                except Exception:
                    errors = {"error": get_traceback()}
                else:
                    if records:
                        allrecs.append(records)
                # Combine errors from success or failure
                errinfo = combine_errinfo(errinfo, errors)

            # Query organization counts
            if good_params["pub_org_key"] is not None:
                errors = {
                    "warning": "Count by Publishing Organization is not implemented"}
                errinfo = combine_errinfo(errinfo, errors)

        # Assemble
        full_out = AnalystOutput(
            cls.SERVICE_TYPE["name"], description=cls.SERVICE_TYPE["description"],
            records=allrecs, errors=errinfo)

        return full_out.response

# ...............................................
    @classmethod
    def _get_dataset_counts(cls, dataset_key):
        """Get counts for datasetKey.

        Args:
            dataset_key: unique GBIF identifier for dataset of interest.

        Returns:
            a flask_app.analyst.s2n_type.AnalystOutput object with optional records as a
            list of records corresponding to occurrence and counts for the dataset.
        """
        records = []
        errors = {}
        s3 = S3Query(PROJ_BUCKET)
        try:
            records = s3.get_dataset_counts(dataset_key)
        except Exception:
            traceback = get_traceback()
            errors["error"] = [HTTPStatus.INTERNAL_SERVER_ERROR, traceback]

        return records, errors



# .............................................................................
if __name__ == "__main__":
    dataset_key = "0000e36f-d0e9-46b0-aa23-cc1980f00515"

    svc = CountSvc()
    response = svc.get_endpoint()
    AnalystOutput.print_output(response, do_print_rec=True)
    # print(response)
    response = svc.get_counts(dataset_key=dataset_key, pub_org_key=None)
    AnalystOutput.print_output(response, do_print_rec=True)
    # print(response)

