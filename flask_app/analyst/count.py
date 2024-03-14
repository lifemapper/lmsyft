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
    def get_counts(cls, dataset_key=None, pub_org_key=None, format="JSON"):
        """Return occurrence and species counts for dataset/organization identifiers.

        Args:
            dataset_key: URL parameter for unique GBIF identifier of dataset.
            pub_org_key: URL parameter for unique GBIF identifier of
                publishingOrganization.
            format: output format, options "CSV" or "JSON"

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
                        good_params["dataset_key"], format)
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
    def get_ranked_counts(cls, descending=True, limit=10):
        allrecs = []
        try:
            good_params, errinfo = cls._standardize_params(
                cls, descending=descending, limit=limit)

        except BadRequest as e:
            errinfo = combine_errinfo(errinfo, {"error": e.description})

        else:
            # Do Query!
            try:
                s3 = S3Query(PROJ_BUCKET)
                records, errors = s3.rank_datasets_by_species(
                    descending=True, limit=limit)
            except Exception:
                errors = {"error": get_traceback()}
            else:
                allrecs.append(records)
            # Combine errors from success or failure
            errinfo = combine_errinfo(errinfo, errors)
        return allrecs, errinfo

# ...............................................
    @classmethod
    def _get_dataset_counts(cls, dataset_key, format):
        """Get counts for datasetKey.

        Args:
            dataset_key: unique GBIF identifier for dataset of interest.
            format: output format, options "CSV" or "JSON"

        Returns:
            a flask_app.analyst.s2n_type.AnalystOutput object with optional records as a
            list of records corresponding to occurrence and counts for the dataset.
        """
        records = []
        errors = {}
        s3 = S3Query(PROJ_BUCKET)
        try:
            records = s3.get_dataset_counts(dataset_key, format=format)
        except Exception:
            traceback = get_traceback()
            errors["error"] = [HTTPStatus.INTERNAL_SERVER_ERROR, traceback]

        return records, errors

    # ...............................................
    @classmethod
    def _get_organization_counts(cls, pub_org_key):
        """Get counts for publishingOrganizationKey.

        Args:
            pub_org_key: Unique identifier for GBIF publishing organizations.

        Returns:
            a flask_app.analyst.s2n_type.AnalystOutput object with optional records as a
            list of records corresponding to occurrence and counts for the organization.
        """
        records = []
        errors = {}
        s3 = S3Query(PROJ_BUCKET)
        try:
            (occ_count, species_count) = s3.get_org_counts(pub_org_key)
        except Exception:
            traceback = get_traceback()
            errors["error"] = [HTTPStatus.INTERNAL_SERVER_ERROR, traceback]
        else:
            records.append((occ_count, species_count))
        return records, errors


    # # ...............................................
    # @classmethod
    # def _get_records(cls, dataset_key, pub_org_key):
    #     allrecs = []
    #     # for response metadata
    #     if dataset_key is not None:
    #         records, errors = cls._get_dataset_counts(dataset_key)
    #         allrecs.append(records)
    #     if pub_org_key is not None:
    #         records, errors = cls._get_organization_counts(pub_org_key)
    #         allrecs.append(records)
    #
    #     # Assemble
    #     full_out = AnalystOutput(
    #         cls.SERVICE_TYPE["name"], description=cls.SERVICE_TYPE["description"],
    #         records=allrecs, errors={})
    #
    #     return full_out


# .............................................................................
if __name__ == "__main__":
    format = "JSON"
    dataset_key = "0000e36f-d0e9-46b0-aa23-cc1980f00515"

    svc = CountSvc()
    response = svc.get_endpoint()
    AnalystOutput.print_output(response, do_print_rec=True)
    # print(response)
    response = svc.get_counts(dataset_key=dataset_key, pub_org_key=None, format=format)
    AnalystOutput.print_output(response, do_print_rec=True)
    # print(response)

