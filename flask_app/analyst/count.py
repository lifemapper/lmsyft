"""Class for the Specify Network Name API service."""
import boto3
from http import HTTPStatus
from werkzeug.exceptions import (BadRequest, InternalServerError)

from flask_app.common.s2n_type import APIService, AnalystOutput
from flask_app.common.util import print_analyst_output
from flask_app.analyst.base import _AnalystService

from sppy.aws.aws_constants import ENCODING, PROJ_BUCKET, REGION
from sppy.tools.provider.awss3 import S3Query
from sppy.tools.s2n.utils import combine_errinfo, get_traceback


# .............................................................................
class CountSvc(_AnalystService):
    """Specify Network API service for retrieving taxonomic information."""
    SERVICE_TYPE = APIService.Count
    ORDERED_FIELDNAMES = []

    # ...............................................
    @classmethod
    def get_counts(cls, dataset_key=None, pub_org_key=None):
        if dataset_key is None and pub_org_key is None:
            return cls.get_endpoint()

        allrecs = []
        try:
            good_params, errinfo = cls._standardize_params(
                cls, dataset_key=dataset_key, pub_org_key=pub_org_key)

        except BadRequest as e:
            errinfo = combine_errinfo(errinfo, {"error": e.description})

        else:

            # Query dataset counts
            if dataset_key is not None:
                try:
                    records, errors = cls._get_dataset_counts(dataset_key)
                except Exception:
                    errors = {"error": get_traceback()}
                else:
                    allrecs.append(records)
                # Combine errors from success or failure
                errinfo = combine_errinfo(errinfo, errors)

            # Query organization counts
            if pub_org_key is not None:
                errors = {"warning": "Count by Publishing Organization is not implemented"}
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
                s3 = S3Query(PROJ_BUCKET, region=REGION, encoding=ENCODING)
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
    def _get_dataset_counts(cls, dataset_key):
        """Get counts for datasetKey.

        Args:
            dataset_key: Unique identifier for GBIF datasets.

        Returns:
            a flask_app.analyst.s2n_type.AnalystOutput object with optional records as a
            list of records corresponding to occurrence and counts for the dataset.
        """
        records = []
        errors = {}
        s3 = S3Query(PROJ_BUCKET, region=REGION, encoding=ENCODING)
        try:
            (occ_count, species_count) = s3.get_dataset_counts(dataset_key)
        except Exception:
            traceback = get_traceback()
            errors["error"] = [HTTPStatus.INTERNAL_SERVER_ERROR, traceback]
        else:
            records.append((occ_count, species_count))
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
        s3 = S3Query(PROJ_BUCKET, region=REGION, encoding=ENCODING)
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
    svc = CountSvc()
    out = svc.get_endpoint()
    print_analyst_output(out, do_print_rec=True)

    coll_id = "a7156437-55ec-4c6f-89de-938f9361753d"
    org_id = None
    out = svc.get_counts(coll_id, org_id)
    print_analyst_output(out, do_print_rec=True)
