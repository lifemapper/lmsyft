"""Class for the Specify Network Name API service."""
import boto3
from http import HTTPStatus
from werkzeug.exceptions import (BadRequest, InternalServerError)

from flask_app.common.s2n_type import APIService, AnalystOutput
from flask_app.common.util import print_analyst_output
from flask_app.analyst.base import _AnalystService

from sppy.aws.aws_constants import ENCODING, PROJ_BUCKET, REGION
from sppy.tools.provider.awss3 import S3Query
from sppy.tools.s2n.utils import get_traceback


# .............................................................................
class CountSvc(_AnalystService):
    """Specify Network API service for retrieving taxonomic information."""
    SERVICE_TYPE = APIService.Count
    ORDERED_FIELDNAMES = []

    # ...............................................
    @classmethod
    def _get_params_errors(cls, *kwargs):
        try:
            good_params, errinfo = cls._standardize_params(cls, kwargs)
            # errinfo indicates bad parameters
            try:
                error_description = "; ".join(errinfo["error"])
                raise BadRequest(error_description)
            except KeyError:
                pass

        except Exception:
            error_description = get_traceback()
            raise BadRequest(error_description)

        return good_params, errinfo

    # ...............................................
    @classmethod
    def get_counts(cls, dataset_key=None, pub_org_key=None):
        if dataset_key is None and pub_org_key is None:
            return cls.get_endpoint()
        else:
            try:
                good_params, errinfo = cls._standardize_params(
                    cls, dataset_key=dataset_key, pub_org_key=pub_org_key)
                # errinfo indicates bad parameters
                try:
                    error_description = "; ".join(errinfo["error"])
                    raise BadRequest(error_description)
                except KeyError:
                    pass

            except Exception:
                error_description = get_traceback()
                raise BadRequest(error_description)

            # Do Query!
            try:
                allrecs = []
                errors = {}
                # for response metadata
                if dataset_key is not None:
                    records, errors = cls._get_dataset_counts(dataset_key)
                    allrecs.append(records)
                if pub_org_key is not None:
                    errors["warning"] = \
                        "Count by Publishing Organization is not implemented"
                    # records, errors = cls._get_organization_counts(pub_org_key)
                    # allrecs.append(records)

                # Assemble
                full_out = AnalystOutput(
                    cls.SERVICE_TYPE["name"], description=cls.SERVICE_TYPE["description"],
                    records=allrecs, errors=errors)

                # Add message on invalid parameters to output
                try:
                    for err in errinfo["warning"]:
                        full_out.append_error("warning", err)
                except KeyError:
                    pass

            except Exception:
                error_description = get_traceback()
                raise InternalServerError(error_description)

        return full_out.response

    # ...............................................
    @classmethod
    def get_ranked_counts(cls, descending=True, limit=10):
            try:
                good_params, errinfo = cls._standardize_params(
                    cls, descending=descending, limit=limit)
                # errinfo indicates bad parameters
                try:
                    error_description = "; ".join(errinfo["error"])
                    raise BadRequest(error_description)
                except KeyError:
                    pass

            except Exception:
                error_description = get_traceback()
                raise BadRequest(error_description)

            # Do Query!
            try:
                s3 = S3Query(PROJ_BUCKET, region=REGION, encoding=ENCODING)
                records = s3.rank_datasets_by_species(descending=True, limit=limit)

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
    # from flask_app.broker.constants import import TST_VALUES
    # occids = TST_VALUES.GUIDS_WO_SPECIFY_ACCESS[0:3]
    occids = ["84fe1494-c378-4657-be15-8c812b228bf4",
              "04c05e26-4876-4114-9e1d-984f78e89c15",
              "2facc7a2-dd88-44af-b95a-733cc27527d4"]
    occids = ["01493b05-4310-4f28-9d81-ad20860311f3",
              "01559f57-62ca-45ba-80b1-d2aafdc46f44",
              "015f35b8-655a-4720-9b88-c1c09f6562cb",
              "016613ba-4e65-44d5-94d1-e24605afc7e1",
              "0170cead-c9cd-48ba-9819-6c5d2e59947e",
              "01792c67-910f-4ad6-8912-9b1341cbd983",
              "017ea8f2-fc5a-4660-92ec-c203daaaa631",
              "018728bb-c376-4562-9ccb-8e3c3fd70df6",
              "018a34a9-55da-4503-8aee-e728ba4be146",
              "019b547a-79c7-47b3-a5ae-f11d30c2b0de"]
    # This occ has 16 issues in IDB, 0 in GBIF
    occids = ["2facc7a2-dd88-44af-b95a-733cc27527d4",
              "2c1becd5-e641-4e83-b3f5-76a55206539a"]
    occids = ["bffe655b-ea32-4838-8e80-a80e391d5b11"]
    occids = ["db193603-1ed3-11e3-bfac-90b11c41863e"]

    svc = CountSvc()
    out = svc.get_endpoint()
    print_analyst_output(out, do_print_rec=True)

    coll_id = "a7156437-55ec-4c6f-89de-938f9361753d"
    org_id = None
    out = svc.get_counts(coll_id, org_id)
    print_analyst_output(out, do_print_rec=True)

    # for occid in occids:
    #     out = svc.get_occurrence_records(occid=occid, provider=None, count_only=False)
    #     outputs = out["records"]
    #     print_broker_output(out, do_print_rec=True)
