"""Class for the Specify Network Name API service."""
from werkzeug.exceptions import BadRequest

from flask_app.common.s2n_type import APIService, AnalystOutput
from flask_app.analyst.base import _AnalystService

from spanalyst.aws.constants import S3_BUCKET
from spanalyst.common.util import (combine_errinfo, get_traceback, prettify_object)

from specnet.provider.spnet import SpNetAnalyses


# .............................................................................
class RankSvc(_AnalystService):
    """Specify Network API service for retrieving taxonomic information."""
    SERVICE_TYPE = APIService.Rank
    ORDERED_FIELDNAMES = []

    # ...............................................
    @classmethod
    def rank_counts(cls, summary_type, rank_by, order=None, limit=10):
        """Return occurrence and species counts for dataset/organization identifiers.

        Args:
            summary_type: data dimension for rank comparison, (currently "species" or
                "dataset")
            rank_by: rank by counts of "occurrence" or another data dimension (not
                the rank type).
            order: rank in "descending" or "ascending" order.
            limit: integer URL parameter specifying the number of ordered records to
                return.

            full_output (flask_app.common.s2n_type.AnalystOutput): including records
                as a list of lists (CSV) or dictionaries (JSON) of records
                containing dataset_key,  occurrence count, and species count.

        Returns:
            JSON response for this API query.
        """
        if summary_type is None:
            return cls.get_endpoint()

        records = []
        try:
            good_params, errinfo = cls._standardize_params(
                summary_type=summary_type, rank_by=rank_by, order=order, limit=limit)

        except BadRequest as e:
            errinfo = {"error": [e.description]}

        else:
            # Query for ordered dataset counts
            try:
                records, errors = cls._get_ordered_counts(
                    good_params["summary_type"], good_params["rank_by"],
                    good_params["order"], good_params["limit"])
            except Exception:
                errors = {"error": [get_traceback()]}

            # Combine errors from success or failure
            errinfo = combine_errinfo(errinfo, errors)

        # Assemble
        full_out = AnalystOutput(
            cls.SERVICE_TYPE["name"], description=cls.SERVICE_TYPE["description"],
            output=records, errors=errinfo)

        return full_out.response

    # ...............................................
    @classmethod
    def _get_ordered_counts(cls, summary_type, rank_by, order, limit):
        records = []
        spnet = SpNetAnalyses(S3_BUCKET)
        if summary_type == "dataset":
            try:
                records, errinfo = spnet.rank_dataset_counts(rank_by, order, limit)
            except Exception:
                errinfo = {"error": [get_traceback()]}
        # species
        else:
            errinfo = {"error": ["Only dataset ranks are currently available"]}
            # try:
            #     records, errinfo = spanalyst.rank_species_counts(rank_by, order, limit)
            # except Exception:
            #     errinfo = {"error": [get_traceback()]}

        return records, errinfo


# .............................................................................
if __name__ == "__main__":
    summary_type = "dataset"
    # Default: order = "descending"
    limit = 5

    svc = RankSvc()
    response = svc.get_endpoint()
    print(prettify_object(response))

    rank_by = "occurrence"
    response = svc.rank_counts(summary_type, rank_by)
    print(prettify_object(response))

    response = svc.rank_counts(summary_type, rank_by, order="ascending")
    print(prettify_object(response))

    rank_by = "species"
    response = svc.rank_counts(summary_type, rank_by)
    print(prettify_object(response))

    response = svc.rank_counts(summary_type, rank_by, order="ascending")
    print(prettify_object(response))

"""
from flask_app.analyst.rank import *

summary_type = "dataset"
limit = 5

svc = RankSvc()
response = svc.get_endpoint()
print(prettify_object(response))

rank_by = "occurrence"
response = svc.rank_counts(summary_type, rank_by)
print(prettify_object(response))

response = svc.rank_counts(summary_type, rank_by, order="ascending")
print(prettify_object(response))

rank_by = "species"
response = svc.rank_counts(summary_type, rank_by)
print(prettify_object(response))

response = svc.rank_counts(summary_type, rank_by, order="ascending")
print(prettify_object(response))
"""
