"""Class for the Specify Network Name API service."""
from http import HTTPStatus
from werkzeug.exceptions import (BadRequest, InternalServerError)

from flask_app.common.s2n_type import S2nKey, APIService, AnalystOutput
from flask_app.analyst.base import _AnalystService

from sppy.tools.s2n.utils import get_traceback


# .............................................................................
class CountSvc(_AnalystService):
    """Specify Network API service for retrieving taxonomic information."""
    SERVICE_TYPE = APIService.Count
    ORDERED_FIELDNAMES = []

    # ...............................................
    @classmethod
    def get_counts(cls, collection_id, organization_id):
        try:
            output = cls._get_records(collection_id, organization_id)
        except Exception:
            traceback = get_traceback()
            output = AnalystOutput(
                cls.SERVICE_TYPE["name"],
                description=cls.SERVICE_TYPE["description"],
                errors={"error": [HTTPStatus.INTERNAL_SERVER_ERROR, traceback]})
        return output.response

    # ...............................................
    @classmethod
    def _get_organization_counts(cls, organization_id):
        return {
            "Organization Raw Counts":
                {
                    organization_id: 1,
                    "org_id_2": 2,
                    "org_id_3": 3
                },
            f"{organization_id} Ratios":
                {
                    "to total": "0.5"
                }
        }

    # ...............................................
    @classmethod
    def _get_collection_counts(cls, collection_id):
        return {
            "Collection Raw Counts":
                {
                    collection_id: 1,
                    "coll_id_2": 2,
                    "coll_id_3": 3
                },
            f"{collection_id} Ratios":
                {
                    "to total": "0.5"
                }
        }

    # ...............................................
    @classmethod
    def _get_records(cls, collection_id, organization_id):
        allrecs = []
        # for response metadata
        query_term = ""
        if collection_id is not None:
            coll_data = cls._get_collection_counts(collection_id)
            allrecs.append(coll_data)
        if organization_id is not None:
            org_data = cls._get_organization_counts(organization_id)
            allrecs.append(org_data)

        # Assemble
        full_out = AnalystOutput(
            cls.SERVICE_TYPE["name"], description=cls.SERVICE_TYPE["description"],
            records=allrecs, errors={})

        return full_out



# .............................................................................
if __name__ == "__main__":
    pass
