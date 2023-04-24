"""Class for the Specify Network Name API service."""
from http import HTTPStatus
from werkzeug.exceptions import (BadRequest, InternalServerError)

from flask_app.common.s2n_type import S2nKey, AnalystService, AnalystOutput
from flask_app.analyst.base import _AnalystService

from sppy.tools.s2n.utils import get_traceback


# .............................................................................
class CountSvc(_AnalystService):
    """Specify Network API service for retrieving taxonomic information."""
    SERVICE_TYPE = AnalystService.Stats
    ORDERED_FIELDNAMES = []

    # ...............................................
    @classmethod
    def get_stats(cls, collection_id, organization_id):
        try:
            output = cls._get_records(collection_id, organization_id)
        except Exception:
            traceback = get_traceback()
            output = cls.get_failure(
                cls.SERVICE_TYPE["endpoint"], HTTPStatus.INTERNAL_SERVER_ERROR,
                errinfo={"error": [traceback]})
        else:
            output.set_value(
                S2nKey.RECORD_FORMAT, cls.SERVICE_TYPE[S2nKey.RECORD_FORMAT])
        return output.response



    # ...............................................
    @classmethod
    def _get_records(cls, collection_id, organization_id):
        allrecs = []
        # for response metadata
        query_term = ""
        if collection_id is not None:
            return

        # Assemble
        prov_meta = cls._get_s2n_provider_response_elt(query_term=query_term)
        full_out = AnalystOutput(
            1, cls.SERVICE_TYPE["endpoint"], provider=prov_meta,
            records=allrecs, errors={})

        return full_out

    # ...............................................
    @classmethod
    def get_name_records(
            cls, namestr=None, provider=None, is_accepted=True, gbif_parse=True,
            gbif_count=True, kingdom=None, **kwargs):
        """Get taxon records for a scientific name from each requested name service.

        Args:
            namestr: a scientific name
            provider: comma-delimited list of requested provider codes.  Codes are
                delimited for each in flask_app.broker.constants ServiceProvider
            is_accepted: flag to indicate whether to limit to "valid" or  "accepted"
                taxa in the ITIS or GBIF Backbone Taxonomy
            gbif_parse: flag to indicate whether to first use the GBIF parser to parse a
                scientific name into canonical name
            gbif_count: flag to indicate whether to count GBIF occurrences of this taxon
            kingdom: not yet implemented
            **kwargs: additional keyword arguments are accepted and ignored

        Returns:
            A flask_app.broker.s2n_type.S2nOutput object containing records for each
            provider.  Each provider element is a S2nOutput object with records as a
            list of dictionaries following the flask_app.broker.s2n_type.S2nSchema.NAME
            corresponding to names in the provider taxonomy.

        Raises:
            BadRequest: on invalid query parameters.
            BadRequest: on unknown exception parsing parameters.
            InternalServerError: on unknown exception when executing request
        """
        if namestr is None:
            return cls.get_endpoint()
        else:
            # No filter_params defined for Name service yet
            try:
                good_params, errinfo = cls._standardize_params(
                    namestr=namestr, provider=provider, is_accepted=is_accepted,
                    gbif_parse=gbif_parse, gbif_count=gbif_count, kingdom=kingdom)
                # Bad parameters
                try:
                    error_description = "; ".join(errinfo["error"])
                    raise BadRequest(error_description)
                except KeyError:
                    pass
            except Exception:
                error_description = get_traceback()
                raise BadRequest(error_description)

            try:
                # Do Query!
                output = cls._get_records(
                    good_params["namestr"], good_params["provider"],
                    good_params["is_accepted"], good_params["gbif_count"],
                    good_params["kingdom"])

                # Add message on invalid parameters to output
                try:
                    for err in errinfo["warning"]:
                        output.append_error("warning", err)
                except KeyError:
                    pass

            except Exception:
                error_description = get_traceback()
                raise InternalServerError(error_description)

        return output.response


# .............................................................................
if __name__ == "__main__":
    pass
