"""Module containing functions for MorphoSource API Queries."""
from http import HTTPStatus

from flask_app.broker.constants import (MorphoSource, ServiceProvider, TST_VALUES)
from flask_app.common.s2n_type import APIEndpoint, S2nKey, BrokerSchema

from sppy.tools.provider.api import APIQuery
from sppy.tools.s2n.utils import add_errinfo, get_traceback


# .............................................................................
class MorphoSourceAPI(APIQuery):
    """Class to query Specify portal APIs and return results."""
    PROVIDER = ServiceProvider.MorphoSource
    OCCURRENCE_MAP = BrokerSchema.get_mopho_occurrence_map()

    # ...............................................
    def __init__(
            self, resource=MorphoSource.OCC_RESOURCE, q_filters=None,
            other_filters=None, logger=None):
        """Constructor.

        Args:
            resource: MorphoSource service for API query
            q_filters: dictionary of filters for the q element of a solr query.
            other_filters: dictionary of other filters.
            logger: object for logging messages and errors.
        """
        url = f"{MorphoSource.REST_URL}/{MorphoSource.COMMAND}/{resource}"
        APIQuery.__init__(
            self, url, q_filters=q_filters,
            other_filters=other_filters, logger=logger)

    # ...............................................
    @classmethod
    def _standardize_record(cls, rec):
        newrec = {}
        view_std_fld = BrokerSchema.get_view_url_fld()
        data_std_fld = BrokerSchema.get_data_url_fld()
        for stdfld, provfld in cls.OCCURRENCE_MAP.items():
            try:
                val = rec[provfld]
            except KeyError:
                val = None

            # Save ID field, plus use to construct URLs
            if provfld == MorphoSource.DWC_ID_FIELD:
                newrec[stdfld] = val
                newrec[data_std_fld] = MorphoSource.get_occurrence_data(val)

            # Use local ID field to also construct webpage url
            elif provfld == MorphoSource.LOCAL_ID_FIELD:
                newrec[view_std_fld] = MorphoSource.get_occurrence_view(val)

            # all others
            else:
                newrec[stdfld] = val
        return newrec

    # ...............................................
    @classmethod
    def get_occurrences_by_occid_page1(cls, occid, count_only=False, logger=None):
        """Class method to return the first page of occurrence records.

        Args:
            occid: OccurrenceID for searching API
            count_only: True to return only a count, no records.
            logger: object for logging messages and errors.

        Returns:
            flask_app.broker.s2n_type.S2nOutput object
        """
        start = 0
        errinfo = {}
        api = MorphoSourceAPI(
            resource=MorphoSource.OCC_RESOURCE,
            q_filters={MorphoSource.OCCURRENCEID_KEY: occid},
            other_filters={"start": start, "limit": MorphoSource.LIMIT})
        # Handle bad SSL certificate on old MorphoSource API until v2 is working
        verify = True
        if api.url.index(MorphoSource.REST_URL) >= 0:
            verify = False
        try:
            api.query_by_get(verify=verify)
        except Exception:
            tb = get_traceback()
            errinfo = add_errinfo(errinfo, "error", cls._get_error_message(err=tb))
            std_out = cls.get_api_failure(
                APIEndpoint.Occurrence, HTTPStatus.INTERNAL_SERVER_ERROR,
                errinfo=errinfo)
        else:
            # Standardize output from provider response
            if api.error:
                errinfo = add_errinfo(errinfo, "error", api.error)

            std_out = cls._standardize_output(
                api.output, MorphoSource.TOTAL_KEY, MorphoSource.RECORDS_KEY,
                MorphoSource.RECORD_FORMAT, APIEndpoint.Occurrence,
                query_status=api.status_code, query_urls=[api.url],
                count_only=count_only, errinfo=errinfo)

        return std_out


# .............................................................................
if __name__ == "__main__":
    # test

    for guid in TST_VALUES.GUIDS_WO_SPECIFY_ACCESS:
        moutput = MorphoSourceAPI.get_occurrences_by_occid_page1(guid)
        for r in moutput.response[S2nKey.RECORDS]:
            occid = notes = None
            try:
                occid = r["specimen.occurrence_id"]
                notes = r["specimen.notes"]
            except Exception as e:
                msg = f"Morpho source record exception {e}"
            else:
                msg = f"{occid}: {notes}"
            print(msg)

"""
https://ms1.morphosource.org/api/v1/find/specimens?start=0&limit=1000&q=occurrence_id%3Aed8cfa5a-7b47-11e4-8ef3-782bcb9cd5b5"
url = "https://ea-boyerlab-morphosource-01.oit.duke.edu/api/v1/find/specimens?start=0&limit=1000&q=occurrence_id%3Aed8cfa5a-7b47-11e4-8ef3-782bcb9cd5b5"
"""
