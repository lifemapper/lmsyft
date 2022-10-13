from lmtrex.common.lmconstants import (APIService)
from lmtrex.frontend.templates import frontend_template
from lmtrex.flask_app.broker.base import _S2nService


# .............................................................................
class FrontendSvc(_S2nService):
    SERVICE_TYPE = APIService.Frontend

    # ...............................................
    @classmethod
    def get_frontend(cls, **kwargs):
        """Front end for the broker services

        Aggregate the results from badge, occ, name and map endpoints into a
        single web page.

        Args:
            occid: an occurrenceID, a DarwinCore field intended for a globally
                unique identifier (https://dwc.tdwg.org/list/#dwc_occurrenceID)
            namestr: Species name. Used only as a fallback if failed to resolve
                occurrenceID

        Return:
            Responses from all agregators formatted as an HTML page
        """

        return frontend_template()
