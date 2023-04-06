"""Class for the frontend UI of the Specify Network API services."""
from flask_app.broker.constants import (APIService)
from sppy.frontend.templates import frontend_template
from flask_app.broker.base import _S2nService


# .............................................................................
class FrontendSvc(_S2nService):
    """Class for the User Interface to the Specify Network services."""
    SERVICE_TYPE = APIService.Frontend

    # ...............................................
    @classmethod
    def get_frontend(cls, **kwargs):
        """Front end for the broker services.

        Aggregate the results from badge, occ, name and map endpoints into a
        single web page.

        Args:
            kwargs: keyword arguments sent by the service

        Returns:
            Responses from all agregators formatted as an HTML page
        """
        return frontend_template()
