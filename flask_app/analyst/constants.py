"""Constants for the Specify Network Analyst API services."""
# .............................................................................
class AnalystKey:
    """Keywords in a valid Specify Network API response."""
    # standard service output keys
    COUNT = "count"
    ERRORS = "errors"

    # ...............................................
    @classmethod
    def response_keys(cls):
        """Top level keywords in valid Specify Network Analyst API response.

        Returns:
            list of all top level keywords in a flask_app.analyst API response
        """
        return {cls.COUNT, cls.ERRORS}


# .............................................................................
class AnalystEndpoint:
    """URL elements for a valid Specify Network API request."""
    Root = "/api/v1"
    # Frontend = "frontend"
    Stats = "stats"

    @classmethod
    def get_endpoints(cls):
        """Get the endpoints for all Specify Network Analyst API services.

        Returns:
            list of all Endpoints
        """
        return [cls.Stats]


class AnalystService:
    """Endpoint, parameters, output format for all Specify Network Analyst APIs."""
    Root = {
        "endpoint": AnalystEndpoint.Root,
        "params": [],
        AnalystKey.RECORD_FORMAT: None
    }
    Stats = {
        "endpoint": AnalystEndpoint.Stats,
        "params": ["group_by"],
        AnalystKey.RECORD_FORMAT: ""
    }


