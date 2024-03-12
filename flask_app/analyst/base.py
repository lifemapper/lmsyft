"""Parent Class for the Specify Network API services."""
from flask import Flask
from werkzeug.exceptions import (BadRequest, InternalServerError)

from flask_app.common.base import _SpecifyNetworkService
from sppy.tools.s2n.utils import add_errinfo, get_traceback
from flask_app.common.s2n_type import AnalystOutput, APIEndpoint, APIService

app = Flask(__name__)


# .............................................................................
class _AnalystService(_SpecifyNetworkService):
    """Base S-to-the-N service, handles parameter names and acceptable values."""
    # overridden by subclasses
    SERVICE_TYPE = APIService.AnalystRoot

    # ...............................................
    @classmethod
    def get_endpoint(cls, **kwargs):
        """Return the http response for this class endpoint.

        Args:
            **kwargs: keyword arguments are accepted but ignored

        Returns:
            flask_app.analyst.s2n_type.S2nOutput object

        Raises:
            Exception: on unknown error.
        """
        try:
            output = cls._show_online()
        except Exception:
            raise
        return output.response

    # ...............................................
    @classmethod
    def _show_online(cls):
        svc = cls.SERVICE_TYPE["name"]
        info = {
            "info": "Specify Network {} service is online.".format(svc)}

        param_lst = []
        for p, pdict in cls.SERVICE_TYPE["params"].items():
            pinfo = pdict.copy()
            pinfo["type"] = str(type(pinfo["type"]))
            param_lst.append({p: pinfo})
        info["parameters"] = param_lst

        output = AnalystOutput(
            svc, description=cls.SERVICE_TYPE["description"], errors=info)
        return output

    # ...............................................
    @classmethod
    def _standardize_params(
            cls, dataset_key=None, pub_org_key=None, order="descending", limit=10):
        """Standardize query parameters to send to appropriate service.

        Args:
            dataset_key: unique GBIF dataset identifier for comparisons
            pub_org_key: unique publishing organization identifier for comparisons

        Returns:
            a dictionary containing keys and properly formatted values for the
                user specified parameters.
        """
        user_kwargs = {
            "collection_id": dataset_key,
            "organization_id": pub_org_key,
            "order": order,
            "limit": limit
        }

        try:
            usr_params, errinfo = cls._process_params(user_kwargs)

            # errinfo indicates bad parameters
            try:
                error_description = "; ".join(errinfo["error"])
                raise BadRequest(error_description)
            except KeyError:
                pass

        except Exception:
            error_description = get_traceback()
            raise BadRequest(error_description)

        return usr_params, errinfo


# .............................................................................
if __name__ == "__main__":
    pass
