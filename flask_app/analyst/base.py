"""Parent Class for the Specify Network API services."""
from logging import INFO
import os
from werkzeug.exceptions import BadRequest

from flask_app.common.base import _SpecifyNetworkService
from flask_app.common.s2n_type import AnalystOutput, APIService

from sppy.aws.aws_tools import get_today_str
from sppy.tools.util.utils import get_traceback
from sppy.tools.util.logtools import Logger

# For local debugging
try:
    LOCAL_PATH = os.environ["WORKING_DIRECTORY"]
    INPUT_DATA_PATH = os.environ["AWS_DATA_DIRECTORY"]
except KeyError:
    LOCAL_PATH = '/tmp'
    INPUT_DATA_PATH = LOCAL_PATH
LOG_PATH = os.path.join(LOCAL_PATH, "log")


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
    def _init_logger(cls):
        script_name = os.path.splitext(os.path.basename(__file__))[0]
        todaystr = get_today_str()
        log_name = f"{script_name}_{todaystr}"
        logger = Logger(
            log_name, log_path=LOG_PATH, log_console=True, log_level=INFO)
        return logger

    # ...............................................
    @classmethod
    def _standardize_params(
            cls, summary_type=None, summary_key=None, rank_by=None, order=None,
            limit=10):
        """Standardize query parameters to send to appropriate service.

        Args:
            summary_type: data dimension for summary, comparisons, rank
            summary_key: unique identifier for the data dimension being examined.
            rank_by: rank by occurrence counts or measurement of another dimension of
                the data
            order: sort records "descending" or "ascending"
            limit: integer indicating how many ranked records to return, value must
                be less than QUERY_LIMIT.

        Raises:
            BadRequest: on invalid query parameters.
            BadRequest: on unknown exception parsing parameters.

        Returns:
            a dictionary containing keys and properly formatted values for the
                user specified parameters.
        """
        user_kwargs = {
            "summary_type": summary_type,
            "summary_key": summary_key,
            "rank_by": rank_by,
            "order": order,
            "limit": limit
        }
        try:
            usr_params, errinfo = cls._process_params(user_kwargs)
        except Exception:
            error_description = get_traceback()
            raise BadRequest(error_description)

        # errinfo["error"] indicates bad parameters, throws exception
        try:
            error_description = "; ".join(errinfo["error"])
            raise BadRequest(error_description)
        except KeyError:
            pass

        return usr_params, errinfo


# .............................................................................
if __name__ == "__main__":
    pass
