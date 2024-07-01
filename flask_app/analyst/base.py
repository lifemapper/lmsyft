"""Parent Class for the Specify Network API services."""
from logging import INFO
import os
from werkzeug.exceptions import BadRequest

from flask_app.common.base import _SpecifyNetworkService
from flask_app.common.s2n_type import AnalystOutput, APIService

from sppy.aws.aws_tools import get_today_str
from sppy.tools.s2n.utils import get_traceback
from sppy.tools.util.logtools import Logger

# For local debugging
try:
    LOCAL_PATH = os.environ["WORKING_DIRECTORY"]
except KeyError:
    LOCAL_PATH = '/tmp'
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
            cls, dataset_key=None, species_key=None, pub_org_key=None, count_by=None,
            aggregate_by=None, stat_type=None, order=None, limit=10):
        """Standardize query parameters to send to appropriate service.

        Args:
            dataset_key: unique GBIF dataset identifier for comparisons
            species_key: species_key: unique GBIF identifier of
                accepted taxon concatenated with the species name.
            pub_org_key: unique publishing organization identifier for comparisons
            count_by: counts of "occurrence" or "species"
            aggregate_by: count occurrences (values), species (matrix axis 0) or
                datasets (matrix axis 1)
            stat_type: "describe" or "compare" indicating whether to
                describe the
                  * total for dataset or species count or
                  * min and max count and dataset or species for occurrence count or
                compare the above to the min/max/mean/median for all datasets
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
            "dataset_key": dataset_key,
            "species_key": species_key,
            "pub_org_key": pub_org_key,
            "count_by": count_by,
            "aggregate_by": aggregate_by,
            "stat_type": stat_type,
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
