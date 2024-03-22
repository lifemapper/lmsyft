"""Parent Class for the Specify Network API services."""
from werkzeug.exceptions import BadRequest

from flask_app.common.base import _SpecifyNetworkService
from flask_app.common.s2n_type import AnalystOutput, APIService

from sppy.tools.s2n.utils import get_traceback


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
            cls, dataset_key=None, pub_org_key=None, count_by=None, order=None,
            limit=10):
        """Standardize query parameters to send to appropriate service.

        Args:
            dataset_key: unique GBIF dataset identifier for comparisons
            pub_org_key: unique publishing organization identifier for comparisons
            count_by: counts of "occurrence" or "species"
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
            "pub_org_key": pub_org_key,
            "count_by": count_by,
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

    # ...............................................
    @classmethod
    def _add_dataset_names_to_records(
            cls, records, dataset_key_field="datasetkey",
            dataset_name_field="dataset_name"):
        pass
        # # TODO: change this to a call to an S3 table with all dataset keys/names
        # # if import is at top level, causes recursion error in awss3.count_datasets
        # from sppy.tools.provider.gbif import GbifAPI
        # gbif = GbifAPI(service="dataset")
        # for rec in records:
        #     dataset_name, _ = gbif.get_dataset(rec[dataset_key_field])
        #     rec[dataset_name_field] = dataset_name


# .............................................................................
if __name__ == "__main__":
    pass
