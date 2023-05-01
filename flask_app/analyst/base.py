"""Parent Class for the Specify Network API services."""
from flask import Flask

import sppy.tools.s2n.utils as lmutil
from flask_app.common.s2n_type import AnalystOutput, APIEndpoint, APIService

app = Flask(__name__)


# .............................................................................
class _AnalystService:
    """Base S-to-the-N service, handles parameter names and acceptable values."""
    # overridden by subclasses
    SERVICE_TYPE = APIService.AnalystRoot

    # ...............................................

    # .............................................................................
    @classmethod
    def _get_valid_requested_params(cls, user_params_string, valid_params):
        """Return valid and invalid options for parameters that accept >1 values.

        Args:
            user_params_string: user-requested parameters as a string.
            valid_params: valid parameter values

        Returns:
            valid_requested_params: list of valid params from the provided query string
            invalid_params: list of invalid params from the provided query string

        Note:
            For the badge service, exactly one provider is required.  For all other
            services, multiple providers are accepted, and None indicates to query all
            valid providers.
        """
        valid_requested_params = invalid_params = []

        if user_params_string:
            tmplst = user_params_string.split(",")
            user_params = {tp.lower().strip() for tp in tmplst}

            valid_requested_params = set()
            invalid_params = set()
            # valid_requested_providers, invalid_providers =
            #   cls.get_multivalue_options(user_provs, valid_providers)
            for param in user_params:
                if param in valid_params:
                    valid_requested_params.add(param)
                else:
                    invalid_params.add(param)

            invalid_params = list(invalid_params)
            if valid_requested_params:
                valid_requested_params = list(valid_requested_params)
            else:
                valid_requested_params = []

        return valid_requested_params, invalid_params


    # .............................................................................
    @classmethod
    def endpoint(cls):
        """Return the URL endpoint for this class.

        Returns:
            URL endpoint for the service
        """
        endpoint = f"{APIEndpoint.analyst_root()}/{cls.SERVICE_TYPE['endpoint']}"
        return endpoint

    # ...............................................
    @classmethod
    def get_endpoint(cls, **kwargs):
        """Return the http response for this class endpoint.

        Args:
            **kwargs: keyword arguments are accepted but ignored

        Returns:
            flask_app.broker.s2n_type.S2nOutput object

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
    def _fix_type_new(cls, key, provided_val):
        """Modify a parameter value to a valid type and value.

        Args:
            key: parameter key
            provided_val: user-provided parameter value

        Returns:
            usr_val: a valid value for the parameter
            valid_options: list of valid options (for error message)

        Note:
            Corrections:
            * cast to correct type
            * validate with any options
            * if value is invalid (type or value), return the default.
        """
        valid_options = None
        if provided_val is None:
            return None
        # all strings are lower case
        try:
            provided_val = provided_val.lower()
        except Exception:
            pass

        # First see if restricted to options
        default_val = cls.SERVICE_TYPE["params"][key]["default"]
        type_val = cls.SERVICE_TYPE["params"][key]["type"]
        # If restricted options, check
        try:
            options = cls.SERVICE_TYPE["params"][key]["options"]
        except KeyError:
            options = None
        else:
            # Invalid option returns default value
            if provided_val in options:
                usr_val = provided_val
            else:
                valid_options = options
                usr_val = default_val

        # If not restricted to options
        if options is None:
            # Cast values to correct type. Failed conversions return default value
            if isinstance(type_val, str) and not options:
                usr_val = str(provided_val)

            elif isinstance(type_val, float):
                try:
                    usr_val = float(provided_val)
                except ValueError:
                    usr_val = default_val

            # Boolean also tests as int, so try boolean first
            elif isinstance(type_val, bool):
                if provided_val in (0, "0", "n", "no", "f", "false"):
                    usr_val = False
                elif provided_val in (1, "1", "y", "yes", "t", "true"):
                    usr_val = True
                else:
                    valid_options = (True, False)
                    usr_val = default_val

            elif isinstance(type_val, int):
                try:
                    usr_val = int(provided_val)
                except ValueError:
                    usr_val = default_val

            else:
                usr_val = provided_val

        return usr_val, valid_options

    # ...............................................
    @classmethod
    def _process_params(cls, user_kwargs=None):
        """Modify all user provided keys to lowercase and values to correct types.

        Args:
            user_kwargs: dictionary of keywords and values sent by the user for
                the current service.

        Returns:
            good_params: dictionary of valid parameters and values
            errinfo: dictionary of errors for different error levels.

        Note:
            A list of valid values for a keyword can include None as a default
                if user-provided value is invalid
        Todo:
            Do we need not_in_valid_options for error message?
        """
        good_params = {}
        errinfo = {}

        # Correct all parameter keys/values present
        for key in cls.SERVICE_TYPE["params"]:
            val = user_kwargs[key]
            # Done in calling function
            if val is not None:
                usr_val, valid_options = cls._fix_type_new(key, val)
                if valid_options is not None and val not in valid_options:
                    errinfo = lmutil.add_errinfo(
                        errinfo, "error",
                        f"Value {val} for parameter {key} is not in valid options "
                        f"{cls.SERVICE_TYPE['params'][key]['options']}")
                    good_params[key] = None
                else:
                    good_params[key] = usr_val

        # Fill in defaults for missing parameters
        for key in cls.SERVICE_TYPE["params"]:
            param_meta = cls.SERVICE_TYPE["params"][key]
            try:
                _ = good_params[key]
            except KeyError:
                good_params[key] = param_meta["default"]

        return good_params, errinfo

    # ...............................................
    @classmethod
    def _standardize_params(cls, collection_id=None, organization_id=None):
        """Standardize query parameters to send to appropriate service.

        Args:
            collection_id: collection identifier for comparisons
            organization_id: organization identifier for comparisons

        Returns:
            a dictionary containing keys and properly formatted values for the
                user specified parameters.
        """
        user_kwargs = {
            "collection_id": collection_id,
            "organization_id": organization_id
        }

        usr_params, errinfo = cls._process_params(user_kwargs)

        return usr_params, errinfo

    # ..........................
    @staticmethod
    def OPTIONS():
        """Common options request for all services (needed for CORS)."""
        return


# .............................................................................
if __name__ == "__main__":
    pass
