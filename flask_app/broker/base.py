"""Parent Class for the Specify Network API services."""
from flask import Flask
from werkzeug.exceptions import BadRequest, InternalServerError

import sppy.tools.s2n.utils as lmutil
from flask_app.common.s2n_type import (
    APIEndpoint, APIService, BrokerOutput, get_host_url, S2nKey, ServiceProvider)
from sppy.tools.provider.gbif import GbifAPI
from sppy.tools.provider.itis import ItisAPI

app = Flask(__name__)

@app.errorhandler(BadRequest)
def handle_bad_request(e):
    return f"Bad request: {e}"


@app.errorhandler(InternalServerError)
def handle_bad_response(e):
    return f"Internal Server Error: {e}"

# .............................................................................
class _BrokerService:
    """Base S-to-the-N service, handles parameter names and acceptable values."""
    # overridden by subclasses
    SERVICE_TYPE = APIService.BrokerRoot

    # ...............................................
    @classmethod
    def _get_s2n_provider_response_elt(cls, query_term=None):
        provider_element = {}
        s2ncode = ServiceProvider.Broker[S2nKey.PARAM]
        provider_element[S2nKey.PROVIDER_CODE] = s2ncode
        provider_element[S2nKey.PROVIDER_LABEL] = ServiceProvider.Broker[S2nKey.NAME]
        icon_url = ServiceProvider.get_icon_url(s2ncode)
        if icon_url:
            provider_element[S2nKey.PROVIDER_ICON_URL] = icon_url
        # Status will be 200 if anyone ever sees this
        provider_element[S2nKey.PROVIDER_STATUS_CODE] = 200
        # # Handle local debugging
        # try:
        #     # TODO: get from headers
        #     base_url = "https://spcoco.org"
        #     # base_url = cherrypy.request.headers["Origin"]
        # except KeyError:
        #     base_url = "https://localhost"
        # Optional URL queries
        standardized_url = f"{get_host_url()}/{cls.SERVICE_TYPE['endpoint']}"
        if query_term:
            standardized_url = "{}?{}".format(standardized_url, query_term)
        provider_element[S2nKey.PROVIDER_QUERY_URL] = [standardized_url]
        return provider_element

    # ...............................................
    @classmethod
    def _order_providers(cls, provnames):
        provnames = list(provnames)
        provnames.sort()
        return provnames

    # ...............................................
    @classmethod
    def get_providers(cls, filter_params=None):
        """Return a list of strings indicating all providers valid for this service.

        Args:
            filter_params: todo - provider filter parameters.

        Returns:
            provnames: list of provider values, suitable for a URL query parameter.

        Note:
            * This returns a list of provider values in alphabetical order.  The values
                are used as URL query parameters.
            * The order of these providers determines the order of records returned for
                multi-provider responses.
        """
        provnames = set()
        # Ignore as-yet undefined filter_params
        for p in ServiceProvider.all():
            if cls.SERVICE_TYPE["name"] in p[S2nKey.SERVICES]:
                provnames.add(p[S2nKey.PARAM])
        provnames = cls._order_providers(provnames)
        return provnames

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
        endpoint = f"{APIEndpoint.broker_root()}/{cls.SERVICE_TYPE['endpoint']}"
        return endpoint

    # ...............................................
    @classmethod
    def get_endpoint(cls, **kwargs):
        """Return the http response for this class endpoint.

        Args:
            **kwargs: keyword arguments are accepted but ignored

        Returns:
            flask_app.broker.s2n_type.BrokerOutput object

        Raises:
            Exception: on unknown error.
        """
        try:
            valid_providers = cls.get_providers()
            output = cls._show_online(valid_providers)
        except Exception:
            raise
        return output.response

    # ...............................................
    @classmethod
    def _show_online(cls, providers):
        svc = cls.SERVICE_TYPE["name"]
        info = {
            "info": "Specify Network {} service is online.".format(svc)}

        param_lst = []
        for p, pdict in cls.SERVICE_TYPE["params"].items():
            pinfo = pdict.copy()
            pinfo["type"] = str(type(pinfo["type"]))
            if providers is not None and p == "provider":
                pinfo["options"] = list(providers)
            param_lst.append({p: pinfo})
        info["parameters"] = param_lst

        prov_meta = cls._get_s2n_provider_response_elt()

        output = BrokerOutput(0, svc, provider=prov_meta, errors=info)
        return output

    # ...............................................
    @classmethod
    def _get_badquery_output(cls, error_msg):
        svc = cls.SERVICE_TYPE["name"]
        errinfo = {"error": [error_msg]}
        prov_meta = cls._get_s2n_provider_response_elt()

        output = BrokerOutput(0, svc, provider=prov_meta, errors=errinfo)
        return output

    # ...............................................
    @classmethod
    def parse_name_with_gbif(cls, namestr):
        """Return the canonical name parsed from a complex scientific name by GBIF.

        Args:
            namestr: a taxonomic name

        Returns:
            namestr: a canonical name
        """
        # output is a dictionary containing a single taxonomic record
        output, _url = GbifAPI.parse_name(namestr)
        try:
            rec = output["record"]
        except KeyError:
            # Default to original namestring if parsing fails
            pass
        else:
            success = rec["parsed"]
            namestr = rec["canonicalName"]

            if success:
                if namestr.startswith("? "):
                    namestr = rec["scientificName"]
        return namestr

    # ...............................................
    def match_name_with_itis(self, namestr):
        """Return a valid canonical name returned from ITIS for a scientific name.

        Args:
            namestr: a taxonomic name

        Returns:
            namestr: a canonical name
        """
        # output is a flask_app.broker.s2n_type.BrokerOutput object
        output = ItisAPI.match_name(namestr, is_accepted=True)
        if output.record_count > 0:
            try:
                namestr = output.records[0]["nameWOInd"]
            except KeyError:
                # Default to original namestring if match fails
                pass
        return namestr

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

        param_meta = cls.SERVICE_TYPE["params"][key]
        # First see if restricted to options
        default_val = param_meta["default"]
        type_val = param_meta["type"]
        # If restricted options, check
        try:
            options = param_meta["options"]
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
        for key, param_meta in cls.SERVICE_TYPE["params"].items():
            val = user_kwargs[key]
            # Done in calling function
            if key == "provider":
                pass

            # Do not edit namestr, maintain capitalization
            elif key == "namestr":
                good_params["namestr"] = val

            # Require one valid icon_status
            elif key == "icon_status":
                valid_stat = param_meta["options"]
                if val is None:
                    errinfo = lmutil.add_errinfo(
                        errinfo, "error",
                        f"Parameter {key} containing one of {valid_stat} options is "
                        f"required")
                elif val not in valid_stat:
                    errinfo = lmutil.add_errinfo(
                        errinfo, "error",
                        f"Value {val} for parameter {key} not in valid options "
                        f"{valid_stat}")
                else:
                    good_params[key] = val

            elif val is not None:
                usr_val, valid_options = cls._fix_type_new(key, val)
                if valid_options is not None and val not in valid_options:
                    errinfo = lmutil.add_errinfo(
                        errinfo, "error",
                        f"Value {val} for parameter {key} is not in valid options "
                        f"{param_meta['options']}")
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
    def _get_providers_from_string(cls, usr_req_providers, filter_params=None):
        errinfo = {}

        valid_providers = cls.get_providers(filter_params=filter_params)
        # Allows None or comma-delimited list
        valid_requested_providers, invalid_providers = cls._get_valid_requested_params(
            usr_req_providers, valid_providers)

        if cls.SERVICE_TYPE != APIEndpoint.Badge:
            if valid_requested_providers:
                providers = valid_requested_providers
            else:
                providers = valid_providers
        else:
            if valid_requested_providers:
                providers = valid_requested_providers[0]
            else:
                providers = None
                errinfo = lmutil.add_errinfo(
                    errinfo, "error",
                    f"Parameter provider containing exactly one of {valid_providers} "
                    f"options is required")

        if invalid_providers:
            for ip in invalid_providers:
                errinfo = lmutil.add_errinfo(
                    errinfo, "warning",
                    f"Value {ip} for parameter provider not in valid options "
                    f"{valid_providers}")

        return providers, errinfo

    # ...............................................
    @classmethod
    def _standardize_params(
            cls, provider=None, namestr=None, is_accepted=False, gbif_parse=False,
            gbif_count=False, itis_match=False, kingdom=None,
            occid=None, gbif_dataset_key=None, count_only=False, url=None,
            icon_status=None, filter_params=None):
        """Standardize query parameters to send to appropriate service.

        Args:
            provider: provider keyword value for requested query.
            namestr: taxonomic name.
            is_accepted: flag indicating to restrict the results to accepted taxa.
            gbif_parse: True to parse a Scientific Name first using the GBIF parsing
                service.
            gbif_count: True to return a count from GBIF for a species name.
            itis_match: True to match with ITIS
            kingdom: Query taxon name in a specific kingdom (for names that appear in
                more than one kingdom).
            occid: Identifier for an occurrence record.
            gbif_dataset_key: Identifier for a GBIF dataset.
            count_only: True to return a count, not records.
            url: URL
            icon_status: keyword for returning a version of an icon.  Options are
                hover, active, inactive.
            filter_params: todo - provider filter parameters.

        Returns:
            a dictionary containing keys and properly formatted values for the
                user specified parameters.

        Note:
            filter_params is present to distinguish between providers for occ service by
            occurrence_id or by dataset_id.
        """
        user_kwargs = {
            "provider": provider,
            "namestr": namestr,
            "is_accepted": is_accepted,
            "gbif_parse": gbif_parse,
            "gbif_count": gbif_count,
            "itis_match": itis_match,
            "kingdom": kingdom,
            "occid": occid,
            "gbif_dataset_key": gbif_dataset_key,
            "count_only": count_only,
            "url": url,
            # "bbox": bbox,
            # "exceptions": exceptions,
            # "height": height,
            # "layers": layers,
            # "request": request,
            # "format": frmat,
            # "srs": srs,
            # "width": width,
            "icon_status": icon_status}

        providers, prov_errinfo = cls._get_providers_from_string(
            provider, filter_params=filter_params)
        usr_params, errinfo = cls._process_params(user_kwargs)
        # consolidate parameters and errors
        usr_params["provider"] = providers
        errinfo = lmutil.combine_errinfo(errinfo, prov_errinfo)

        # Remove gbif_parse and itis_match flags
        gbif_parse = itis_match = False
        try:
            gbif_parse = usr_params.pop("gbif_parse")
        except Exception:
            pass
        try:
            itis_match = usr_params.pop("itis_match")
        except Exception:
            pass
        # Replace namestr with GBIF-parsed namestr
        if namestr and (gbif_parse or itis_match):
            usr_params["namestr"] = cls.parse_name_with_gbif(namestr)

        return usr_params, errinfo

    # ..........................
    @staticmethod
    def OPTIONS():
        """Common options request for all services (needed for CORS)."""
        return


# .............................................................................
if __name__ == "__main__":
    kwarg_defaults = {
        "count_only": False,
        "width": 600,
        "height": 300,
        "type": [],
        }
