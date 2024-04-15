"""Parent Class for the Specify Network API services."""
from werkzeug.exceptions import BadRequest

from sppy.tools.s2n.utils import add_errinfo, combine_errinfo, get_traceback
from flask_app.common.base import _SpecifyNetworkService
from flask_app.common.s2n_type import (
    APIEndpoint, APIService, BrokerOutput, get_host_url, S2nKey, ServiceProvider)
from sppy.tools.provider.gbif import GbifAPI
from sppy.tools.provider.itis import ItisAPI


# .............................................................................
class _BrokerService(_SpecifyNetworkService):
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
                errinfo = add_errinfo(
                    errinfo, "error",
                    f"Parameter provider containing exactly one of {valid_providers} "
                    f"options is required")

        if invalid_providers:
            for ip in invalid_providers:
                errinfo = add_errinfo(
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

        Raises:
            BadRequest: on invalid query parameters.
            BadRequest: on unknown exception when parsing request
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
            "icon_status": icon_status}

        providers, errinfo = cls._get_providers_from_string(
            provider, filter_params=filter_params)

        try:
            usr_params, param_errinfo = cls._process_params(user_kwargs)
        except Exception:
            error_description = get_traceback()
            raise BadRequest(error_description)

        # consolidate parameters and errors
        usr_params["provider"] = providers
        errinfo = combine_errinfo(errinfo, param_errinfo)

        # errinfo["error"] indicates bad parameters, throws exception
        try:
            error_description = "; ".join(errinfo["error"])
            raise BadRequest(error_description)
        except KeyError:
            pass

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


# .............................................................................
if __name__ == "__main__":
    pass
