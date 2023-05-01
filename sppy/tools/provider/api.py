"""Module containing functions for API Queries."""
from http import HTTPStatus
from logging import WARN
import requests
import urllib

from flask_app.common.s2n_type import BrokerOutput, S2nKey, ServiceProvider
from flask_app.common.constants import ENCODING, URL_ESCAPES

from sppy.tools.util.logtools import logit
from sppy.tools.s2n.lm_xml import fromstring, deserialize
from sppy.tools.s2n.utils import add_errinfo, get_traceback


# .............................................................................
class APIQuery:
    """Class to query APIs and return results.

    Note:
        CSV files are created with tab delimiter
    """
    # Not implemented in base class
    PROVIDER = ServiceProvider.Broker
    Q_KEY = "q"

    def __init__(
            self, base_url, q_filters=None, other_filters=None,
            filter_string=None, headers=None, logger=None):
        """Constructor.

        Args:
            base_url: URL for the API to query.
            q_filters: dictionary of filters for the q element of a solr query.
            other_filters: dictionary of other filters.
            filter_string: assembled URL query string.
            headers: any headers to be sent to the server
            logger: object for logging messages and errors.
        """
        self.logger = logger
        self.headers = {} if headers is None else headers
        # No added filters are on url (unless initialized with filters in url)
        self.base_url = base_url
        self._q_filters = {} if q_filters is None else q_filters
        self._other_filters = {} if other_filters is None else other_filters
        self.filter_string = self._assemble_filter_string(
            filter_string=filter_string)
        self.output = None
        self.error = None
        self.debug = False

    # ...............................................
    @classmethod
    def _standardize_record(cls, rec):
        # Standardize record to common schema - implemented in subclasses
        raise Exception("Not implemented in base class")

    # ...............................................
    @classmethod
    def _standardize_output(
            cls, output, count_key, records_key, record_format, service,
            provider_meta, query_urls=None, count_only=False, errinfo=None):
        if output is None:
            raise Exception(f"Failed to return output from {cls.url}")

        stdrecs = []
        total = 0

        # Count
        try:
            total = output[count_key]
        except KeyError:
            msg = cls._get_error_message(msg=f"Missing `{count_key}` element")
            add_errinfo(errinfo, "warning", msg)
        # Records
        if not count_only:
            try:
                recs = output[records_key]
            except KeyError:
                msg = cls._get_error_message(
                    msg=f"Output missing `{records_key}` element")
                errinfo = add_errinfo(errinfo, "warning", msg)
            else:
                for r in recs:
                    try:
                        stdrecs.append(cls._standardize_record(r))
                    except Exception as e:
                        msg = cls._get_error_message(err=e)
                        errinfo = add_errinfo(errinfo, "error", msg)

        # prov_meta = cls._get_provider_response_elt(
        #     query_status=query_status, query_urls=query_urls)
        std_output = BrokerOutput(
            total, service, provider=provider_meta, record_format=record_format,
            records=stdrecs, errors=errinfo)

        return std_output

    # .....................................
    @classmethod
    def _get_error_message(cls, msg=None, err=None):
        text = cls.__name__
        if msg is not None:
            text = "{}; {}".format(text, msg)
        if err is not None:
            text = "{}; (exception: {})".format(text, err)
        return text

    # ...............................................
    @classmethod
    def _get_code2description_dict(cls, code_lst, code_map):
        # May contain "issues"
        code_dict = {}
        if code_lst:
            for tmp in code_lst:
                code = tmp.strip()
                try:
                    code_dict[code] = code_map[code]
                except KeyError:
                    code_dict[code] = code
        return code_dict

    # ...............................................
    @classmethod
    def _get_query_fail_output(cls, broker_url, query_urls, api_endpoint):
        errinfo = {"error": [cls._get_error_message(err=get_traceback())]}
        prov_meta = cls._get_provider_response_elt(
            broker_url, query_status=HTTPStatus.INTERNAL_SERVER_ERROR,
            query_urls=query_urls)
        std_output = BrokerOutput(
            0, api_endpoint, provider=prov_meta, errors=errinfo)

    # ...............................................
    @classmethod
    def _get_provider_response_elt(cls, broker_url, query_status=None, query_urls=None):
        provider_element = {}
        provcode = cls.PROVIDER[S2nKey.PARAM]
        provider_element[S2nKey.PROVIDER_CODE] = provcode
        provider_element[S2nKey.PROVIDER_LABEL] = cls.PROVIDER[S2nKey.NAME]
        icon_url = ServiceProvider.get_icon_url(broker_url, provcode)
        if icon_url:
            provider_element[S2nKey.PROVIDER_ICON_URL] = icon_url
        # Optional http status_code
        if query_status is not None:
            try:
                stat = int(query_status)
            except ValueError:
                try:
                    stat = max(query_status)
                except ValueError:
                    stat = None
            if stat:
                provider_element[S2nKey.PROVIDER_STATUS_CODE] = stat
        # Optional URL queries
        if query_urls:
            provider_element[S2nKey.PROVIDER_QUERY_URL] = query_urls
        return provider_element

    # .....................................
    @classmethod
    def init_from_url(cls, url, headers=None, logger=None):
        """Initialize APIQuery from a url.

        Args:
            url (str): The url to use as the base
            headers (dict): Headers to use for query
            logger: logger for info and error messages.

        Returns:
            qry: APIQuery object
        """
        if headers is None:
            headers = {}
        base, filters = url.split("?")
        qry = APIQuery(
            base, filter_string=filters, headers=headers, logger=logger)
        return qry

    # .........................................
    @property
    def url(self):
        """Retrieve a url for the query.

        Returns:
            The URL for a query object.
        """
        # All filters added to url
        if self.filter_string and len(self.filter_string) > 1:
            return "{}?{}".format(self.base_url, self.filter_string)

        return self.base_url

    # ...............................................
    def add_filters(self, q_filters=None, other_filters=None):
        """Add or replace filters.

        Args:
            q_filters: dictionary of filters for the q element of a solr query.
            other_filters: dictionary of other filters for a solr query.

        Note:
            This does not remove existing filters unless they are replaced
        """
        self.output = None
        q_filters = {} if q_filters is None else q_filters
        other_filters = {} if other_filters is None else other_filters

        for k, val in q_filters.items():
            self._q_filters[k] = val
        for k, val in other_filters.items():
            self._other_filters[k] = val
        self.filter_string = self._assemble_filter_string()

    # ...............................................
    def clear_all(self, q_filters=True, other_filters=True):
        """Clear existing q_filters, other_filters, and output.

        Args:
            q_filters: dictionary of filters for the q element of a solr query.
            other_filters: dictionary of other filters for a solr query.
        """
        self.output = None
        if q_filters:
            self._q_filters = {}
        if other_filters:
            self._other_filters = {}
        self.filter_string = self._assemble_filter_string()

    # ...............................................
    def clear_other_filters(self):
        """Clear existing other_filters and output."""
        self.clear_all(other_filters=True, q_filters=False)

    # ...............................................
    def clear_q_filters(self):
        """Clear existing q_filters and output."""
        self.clear_all(other_filters=False, q_filters=True)

    # ...............................................
    def _burrow(self, key_list):
        this_dict = self.output
        if isinstance(this_dict, dict):
            for key in key_list:
                try:
                    this_dict = this_dict[key]
                except KeyError:
                    raise Exception("Missing key {} in output".format(key))
        else:
            raise Exception("Invalid output type ({})".format(type(this_dict)))
        return this_dict

    # ...............................................
    def _assemble_filter_string(self, filter_string=None):
        # Assemble key/value pairs
        if filter_string is None:
            all_filters = self._other_filters.copy()
            if self._q_filters:
                q_val = self._assemble_q_val(self._q_filters)
                all_filters[self.Q_KEY] = q_val
            for k, val in all_filters.items():
                if isinstance(val, bool):
                    val = str(val).lower()
#                 elif isinstance(val, str):
#                     for oldstr, newstr in URL_ESCAPES:
#                         val = val.replace(oldstr, newstr)
                # works for GBIF, iDigBio, ITIS web services (no manual escaping)
                all_filters[k] = str(val).encode(ENCODING)
            filter_string = urllib.parse.urlencode(all_filters)
        # Escape filter string
        else:
            for oldstr, newstr in URL_ESCAPES:
                filter_string = filter_string.replace(oldstr, newstr)

        return filter_string

    # ...............................................
    @classmethod
    def _interpret_q_clause(cls, key, val, logger=None):
        clause = None
        if isinstance(val, (float, int, str)):
            clause = "{}:{}".format(key, str(val))
        # Tuple for negated or range value
        elif isinstance(val, tuple):
            # negated filter
            if isinstance(val[0], bool) and val[0] is False:
                clause = "NOT " + key + ":" + str(val[1])
            # range filter (better be numbers)
            elif isinstance(
                    val[0], (float, int)) and isinstance(val[1], (float, int)):
                clause = "{}:[{} TO {}]".format(key, str(val[0]), str(val[1]))
            else:
                logit(
                    logger, f"Unexpected value type {val}",
                    refname=cls.__class__.__name, log_level=WARN)
        else:
            logit(
                logger, f"Unexpected value type {val}", refname=cls.__class__.__name__,
                log_level=WARN)
        return clause

    # ...............................................
    def _assemble_q_item(self, key, val):
        itm_clauses = []
        # List for multiple values of same key
        if isinstance(val, list):
            for list_val in val:
                itm_clauses.append(self._interpret_q_clause(key, list_val))
        else:
            itm_clauses.append(self._interpret_q_clause(key, val))
        return itm_clauses

    # ...............................................
    def _assemble_q_val(self, q_dict):
        clauses = []
        q_val = ""
        # interpret dictionary
        for key, val in q_dict.items():
            clauses.extend(self._assemble_q_item(key, val))
        # convert to string
        first_clause = ""
        for cls in clauses:
            if not first_clause and not cls.startswith("NOT"):
                first_clause = cls
            elif cls.startswith("NOT"):
                q_val = " ".join((q_val, cls))
            else:
                q_val = " AND ".join((q_val, cls))
        q_val = first_clause + q_val
        return q_val

    # # ...............................................
    # @classmethod
    # def get_api_failure(
    #         cls, broker_url, service, provider_response_status, errinfo=None):
    #     """Output format for all (soon) API queries.
    #
    #     Args:
    #         service: type of Specify Network service
    #         provider_response_status: HTTPStatus of provider query
    #         errinfo: dictionary of info messages, warnings, errors
    #
    #     Returns:
    #         flask_app.broker.s2n_type.BrokerOutput object
    #     """
    #     prov_meta = cls._get_provider_response_elt(
    #         broker_url, query_status=provider_response_status)
    #     return BrokerOutput(0, service, provider=prov_meta, errors=errinfo)

    # ...............................................
    def query_by_get(self, output_type="json", verify=True):
        """Query the API, setting the output attribute to a JSON or ElementTree object.

        Args:
            output_type: data type of body of URL GET response
            verify: boolean indicating whether to verify the response.

        Note:
            Sets a single error message, not a list, to error attribute
        """
        self.output = {}
        self.error = None
        self.status_code = None
        self.reason = None
        errmsg = None
        try:
            if verify:
                response = requests.get(self.url, headers=self.headers)
            else:
                response = requests.get(self.url, headers=self.headers, verify=False)
        except Exception as e:
            errmsg = self._get_error_message(err=e)
        else:
            # Save server status
            try:
                self.status_code = response.status_code
                self.reason = response.reason
            except Exception:
                self.status_code = HTTPStatus.INTERNAL_SERVER_ERROR
                self.reason = "Unknown API status_code/reason"
            # Parse response
            if response.status_code == HTTPStatus.OK:
                if output_type == "json":
                    try:
                        self.output = response.json()
                    except Exception:
                        output = response.content
                        if output.find(b"<html") != -1:
                            self.status_code = HTTPStatus.INTERNAL_SERVER_ERROR
                            errmsg = self._get_error_message(
                                msg="Provider error",
                                err="Invalid JSON response ({})".format(output))
                        else:
                            try:
                                self.output = deserialize(fromstring(output))
                            except Exception:
                                self.status_code = HTTPStatus.INTERNAL_SERVER_ERROR
                                errmsg = self._get_error_message(
                                    msg="Provider error",
                                    err=f"Unrecognized output {output}")
                elif output_type == "xml":
                    try:
                        output = fromstring(response.text)
                        self.output = output
                    except Exception:
                        self.output = response.text
                else:
                    self.status_code = HTTPStatus.INTERNAL_SERVER_ERROR
                    errmsg = self._get_error_message(
                        msg=f"Unrecognized output type {output_type}")
            else:
                errmsg = self._get_error_message(
                    msg=f"URL {self.base_url}, code = {self.status_code}, reason = {self.reason}")

        if errmsg:
            self.error = errmsg

    # ...........    ....................................
    def query_by_post(self, output_type="json", file=None):
        """Perform a POST request.

        Args:
            output_type: data type of body of post response
            file: optional file to post to the API.
        """
        self.output = None
        self.error = None
        errmsg = None
        # Post a file
        if file is not None:
            # TODO: send as bytes here?
            files = {"files": open(file, "rb")}
            try:
                response = requests.post(self.base_url, files=files)
            except Exception as e:
                self.status_code = HTTPStatus.INTERNAL_SERVER_ERROR
                self.reason = f"Error posting to {self.base_url} {e}"
            else:
                self.status_code = response.status_code
                self.reason = response.reason

        # Post parameters
        else:
            all_params = self._other_filters.copy()
            if self._q_filters:
                all_params[self.Q_KEY] = self._q_filters
            query_as_string = urllib.parse.urlencode(all_params)
            url = f"{self.base_url}/?{query_as_string}"
            try:
                response = requests.post(url, headers=self.headers)
            except Exception as e:
                self.status_code = HTTPStatus.INTERNAL_SERVER_ERROR
                self.reason = f"Error posting to {self.base_url} {e}"
            else:
                self.status_code = response.status_code
                self.reason = response.reason

        # Parse response
        if response.ok:
            try:
                if output_type == "json":
                    try:
                        self.output = response.json()
                    except Exception:
                        output = response.content
                        self.output = deserialize(fromstring(output))
                elif output_type == "xml":
                    output = response.text
                    self.output = deserialize(fromstring(output))
                else:
                    errmsg = f"Unrecognized output type {output_type}"
            except Exception as e:
                errmsg = self._get_error_message(
                    msg=f"Unrecognized output, {self.base_url} --> {response.content}",
                    err=e)
        else:
            errmsg = self._get_error_message(
                msg=f"URL {self.base_url}, code = {self.status_code}, "
                    f"reason = {self.reason}"
            )

        if errmsg is not None:
            self.error = errmsg
