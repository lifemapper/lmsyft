"""Class for the Specify Network badge (icon) API service."""
from flask import json, send_file
import io
import os
from werkzeug.exceptions import (BadRequest, InternalServerError)

from flask_app.broker.constants import (
    APIService, ICON_CONTENT, ICON_DIR, ServiceProvider, VALID_ICON_OPTIONS)
from flask_app.broker.s2n_type import S2nKey

from sppy.tools.s2n.utils import get_traceback

from flask_app.broker.base import _S2nService


# .............................................................................
class BadgeSvc(_S2nService):
    """Specify Network API service for retrieving icon images."""
    SERVICE_TYPE = APIService.Badge

    # ...............................................
    @classmethod
    def _get_icon_filename(cls, provider, icon_status):
        icon_fname = None
        try:
            # GBIF
            if provider == ServiceProvider.GBIF[S2nKey.PARAM]:
                icon_fname = ServiceProvider.GBIF["icon"][icon_status]
            # iDigBio
            elif provider == ServiceProvider.iDigBio[S2nKey.PARAM]:
                icon_fname = ServiceProvider.iDigBio["icon"][icon_status]
            # ITIS
            elif provider == ServiceProvider.ITISSolr[S2nKey.PARAM]:
                icon_fname = ServiceProvider.ITISSolr["icon"][icon_status]
            # MorphoSource
            elif provider == ServiceProvider.MorphoSource[S2nKey.PARAM]:
                icon_fname = ServiceProvider.MorphoSource["icon"][icon_status]
            # WoRMS
            elif provider == ServiceProvider.WoRMS[S2nKey.PARAM]:
                icon_fname = ServiceProvider.WoRMS["icon"][icon_status]

        except Exception:
            error_description = get_traceback()
            raise InternalServerError(error_description)

        return icon_fname

    # ...............................................
    @classmethod
    def _get_json_service_info(cls, output):
        # cherrypy.response.headers["Content-Type"] = "application/json"
        # import json
        boutput = bytes(json.dumps(output.response), "utf-8")
        return boutput

    # ...............................................
    @classmethod
    def get_icon(
            cls, provider=None, icon_status=None, stream=True, app_path="", **kwargs):
        """Get one icon to indicate a provider in a GUI.

        Args:
            provider: comma-delimited list of requested provider codes.  Codes are
                delimited for each in lmtrex.common.lmconstants ServiceProvider
            icon_status: string indicating which version of the icon to return,
                valid options are:
                    lmtrex.common.lmconstants.VALID_ICON_OPTIONS
                    (active, inactive, hover)
            stream: If true, return a generator for streaming output, else return file
                contents.
            app_path: Base application path used for locating the icon files.
            **kwargs: any additional keyword arguments are ignored

        Returns:
            a file containing the requested icon

        Raises:
            BadRequest: on invalid query parameters.
            BadRequest: on unknown exception when parsing request
            NotImplementedError: on request for a non-supported provider icon.
        """
        # return info for empty request
        if provider is None and icon_status is None:
            return cls.get_endpoint()

        try:
            good_params, errinfo = cls._standardize_params(
                provider=provider, icon_status=icon_status)
            # Bad parameters
            try:
                error_description = "; ".join(errinfo["error"])
                raise BadRequest(error_description)
            except Exception:
                pass

        except Exception:
            # Unknown error
            error_description = get_traceback()
            raise BadRequest(error_description)

        icon_basename = cls._get_icon_filename(
            good_params["provider"], good_params["icon_status"])
        icon_fname = os.path.join(app_path, ICON_DIR, icon_basename)

        if icon_fname is not None:
            if stream:
                return send_file(
                    icon_fname, mimetype=ICON_CONTENT, as_attachment=False)
            else:
                return send_file(
                    icon_fname, mimetype=ICON_CONTENT, as_attachment=True,
                    attachment_filename=icon_fname)

        else:
            raise NotImplementedError(
                f"Badge {icon_status} not implemented for provider {provider}")


# .............................................................................
if __name__ == "__main__":
    svc = BadgeSvc()
    # Get all providers
    valid_providers = svc.get_providers()
    for pr in valid_providers:
        for stat in VALID_ICON_OPTIONS:
            retval = svc.get_icon(
                provider=pr, icon_status=stat,
                app_path="/home/astewart/git/sp_network/sppy/frontend")
            print(retval)
