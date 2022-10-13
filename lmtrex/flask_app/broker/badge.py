from flask import json, send_file
import io
import os
from werkzeug.exceptions import (BadRequest, InternalServerError, NotImplemented)

from lmtrex.common.lmconstants import (
    APIService, ICON_CONTENT, ICON_DIR, ServiceProvider, VALID_ICON_OPTIONS)
from lmtrex.common.s2n_type import S2nKey

from lmtrex.tools.s2n.utils import get_traceback

from lmtrex.flask_app.broker.base import _S2nService


# .............................................................................
class BadgeSvc(_S2nService):
    SERVICE_TYPE = APIService.Badge

    # ...............................................
    @classmethod
    def _get_icon_filename(cls, provider, icon_status):
        icon_fname = None
        try:
            # GBIF
            if provider == ServiceProvider.GBIF[S2nKey.PARAM]:
                icon_fname = ServiceProvider.GBIF['icon'][icon_status]
            # iDigBio
            elif provider == ServiceProvider.iDigBio[S2nKey.PARAM]:
                icon_fname = ServiceProvider.iDigBio['icon'][icon_status]
            # ITIS
            elif provider == ServiceProvider.ITISSolr[S2nKey.PARAM]:
                icon_fname = ServiceProvider.ITISSolr['icon'][icon_status]
            # Lifemapper
            elif provider == ServiceProvider.Lifemapper[S2nKey.PARAM]:
                icon_fname = ServiceProvider.Lifemapper['icon'][icon_status]
            # MorphoSource
            elif provider == ServiceProvider.MorphoSource[S2nKey.PARAM]:
                icon_fname = ServiceProvider.MorphoSource['icon'][icon_status]
            # Specify
            elif provider == ServiceProvider.Specify[S2nKey.PARAM]:
                icon_fname = ServiceProvider.Specify['icon'][icon_status]
            # WoRMS
            elif provider == ServiceProvider.WoRMS[S2nKey.PARAM]:
                icon_fname = ServiceProvider.WoRMS['icon'][icon_status]
                
        except Exception as e:
            error_description = get_traceback()
            raise InternalServerError(error_description)
        
        return icon_fname


    # ...............................................
    @classmethod
    def _get_json_service_info(cls, output):
        # cherrypy.response.headers['Content-Type'] = 'application/json'
        # import json
        boutput = bytes(json.dumps(output.response), 'utf-8')
        return boutput

    # ...............................................
    @classmethod
    def get_icon(cls, provider=None, icon_status=None, stream=True, app_path='', **kwargs):
        """Get one icon to indicate a provider in a GUI
        
        Args:
            provider: comma-delimited list of requested provider codes.  Codes are delimited
                for each in lmtrex.common.lmconstants ServiceProvider
            icon_status: string indicating which version of the icon to return, valid options are:
                lmtrex.common.lmconstants.VALID_ICON_OPTIONS (active, inactive, hover) 
            stream: If true, return a generator for streaming output, else return file contents.
            kwargs: any additional keyword arguments are ignored

        Return:
            a file containing the requested icon
        """
        # return info for empty request
        if provider is None and icon_status is None:
            return cls.get_endpoint()

        try:
            good_params, errinfo = cls._standardize_params(
                provider=provider, icon_status=icon_status)
            # Bad parameters
            try:
                error_description = '; '.join(errinfo['error'])                            
                raise BadRequest(error_description)
            except:
                pass
                
        except Exception as e:
            # Unknown error
            error_description = get_traceback()
            raise InternalServerError(error_description)

        icon_basename = cls._get_icon_filename(good_params['provider'], good_params['icon_status'])
        icon_fname = os.path.join(app_path, ICON_DIR, icon_basename)
        
        if icon_fname is not None:
            ifile = open(icon_fname, mode='rb')
            image_binary = ifile.read()
            if stream:
                return send_file(
                    io.BytesIO(image_binary), mimetype=ICON_CONTENT, as_attachment=False)
            else:
                return send_file(
                    io.BytesIO(image_binary), mimetype=ICON_CONTENT, as_attachment=True, 
                    attachment_filename=icon_fname)
            # # Return image data or file
            # try:
            #     ifile = open(icon_fname, mode='rb')
            #     image_binary = io.BytesIO(ifile.read())
            # except Exception as e:
            #     # Unknown error
            #     error_description = get_traceback()
            #     raise InternalServerError(error_description)
            # else:
            #     if stream:
            #         return send_file(
            #             io.BytesIO(image_binary), mimetype=ICON_CONTENT, as_attachment=False)
            #     else:
            #         return send_file(
            #             io.BytesIO(image_binary), mimetype=ICON_CONTENT, as_attachment=True, 
            #             attachment_filename=icon_fname)
            # finally:
            #     ifile.close()
            
        else:
            error_description = 'Badge {} not implemented for provider {}'.format(
                icon_status, provider)
            raise NotImplemented(error_description)


# .............................................................................
if __name__ == '__main__':
    svc = BadgeSvc()
    # Get all providers
    valid_providers = svc.get_valid_providers()
    for pr in valid_providers:
        for stat in VALID_ICON_OPTIONS:
            retval = svc.get_icon(provider=pr, icon_status=stat)
            print(retval)
    