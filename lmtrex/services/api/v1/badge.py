import cherrypy
from io import BytesIO
import os

from lmtrex.common.lmconstants import (ServiceProvider, APIService, ICON_OPTIONS, ICON_CONTENT)

from lmtrex.tools.utils import get_traceback

from lmtrex.common.lmconstants import IMG_PATH

from lmtrex.services.api.v1.base import _S2nService
from lmtrex.services.api.v1.s2n_type import S2nKey

# .............................................................................
@cherrypy.expose
# @cherrypy.popargs('path_occ_id')
class BadgeSvc(_S2nService):
    SERVICE_TYPE = APIService.Badge

    # ...............................................
    def get_icon(self, provider, icon_status):
        # GBIF
        if provider == ServiceProvider.GBIF[S2nKey.PARAM]:
            fname = ServiceProvider.GBIF['icon'][icon_status]
        # iDigBio
        elif provider == ServiceProvider.iDigBio[S2nKey.PARAM]:
            fname = ServiceProvider.iDigBio['icon'][icon_status]
        # iDigBio
        elif provider == ServiceProvider.Lifemapper[S2nKey.PARAM]:
            fname = ServiceProvider.Lifemapper['icon'][icon_status]
        # MorphoSource
        elif provider == ServiceProvider.MorphoSource[S2nKey.PARAM]:
            fname = ServiceProvider.MorphoSource['icon'][icon_status]
        # Specify
        elif provider == ServiceProvider.Specify[S2nKey.PARAM]:
            fname = ServiceProvider.Specify['icon'][icon_status]
            
        full_filename = os.path.join(IMG_PATH, fname)

        return full_filename

    # ...............................................
    # ...............................................
    @cherrypy.tools.json_out()
    def GET(self, provider=None, icon_status=None, stream=True, **kwargs):
        """Get one icon to indicate a provider in a GUI
        
        Args:
            provider: string containing a comma delimited list of provider codes.  The icon 
                for only the first provider will be returned.  If the string is not present
                or 'all', the first provider in the default list of providers will be returned.
            icon_status: string indicating which version of the icon to return, valid options are:
                lmtrex.common.lmconstants.ICON_OPTIONS (active, inactive, hover) 
            stream: If true, return a generator for streaming output, else return
                file contents
            kwargs: any additional keyword arguments are ignored

        Return:
            a file containing the requested icon
        """
        try:
            usr_params = self._standardize_params(provider=provider, icon_status=icon_status)
        except Exception as e:
            traceback = get_traceback()
            return traceback
        
        # Who to query
        # TODO: currently only allow one for now, default is first one in list
        valid_providers = self.get_providers()
        valid_req_providers, _ = self.get_valid_requested_providers(
            usr_params['provider'], valid_providers)
        if len(valid_req_providers) > 0:
            provider = valid_req_providers.pop()
        
        # Find file
        icon_status = usr_params['icon_status']
        try:
            icon_fname = self.get_icon(provider, icon_status)
        except Exception as e:
            traceback = get_traceback()
            return traceback

        ifile = open(icon_fname, mode='r')
        ret_file_name = os.path.basename(icon_fname)
        cherrypy.response.headers[
            'Content-Disposition'] = 'attachment; filename="{}"'.format(ret_file_name)
        cherrypy.response.headers['Content-Type'] = ICON_CONTENT
        
        if stream:
            return cherrypy.lib.file_generator(ifile)
        else:
            icontent = ifile.read()
            ifile.close()
            return icontent
    
        # cherrypy.lib.static.serve_file(icon_fname, 'application/x-download',
        #                  'attachment', os.path.basename(icon_fname))
    

# .............................................................................
if __name__ == '__main__':
    svc = BadgeSvc()
    # Get all providers
    valid_providers = svc.get_providers()
    for pr in valid_providers:
        for stat in ICON_OPTIONS:
            svc.GET(provider=pr, icon_status=stat)
    