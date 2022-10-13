
from lmtrex.common.lmconstants import (APIService, SPECIFY_CACHE_API)
from lmtrex.flask_app.broker.base import _S2nService

# .............................................................................
class AddressSvc(_S2nService):
    """Query the Specify Resolver with a UUID for a resolvable GUID and URL"""
    SERVICE_TYPE = APIService.Address
    
    # ...............................................
    @classmethod
    def get_endpoint(cls, **kwargs):
        """Get address for the Specify Cache.
        Overrides _S2nService.get_endpoint
        
        Return:
            an API url for posting a DwCA file to be included in the Specify Cache
        """
        return SPECIFY_CACHE_API



# .............................................................................
if __name__ == '__main__':
    svc = AddressSvc()
    
    retval = svc.GET()
    print(retval)
    