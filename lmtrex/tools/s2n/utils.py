import sys
import traceback
from uuid import UUID

from lmtrex.common.lmconstants import ICON_API, ServiceProvider
from lmtrex.common.s2n_type import S2nEndpoint


# ......................................................
def is_valid_uuid(uuid_to_test, version=4):
    """Check if uuid_to_test is a valid UUID.
    
    Args:
        uuid_to_test : str
        version : {1, 2, 3, 4}
    
    Returns: `True` if uuid_to_test is a valid UUID, otherwise `False`.
    
    Examples:
        >>> is_valid_uuid('c9bf9e57-1685-4c89-bafb-ff5af830be8a')
        True
        >>> is_valid_uuid('c9bf9e58')
        False
    """
    
    try:
        uuid_obj = UUID(uuid_to_test, version=version)
    except ValueError:
        return False
    return str(uuid_obj) == uuid_to_test

# ..........................
def get_traceback():
    """Get the traceback for this exception"""
    exc_type, exc_val, this_traceback = sys.exc_info()
    tb = traceback.format_exception(exc_type, exc_val, this_traceback)
    tblines = []
    cr = '\n'
    for line in tb:
        line = line.rstrip(cr)
        parts = line.split(cr)
        tblines.extend(parts)
    trcbk = cr.join(tblines)
    return trcbk

# ...............................................
def get_icon_url(provider_code, icon_status=None):
    """Add link to badge service with provider param and optionally icon_status."""
    url = None
    # for function testing outside of cherrypy
    try:
        # TODO: get the URL from headers
        base_url = 'https://broker.spcoco.org'
        # base_url = cherrypy.request.headers['Origin']
    except:
        base_url = 'https://localhost'
        
    if ServiceProvider.is_valid_service(provider_code, S2nEndpoint.Badge):
        url = '{}{}?provider={}'.format(
            base_url,
            ICON_API,
            provider_code
        )
        if icon_status:
            url = '{}&icon_status={}'.format(url, icon_status)
    return url
    

# ...............................................
def combine_errinfo(errinfo1, errinfo2):
    """Combine 2 dictionaries with keys 'error', 'warning' and 'info'"""
    errinfo = {}
    for key in ('error', 'warning', 'info'):
        try:
            lst = errinfo1[key]
        except:
            lst = []
        try:
            lst2 = errinfo2[key]
        except:
            lst2 = []
        
        if lst or lst2:
            lst.extend(lst2)
            errinfo[key] = lst
    return errinfo

# ...............................................
def add_errinfo(errinfo, key, val):
    """Combine 2 dictionaries with keys 'error', 'warning' and 'info'"""
    if val and key in ('error', 'warning', 'info'):
        try:
            errinfo[key].append(val)
        except:
            errinfo[key] = [val]
    return errinfo
    

if __name__ == '__main__':
    import doctest
    doctest.testmod()