"""Random tools used frequently in Specify Network."""
import sys
import traceback
from uuid import UUID

# from flask_app.broker.constants import ICON_API, ServiceProvider
# from flask_app.common.s2n_type import APIEndpoint


# ......................................................
def is_valid_uuid(uuid_to_test, version=4):
    """Check if uuid_to_test is a valid UUID.

    Args:
        uuid_to_test (str): UUID with 5 parts, separated by -, each with hex chars.
        version : {1, 2, 3, 4}

    Returns:
        bool: `True` if uuid_to_test is a valid UUID, otherwise `False`.

    Examples:
        >>> is_valid_uuid("c9bf9e57-1685-4c89-bafb-ff5af830be8a")
        True
        >>> is_valid_uuid("c9bf9e58")
        False
    """
    try:
        uuid_obj = UUID(uuid_to_test, version=version)
    except ValueError:
        return False
    return str(uuid_obj) == uuid_to_test


# ..........................
def get_traceback():
    """Get the traceback for this exception.

    Returns:
        trcbk: traceback of steps executed before an exception
    """
    exc_type, exc_val, this_traceback = sys.exc_info()
    tb = traceback.format_exception(exc_type, exc_val, this_traceback)
    tblines = []
    cr = "\n"
    for line in tb:
        line = line.rstrip(cr)
        parts = line.split(cr)
        tblines.extend(parts)
    trcbk = cr.join(tblines)
    return trcbk


# # ...............................................
# def get_icon_url(provider_code, icon_status=None):
#     """Get a URL to the badge service with provider param and optionally icon_status.
#
#     Args:
#         provider_code: code for provider to get an icon for.
#         icon_status: one of flask_app.broker.constants.VALID_ICON_OPTIONS:
#             active, inactive, hover
#
#     Returns:
#         URL of for the badge API
#     """
#     url = None
#     try:
#         # TODO: get the URL from headers
#         base_url = "https://broker-dev.spcoco.org"
#         # base_url = cherrypy.request.headers["Origin"]
#     except Exception:
#         base_url = "https://localhost"
#
#     if ServiceProvider.is_valid_service(provider_code, APIEndpoint.Badge):
#         url = f"{base_url}{ICON_API}/{provider_code}"
#         if icon_status:
#             url = f"{url}&icon_status={icon_status}"
#     return url
#

# ...............................................
def combine_errinfo(errinfo1, errinfo2):
    """Combine 2 dictionaries with keys `error`, `warning` and `info`.

    Args:
        errinfo1: dictionary of errors
        errinfo2: dictionary of errors

    Returns:
        dictionary of errors
    """
    errinfo = {}
    for key in ("error", "warning", "info"):
        try:
            lst = errinfo1[key]
        except KeyError:
            lst = []
        try:
            lst2 = errinfo2[key]
        except KeyError:
            lst2 = []

        if lst or lst2:
            lst.extend(lst2)
            errinfo[key] = lst
    return errinfo


# ...............................................
def add_errinfo(errinfo, key, val):
    """Add to a dictionary with keys `error`, `warning` and `info`.

    Args:
        errinfo: dictionary of errors
        key: error type, `error`, `warning` or `info`
        val: error message

    Returns:
        updated dictionary of errors
    """
    if errinfo is None:
        errinfo = {}
    if val and key in ("error", "warning", "info"):
        try:
            errinfo[key].append(val)
        except KeyError:
            errinfo[key] = [val]
    return errinfo
