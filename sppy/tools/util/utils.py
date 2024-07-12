"""Random tools used frequently in Specify Network."""
import boto3
from botocore.exceptions import ClientError, SSLError
from io import StringIO
from numpy import integer as np_int, floating as np_float, ndarray
import os
from pprint import pp
import sys
import traceback
from uuid import UUID


# ......................................................
def convert_np_vals_for_json(obj):
    """Encode numpy values (from matrix operations) for JSON output.

    Args:
        obj: a simple numpy object, value or array

    Returns:
        an object serializable by JSON

    Note:
        from https://stackoverflow.com/questions/27050108/convert-numpy-type-to-python
    """
    if isinstance(obj, np_int):
        return int(obj)
    elif isinstance(obj, np_float):
        return float(obj)
    elif isinstance(obj, ndarray):
        return obj.tolist()
    else:
        return obj


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
    if key in ("error", "warning", "info"):
        if isinstance(val, str):
            val_lst = [val]
        try:
            errinfo[key].extend(val_lst)
        except KeyError:
            errinfo[key] = val_lst
    return errinfo

# ...............................................
def upload_to_s3(full_filename, bucket, bucket_path, region):
    """Upload a file to S3.

    Args:
        full_filename (str): Full filename to the file to upload.
        bucket (str): Bucket identifier on S3.
        bucket_path (str): Parent folder path to the S3 data.
        region (str): AWS region to upload to.

    Returns:
        s3_filename (str): path including bucket, bucket_folder, and filename for the
            uploaded data
    """
    s3_filename = None
    s3_client = boto3.client("s3", region_name=region)
    obj_name = os.path.basename(full_filename)
    if bucket_path:
        obj_name = f"{bucket_path}/{obj_name}"
    try:
        s3_client.upload_file(full_filename, bucket, obj_name)
    except SSLError:
        raise Exception(f"Failed with SSLError to upload {obj_name} to {bucket}")
    except ClientError as e:
        raise Exception(f"Failed to upload {obj_name} to {bucket}, ({e})")
    else:
        s3_filename = f"s3://{bucket}/{obj_name}"
        print(f"Uploaded {s3_filename} to S3")
    return s3_filename


# ......................................................
def prettify_object(print_obj):
    """Format an object for output.

    Args:
        print_obj (obj): Object to pretty print in output

    Returns:
        formatted string representation of object

    Note: this splits a string containing spaces in a list to multiple strings in the
        list.
    """
    strm = StringIO()
    pp(print_obj, stream=strm)
    obj_str = strm.getvalue()
    return obj_str
