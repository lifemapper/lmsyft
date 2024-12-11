"""Functions to test the span.tools.util.dwca.DwCArchive with known URLs."""
import os
import shutil
import time

from sppy.tools.util.dwca import (
    assemble_download_filename, DwCArchive, get_dwca_urls, download_dwca)
from sppy.tools.util.utils import is_valid_uuid
from flask_app.broker.constants import (DWCA, TST_VALUES)

today = time.localtime()
DATE_STR = f"{today.tm_year}.{today.tm_mon}.{today.tm_mday}"
TEST_PATH = f"/tmp/test.{DATE_STR}"


# ...............................................
def prep_dwca_data(do_download=False, do_extract=False):
    """Download a DwCA and/or extract a DwCA.

    Args:
        do_download: True indicates to download the DwCA zipfile.
        do_extract: True indicates to unzip the DwCA zipfile.

    Raises:
        Exception: on failure to download file from URL.

    Returns:
        a span.tools.util.dwca.DwCArchive object
    """
    url = TST_VALUES.SPECIFY_URLS[0]
    zip_dwca_fullfname = assemble_download_filename(url, TEST_PATH)
    # Download DWCA file or clear all test data
    if do_download:
        if not os.path.exists(zip_dwca_fullfname):
            # download file
            dfname = download_dwca(url, TEST_PATH, overwrite=True)
            if not os.path.exists(dfname):
                raise Exception(f"Failed to download {url}")
    elif os.path.exists(zip_dwca_fullfname):
        _clear_data(TEST_PATH)

    # extract to location we are downloading to
    archive = DwCArchive(zip_dwca_fullfname)

    # Extract DWCA file or clear dwca data
    if do_extract:
        if not os.path.exists(archive.meta_fname):
            archive.extract_from_zip()
#     elif os.path.exists(archive.meta_fname):
#         extract_path, _  = os.path.split(zip_dwca_fullfname)
#         _clear_data(extract_path)
    return archive


# ............................
def test_find_links():
    """Retrieve DwCA URLs and test against expected values."""
    datasets = get_dwca_urls(TST_VALUES.SPECIFY_RSS, isIPT=False)
    for ds in datasets.values():
        if ds["url"] in TST_VALUES.SPECIFY_URLS:
            print("ok")
        else:
            print("URL {} not in test urls".format(ds["url"]))
        # assert(ds["url"] in TST_VALUES.SPECIFY_URLS)


# ............................
def test_download_link():
    """Download a DwCA."""
    #         test_path, extract_dir, zip_dwca_fname = prep_dwca_data()
    archive = prep_dwca_data(do_download=True, do_extract=False)
    assert(os.path.exists(archive.zipfile))


# ............................
def test_extract_dwca():
    """Download a DwCA and extract it."""
    archive = prep_dwca_data(do_download=True, do_extract=True)

    assert(os.path.exists(archive.meta_fname))
    assert(os.path.exists(archive.ds_meta_fname))


# ............................
def test_read_dwca_meta():
    """Download a DwCA and/or extract a DwCA, then read the dataset UUID."""
    archive = prep_dwca_data(do_download=True, do_extract=True)

    # Read dataset metadata
    ds_uuid = archive.read_dataset_uuid()
    assert(is_valid_uuid(ds_uuid))

    # Read DWCA metadata
    fileinfo = archive.read_core_fileinfo()
    for key in (
            DWCA.DELIMITER_KEY, DWCA.LINE_DELIMITER_KEY, DWCA.QUOTE_CHAR_KEY,
            DWCA.LOCATION_KEY, DWCA.UUID_KEY, DWCA.FLDMAP_KEY, DWCA.FLDS_KEY
    ):
        # Key exists and is not empty
        assert(key in fileinfo.keys())
        assert(fileinfo[key])


# ...............................................
def _clear_data(path_to_delete):
    # Delete a file or recursively delete a directory.
    if os.path.isdir(path_to_delete):
        try:
            shutil.rmtree(path_to_delete)
        except Exception:
            print(f"Failed to remove directory {path_to_delete}")

    elif os.path.isfile(path_to_delete):
        try:
            os.remove(path_to_delete)
        except Exception:
            print(f"Failed to remove file {path_to_delete}")

    else:
        print(f"Path {path_to_delete} does not exist to delete")
    # Read record metadata
    """
    fileinfo[DWCA.LOCATION_KEY] = core_loc_elt.text
    fileinfo[DWCA.DELIMITER_KEY] = core_elt.attrib[DWCA.DELIMITER_KEY]
    fileinfo[DWCA.LINE_DELIMITER_KEY] = core_elt.attrib[DWCA.LINE_DELIMITER_KEY]
    fileinfo[DWCA.QUOTE_CHAR_KEY] = core_elt.attrib[DWCA.QUOTE_CHAR_KEY]
    fileinfo["fieldname_index_map"] = field_idxs
    fileinfo["fieldnames"] = ordered_fldnames
    """


# .............................................................................
if __name__ == "__main__":
    test_find_links()
    test_download_link()
    test_extract_dwca()
    test_read_dwca_meta()
