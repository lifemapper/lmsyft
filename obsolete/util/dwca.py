"""Tools for downloading, saving, reading a Darwin Core Archive file."""
from logging import ERROR
import os
import requests
import xml.etree.ElementTree as ET
import zipfile

from flask_app.broker.constants import DWCA, TST_VALUES

from sppy.tools.fileop.ready_file import ready_filename
from sppy.tools.provider.api import APIQuery
from sppy.tools.util.utils import is_valid_uuid
from sppy.tools.util.logtools import Logger, logit

INCR_KEY = 0
#
# # Pull dataset/record guids from specify RSS
# rurl = (
#     f"{TEST_SPECIFY7_SERVER}/{SPECIFY7_RECORD_ENDPOINT}/"
#     f"{TST_VALUES.DS_GUIDS_W_SPECIFY_ACCESS_RECS[0]}/"
#     f"{TST_VALUES.GUIDS_W_SPECIFY_ACCESS[0]}"
# )

# Read RSS feed for download link
# Download and unzip DWCA
# Read eml.xml for dataset UUID
# Read occurrence.csv for specimen UUIDs
# Write fields for solr to CSV


# .............................................................................
def get_dwca_urls(rss_url, isIPT=False):
    """Get download URLs from an RSS publishing them.

    Args:
        rss_url: RSS feed containing URLs for download DWCA files
        isIPT: boolean flag, True indicates this is an IPT instance

    Returns:
        dictionary of datasets with their name and URL
    """
    if isIPT:
        ds_ident_key = "title"
        link_key = "{http://ipt.gbif.org/}dwca"
    else:
        ds_ident_key = "id"
        link_key = "link"
    datasets = {}
    api = APIQuery(rss_url)
    api.query_by_get(output_type="xml", verify=False)
    # API should return XML as ElementTree element
    root = api.output
    elt = root.find("channel")
    ds_elts = elt.findall("item")
    INCR_KEY = 0
    for delt in ds_elts:
        ds_id_elt = delt.find(ds_ident_key)
        url_elt = delt.find(link_key)
        if url_elt is not None:
            if ds_id_elt is None:
                INCR_KEY += 1
                ds_key_val = str(INCR_KEY)
            else:
                ds_key_val = ds_id_elt.text
            datasets[ds_key_val] = {"url": url_elt.text, "name": ds_key_val}
    return datasets


# ......................................................
def assemble_download_filename(url, baseoutpath):
    """Construct the full filename to download a DWCA into.

    Args:
        url: location of the DWCA file to be downloaded.
        baseoutpath: destination directory for subdirectory and DWCA file

    Returns:
        full destination filename for the file to be downloaded

    Note:
        * Destination filename is based on the remote filename to be downloaded or the
            IPT endpoint containing the DWCA.
        * DWCA files must be downloaded into different directories, as the
            contents unzip into the top level directory, and standard files for
            different datasets would be overwritten.
    """
    if url.endswith(".zip"):
        _, fname = os.path.split(url)
        basename, _ = os.path.splitext(fname)
        outpath = os.path.join(baseoutpath, basename)
        outfilename = os.path.join(outpath, fname)
    else:
        # IPT link does not contain filename
        idx = url.find("r=")
        tmp = url[idx+2:]
        parts = tmp.split("&")
        if len(parts) == 1:
            name = tmp
        else:
            name = ".".join(parts)
        outfilename = os.path.join(baseoutpath, name, "{}.zip".format(name))
    return outfilename


# ......................................................
def download_dwca(url, baseoutpath, overwrite=False, logger=None):
    """Download a DarwinCore Archive file from a URL.

    Args:
        url: location of DWCA data file
        baseoutpath: destination directory for DWCA file
        overwrite: True if an existing file should be replaced
        logger: optional logger for saving output messages to file.

    Returns:
        The destination filename for the downloaded DWCA file.
    """
    outfilename = assemble_download_filename(url, baseoutpath)
    success = ready_filename(outfilename, overwrite=overwrite)
    if not success:
        logit(
            logger, f"File {outfilename} is not ready for writing",
            refname="download_dwca", log_level=ERROR)
    else:
        ret_code = None
        try:
            response = requests.get(url)
        except Exception as e:
            logit(
                logger, f"Failed to get a response from {url}, {e}",
                refname="download_dwca", log_level=ERROR)
        else:
            try:
                ret_code = response.status_code
            except AttributeError:
                logit(
                    logger, f"Failed on URL {url}, no status_code",
                    refname="download_dwca", log_level=ERROR)
    if ret_code != 200:
        logit(
            logger, f"Failed on URL {url}, code {ret_code}",
            refname="download_dwca", log_level=ERROR)
        return None
    else:
        output = response.content
        with open(outfilename, "wb") as outf:
            outf.write(output)
    return outfilename


# .............................................................................
class DwCArchive:
    """Class to download and read a Darwin Core Archive."""

    # ......................................................
    def __init__(self, zipfile_or_directory, outpath=None, logger=None):
        """Constructor.

        Args:
            zipfile_or_directory: Full path to zipfile or directory containing
                Darwin Core Archive
            outpath: file location for output data and log files
            logger: LMLog object for logging processing information

        Raises:
            Exception: on non-existent zipfile_or_directory.

        Note:
            Produces data requiring http post to contain
            headers={"Content-Type": "text/csv"}
        """
        if os.path.exists(zipfile_or_directory):
            self.logger = logger
            # DWCA is zipped
            if (
                    os.path.isfile(zipfile_or_directory) and
                    zipfile_or_directory.endswith(".zip")
            ):
                self.zipfile = zipfile_or_directory
                if outpath is not None:
                    self.dwca_path = outpath
                else:
                    self.dwca_path, _ = os.path.split(zipfile_or_directory)

            # DWCA is ready
            elif os.path.isdir(zipfile_or_directory):
                self.dwca_path = zipfile_or_directory
            # Metadata files
            self.meta_fname = os.path.join(self.dwca_path, DWCA.META_FNAME)
            self.ds_meta_fname = os.path.join(self.dwca_path, DWCA.DATASET_META_FNAME)

        else:
            raise Exception(f"File or directory {zipfile_or_directory} does not exist")

    # ......................................................
    def _get_date(self, dwc_rec):
        coll_date = ""
        try:
            yr = dwc_rec["year"]
            int(yr)
        except Exception:
            pass
        else:
            coll_date = yr
            try:
                mo = dwc_rec["month"]
                int(mo)
            except Exception:
                pass
            else:
                coll_date = "{}-{}".format(coll_date, mo)
                try:
                    dy = dwc_rec["day"]
                    int(dy)
                except Exception:
                    pass
                else:
                    coll_date = "{}-{}".format(coll_date, dy)
        return coll_date

    # ......................................................
    def extract_from_zip(self, extract_path=None):
        """Extract a zipfile.

        Args:
            extract_path: destination directory for writing the zipfile contents.
        """
        zfile = zipfile.ZipFile(self.zipfile, mode="r", allowZip64=True)
        if extract_path is None:
            extract_path, _ = os.path.split(self.zipfile)
        # unzip zip file stream
        for zinfo in zfile.infolist():
            _, ext = os.path.splitext(zinfo.filename)
            # Check file extension and only unzip valid files
            if ext in [".xml", ".csv", ".txt"]:
                zfile.extract(zinfo, path=extract_path)
            else:
                logit(
                    self.logger,
                    f"Unexpected filename {zinfo.filename} in zipfile {self.zipfile}",
                    refname=self.__class__.__name__, log_level=ERROR)

    # ......................................................
    def read_dataset_uuid(self):
        """Read the GBIF datasetKey from the metadata in a DwC Archive.

        Returns:
            idstr: the UUID for the current dataset.
        """
        idstr = None
        if os.path.split(self.ds_meta_fname)[1] != DWCA.DATASET_META_FNAME:
            logit(
                self.logger,
                f"Expected filename {DWCA.DATASET_META_FNAME} at {self.ds_meta_fname}",
                refname=self.__class__.__name__, log_level=ERROR)
            return ""
        tree = ET.parse(self.ds_meta_fname)
        root = tree.getroot()
        elt = root.find("dataset")
        id_elts = elt.findall("alternateIdentifier")
        for ie in id_elts:
            idstr = ie.text
            if is_valid_uuid(idstr):
                break
        return idstr

    # ......................................................
    def _fix_char(self, ch):
        if not ch:
            ch = None
        elif ch == "\\t":
            ch = "\t"
        elif ch == "\\n":
            ch = "\n"
        elif ch == "":
            ch = None
        return ch

    # ......................................................
    def read_core_fileinfo(self):
        """Read meta.xml file for information about the core occurrence file.

        Returns:
            Dictionary of core occurrence file information, with keys matching the
                names/tags in the meta.xml file:
                    location (for filename), id (for fieldname of record UUID)
                    fieldsTerminatedBy, linesTerminatedBy, fieldsEnclosedBy,
                plus:
                    fieldnames: ordered fieldnames
                    fieldname_index_map: dict of fields and corresponding column indices
        """
        if os.path.split(self.meta_fname)[1] != DWCA.META_FNAME:
            logit(
                self.logger,
                f"Expected filename {DWCA.META_FNAME} at {self.meta_fname}",
                refname=self.__class__.__name__
            )
            return ""

        fileinfo = {}
        field_idxs = {}
        tree = ET.parse(self.meta_fname)
        root = tree.getroot()
        core_elt = root.find("{}core".format(DWCA.NS))
        if core_elt.attrib["rowType"] == DWCA.CORE_TYPE:
            # CSV file name
            core_files_elt = core_elt.find("{}files".format(DWCA.NS))
            core_loc_elt = core_files_elt.find("{}{}".format(DWCA.NS, DWCA.LOCATION_KEY))
            fileinfo[DWCA.LOCATION_KEY] = core_loc_elt.text
            # CSV file structure
            fileinfo[DWCA.DELIMITER_KEY] = self._fix_char(
                core_elt.attrib[DWCA.DELIMITER_KEY])
            fileinfo[DWCA.LINE_DELIMITER_KEY] = self._fix_char(
                core_elt.attrib[DWCA.LINE_DELIMITER_KEY])
            quote_char = self._fix_char(
                core_elt.attrib[DWCA.QUOTE_CHAR_KEY])
            fileinfo[DWCA.QUOTE_CHAR_KEY] = quote_char
            # CSV file fields/indices
            # Dictionary of field --> index, index --> field
            # UUID key and index
            uuid_idx = core_elt.find("{}{}".format(
                DWCA.NS, DWCA.UUID_KEY)).attrib["index"]
            # The uuid_idx index --> fieldname
            #     plus fieldname --> uuid_idx
            field_idxs[DWCA.UUID_KEY] = uuid_idx
            field_idxs[uuid_idx] = DWCA.UUID_KEY
            all_idxs = {int(uuid_idx)}
            # Rest of fields and indices
            field_elts = core_elt.findall("{}field".format(DWCA.NS))
            startidx = len(DWCA.NS)-1
            # Default UUID fieldname
            uuid_fldname = DWCA.UUID_KEY
            for celt in field_elts:
                tmp = celt.attrib["term"]
                # strip namespace url from term
                startidx = tmp.rfind("/") + 1
                term = tmp[startidx:]
                idx = celt.attrib["index"]
                # Correct UUID fieldname
                if idx == uuid_idx:
                    uuid_fldname = term
                    field_idxs.pop(DWCA.UUID_KEY)
                all_idxs.add(int(idx))
                field_idxs[idx] = term
                field_idxs[term] = idx
            fileinfo[DWCA.UUID_KEY] = uuid_fldname
            fileinfo[DWCA.FLDMAP_KEY] = field_idxs
            # CSV file fieldnames ordered by column index
            all_idxs = list(all_idxs)
            all_idxs.sort()
            ordered_fldnames = []
            for i in all_idxs:
                ordered_fldnames.append(field_idxs[str(i)])
            fileinfo[DWCA.FLDS_KEY] = ordered_fldnames

        return fileinfo


# # ...............................................
# def index_specify7_dataset(
#         zname, dwca_url, outpath, solr_location, collection, testguids=[]):
#     """
#     Args:
#         zname: zipfilename
#         dwca_url: URL for DWCA download
#         outpath: output path for saved data.
#         solr_location: IP or FQDN for solr index
#         collection: Solr collection for indexing records
#         testguids: optional guids for testing
#     """
#     name = os.path.splitext(os.path.basename(__file__))[0]
#     log_name = f"{name}.{int(time.time())}"
#     logger = Logger(log_name, log_path=outpath, log_console=True)
#
#     # IPT url does not host Specify occurrence server
#     isIPT = (dwca_url.find("http://ipt") == 0)
#     specify_url = "unknown_url"
#     if dwca_url is not None and not isIPT:
#         # Assumes the base RSS/DWCA url is the Specify server
#         specify_url = TEST_SPECIFY7_SERVER
#
#     # Existing Zipfile
#     if zname is not None:
#         datasets = {"unknown_guid": {"filename": zname}}
#     # Download Zipfiles and save info on each
#     else:
#         datasets = get_dwca_urls(dwca_url, isIPT=isIPT)
#         for guid, meta in datasets.items():
#             try:
#                 url = meta["url"]
#             except KeyError:
#                 logger.log(
#                     f"Failed to get URL for IPT dataset {guid}", refname=name,
#                     log_level=ERROR)
#             else:
#                 zipfname = download_dwca(url, outpath, overwrite=False)
#                 meta["filename"] = zipfname
#                 datasets[guid] = meta
#     fixme = []
#     # Process Zipfiles
#     for tmp_guid, meta in datasets.items():
#         try:
#             zipfname = meta["filename"]
#         except KeyError:
#             logger.log(
#                 "Failed to download data for IPT dataset {guid}", refname=name,
#                 log_level=WARN)
#         else:
#             dwca = DwCArchive(zipfname, logger=logger)
#
#             extract_path, _ = os.path.split(zipfname)
#             meta_fname = os.path.join(extract_path, DWCA.META_FNAME)
#             ds_meta_fname = os.path.join(extract_path, DWCA.DATASET_META_FNAME)
#             # Extract if needed
#             if not os.path.exists(meta_fname):
#                 dwca.extract_from_zip(zipfname, extract_path=extract_path)
#
#         # Read DWCA and dataset metadata
#         core_fileinfo = dwca.read_core_fileinfo(meta_fname)
#         core_fileinfo[SPECIFY7_SERVER_KEY] = specify_url
#         dwca_guid = dwca.read_dataset_uuid(ds_meta_fname)
#         # Save new guid for update of datasets dict
#         # if zname argument is provided, we have dataset without guid from download site
#         if is_valid_uuid(tmp_guid) and dwca_guid != tmp_guid:
#             logger.log(
#                 f"DWCA meta.xml guid {dwca_guid} conflicts with reported {tmp_guid}",
#                 refname=name, log_level=WARN)
#             # new/obsolete guid pair
#             fixme.append((dwca_guid, tmp_guid))
#
#         # Read record metadata, dwca_guid takes precedence
#         solr_fname, content_type, is_new = dwca.rewrite_recs_for_solr(
#             core_fileinfo, dwca_guid, extract_path, overwrite=False)
#
#         # Post
#         if is_new:
#             start_count = SpSolr.count_docs(collection, solr_location=solr_location)
#             retcode, output = SpSolr.post(
#                 collection, solr_fname, solr_location=solr_location,
#                 headers={"Content-Type": content_type})
#
#             # Report old/new solr index count
#             end_count = SpSolr.count_docs(collection, solr_location=solr_location)
#             logger.log(
#                 f"Posted, code {retcode}, to {collection}, {start_count} --> "
#                 f"{end_count} docs", refname=name)
#
#     # May use dataset guid somewhere
#     for new_obsolete_pair in fixme:
#         # Remove invalid key
#         meta = datasets.pop(new_obsolete_pair[1])
#         # Add value back with updated key
#         datasets[new_obsolete_pair[0]] = meta
#
#
# .............................................................................
if __name__ == "__main__":
    import argparse

    test_rss = TST_VALUES.KU_IPT_RSS_URL
#     test_rss = TST_VALUES.ICH_RSS_URL

    parser = argparse.ArgumentParser(
        description=("Download, extract, and read a zipped DWCA file."))
    parser.add_argument(
        "--dwca_file", type=str, default=None,
        help="Zipped DWCA to process")
    parser.add_argument(
        "--rss", type=str, default=test_rss,
        help="URL for RSS feed with download links")
    parser.add_argument(
        "--outpath", type=str, default="/tmp",
        help="Optional path for DWCA extraction and logger")
    args = parser.parse_args()

    script_name = os.path.splitext(os.path.basename(__file__))[0]
    zipfilename = args.dwca_file
    dwca_url = args.rss
    outpath = args.outpath
    logger = Logger(script_name, log_path=args.outpath, log_console=True)

    if zipfilename is None:
        zipfilename = download_dwca(
            dwca_url, args.outpath, overwrite=True, logger=logger)

    dwca = DwCArchive(zipfilename, outpath=None, logger=logger)
    dwca.extract_from_zip(extract_path=outpath)
    ds_uuid = dwca.read_dataset_uuid()
    fileinfo = dwca.read_core_fileinfo()

    # occguids = [
    #     "2c1becd5-e641-4e83-b3f5-76a55206539a",
    #     "a413b456-0bff-47da-ab26-f074d9be5219",
    #     "fa7dd78f-8c91-49f5-b01c-f61b3d30caee",
    #     "db1af4fe-1ed3-11e3-bfac-90b11c41863e",
    #     "dbe1622c-1ed3-11e3-bfac-90b11c41863e",
    #     "dcbdb494-1ed3-11e3-bfac-90b11c41863e",
    #     "dc92869c-1ed3-11e3-bfac-90b11c41863e",
    #     "21ac6644-5c55-44fd-b258-67eb66ea231d"]
