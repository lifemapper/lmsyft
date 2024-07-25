"""Parent Class for the Specify Network API services."""
from logging import INFO
import os
from werkzeug.exceptions import BadRequest

from flask_app.common.base import _SpecifyNetworkService
from flask_app.common.s2n_type import AnalystOutput, APIService

from sppy.aws.aws_constants import PROJ_BUCKET, SUMMARY_FOLDER
from sppy.aws.aws_tools import (
    download_from_s3, get_current_datadate_str, get_today_str)

from sppy.tools.s2n.constants import (Summaries, SUMMARY_TABLE_TYPES)
from sppy.tools.s2n.sparse_matrix import SparseMatrix
from sppy.tools.s2n.summary_matrix import SummaryMatrix
from sppy.tools.util.utils import add_errinfo, get_traceback
from sppy.tools.util.logtools import Logger


try:
    # For docker deployment
    WORKING_PATH = os.environ["WORKING_DIRECTORY"]
    # Read-only volume
    INPUT_DATA_PATH = os.environ["AWS_DATA_DIRECTORY"]
except KeyError:
    # For local debugging
    WORKING_PATH = '/tmp'
    INPUT_DATA_PATH = WORKING_PATH
LOG_PATH = os.path.join(WORKING_PATH, "log")


# .............................................................................
class _AnalystService(_SpecifyNetworkService):
    """Base S-to-the-N service, handles parameter names and acceptable values."""
    # overridden by subclasses
    SERVICE_TYPE = APIService.AnalystRoot

    # ...............................................
    @classmethod
    def get_endpoint(cls, **kwargs):
        """Return the http response for this class endpoint.

        Args:
            **kwargs: keyword arguments are accepted but ignored

        Returns:
            flask_app.analyst.s2n_type.S2nOutput object

        Raises:
            Exception: on unknown error.
        """
        try:
            output = cls._show_online()
        except Exception:
            raise
        return output.response

    # ...............................................
    @classmethod
    def _show_online(cls):
        svc = cls.SERVICE_TYPE["name"]
        info = {
            "info": "Specify Network {} service is online.".format(svc)}

        param_lst = []
        for p, pdict in cls.SERVICE_TYPE["params"].items():
            pinfo = pdict.copy()
            pinfo["type"] = str(type(pinfo["type"]))
            param_lst.append({p: pinfo})
        info["parameters"] = param_lst

        output = AnalystOutput(
            svc, description=cls.SERVICE_TYPE["description"], errors=info)
        return output

    # ...............................................
    @classmethod
    def _init_logger(cls):
        script_name = os.path.splitext(os.path.basename(__file__))[0]
        todaystr = get_today_str()
        log_name = f"{script_name}_{todaystr}"
        logger = Logger(
            log_name, log_path=LOG_PATH, log_console=True, log_level=INFO)
        return logger

    # ...............................................
    @classmethod
    def _standardize_params(
            cls, summary_type=None, summary_key=None, rank_by=None, order=None,
            limit=10):
        """Standardize query parameters to send to appropriate service.

        Args:
            summary_type: data dimension for summary, comparisons, rank
            summary_key: unique identifier for the data dimension being examined.
            rank_by: rank by occurrence counts or measurement of another dimension of
                the data
            order: sort records "descending" or "ascending"
            limit: integer indicating how many ranked records to return, value must
                be less than QUERY_LIMIT.

        Raises:
            BadRequest: on invalid query parameters.
            BadRequest: on summary_type == rank_by for Rank service.
            BadRequest: on unknown exception parsing parameters.

        Returns:
            a dictionary containing keys and properly formatted values for the
                user specified parameters.
        """
        user_kwargs = {
            "summary_type": summary_type,
            "summary_key": summary_key,
            "rank_by": rank_by,
            "order": order,
            "limit": limit
        }
        try:
            usr_params, errinfo = cls._process_params(user_kwargs)
        except Exception:
            error_description = get_traceback()
            raise BadRequest(error_description)

        # In RankSvc, summary_type and rank_by may not be the same data dimension
        if rank_by is not None and usr_params["summary_type"] == usr_params["rank_by"]:
            raise BadRequest(
                f"Cannot rank by the same dimension as summarizing by. "
                f"URL arguments summary_type ({usr_params['summary_type']}) "
                f"and rank_by ({usr_params['rank_by']}) may not be equal.")

        # errinfo["error"] indicates bad parameters, throws exception
        try:
            error_description = "; ".join(errinfo["error"])
            raise BadRequest(error_description)
        except KeyError:
            pass

        return usr_params, errinfo

    # ...............................................
    @classmethod
    def _get_matrix_input_filenames(cls, table, input_path):
        basename = table["fname"]
        mtx_ext = table["matrix_extension"]
        mtx_fname = f"{input_path}/{basename}{mtx_ext}"
        meta_fname = f"{input_path}/{basename}.json"
        zip_fname = f"{input_path}/{basename}.zip"
        return mtx_fname, meta_fname, zip_fname

    # ...............................................
    @classmethod
    def _test_download(cls, filename):
        success = True
        msg = ""
        # Test successful download
        retries = 0
        interval = 6
        while not os.path.exists(filename) and retries < 10:
            import time
            time.sleep(6)
            retries += 1
        if not os.path.exists(filename):
            success = False
            msg = f"Failed to access {filename} in {retries * interval} seconds"
        return success, msg

    # ...............................................
    @classmethod
    def _retrieve_sparse_matrix(cls, zip_basename, local_path):
        sparse_coo = None
        row_categ = None
        col_categ = None
        table_type = None
        errinfo = {"info": [f"Download data {zip_basename} locally"]}
        # Download to local working directory if file does not exist
        try:
            zip_filename = download_from_s3(
                PROJ_BUCKET, SUMMARY_FOLDER, zip_basename, local_path=local_path,
                overwrite=True)
        except Exception as e:
            errinfo = add_errinfo(errinfo, "error", str(e))

        else:
            success, msg = cls._test_download(zip_filename)
            if success:
                try:
                    sparse_coo, row_categ, col_categ, table_type, _data_datestr = \
                        SparseMatrix.uncompress_zipped_data(
                            zip_filename, local_path=INPUT_DATA_PATH, overwrite=False)
                except Exception as e:
                    errinfo = add_errinfo(errinfo, "error", str(e))
            else:
                errinfo = add_errinfo(errinfo, "error", [msg])
        return sparse_coo, row_categ, col_categ, table_type, errinfo

    # ...............................................
    @classmethod
    def _init_sparse_matrix(cls):
        errinfo = {}
        data_datestr = get_current_datadate_str()
        mtx_table_type = SUMMARY_TABLE_TYPES.SPECIES_DATASET_MATRIX
        table = Summaries.get_table(mtx_table_type, data_datestr)
        # Look for uncompressed files in Read-only volume first
        mtx_filename, meta_filename, zip_filename = cls._get_matrix_input_filenames(
            table, INPUT_DATA_PATH)
        if os.path.exists(meta_filename) and os.path.exists(mtx_filename):
            # Read
            sparse_coo, row_categ, col_categ = SparseMatrix.read_data(
                mtx_filename, meta_filename)
        else:
            # or Download to working path and read
            sparse_coo, row_categ, col_categ, _table_type, errinfo = \
                cls._retrieve_sparse_matrix(
                    os.path.basename(zip_filename), WORKING_PATH)
        # Create
        sp_mtx = SparseMatrix(
            sparse_coo, mtx_table_type, data_datestr, row_categ, col_categ)
        return sp_mtx, errinfo

    # ...............................................
    @classmethod
    def _retrieve_summary_matrix(cls, zip_basename, local_path):
        dataframe = None
        meta_dict = None
        table_type = None
        errinfo = {"info": [f"Download data {zip_basename} locally"]}
        # Download to local working directory if file does not exist
        try:
            zip_filename = download_from_s3(
                PROJ_BUCKET, SUMMARY_FOLDER, zip_basename, local_path=local_path,
                overwrite=True)
        except Exception as e:
            errinfo = add_errinfo(errinfo, "error", str(e))

        else:
            success, msg = cls._test_download(zip_filename)
            if success:
                # Extract if matrix and metadata files do not exist, create objects
                try:
                    dataframe, meta_dict, table_type, _data_datestr = \
                        SummaryMatrix.uncompress_zipped_data(
                            zip_filename, local_path=WORKING_PATH, overwrite=False)
                except Exception as e:
                    errinfo = add_errinfo(errinfo, "error", str(e))
            else:
                errinfo = add_errinfo(errinfo, "error", [msg])
        return dataframe, meta_dict, table_type, errinfo

    # ...............................................
    @classmethod
    def _init_summary_matrix(cls, summary_type):
        errinfo = {}
        data_datestr = get_current_datadate_str()
        if summary_type == "dataset":
            mtx_table_type = SUMMARY_TABLE_TYPES.DATASET_SPECIES_SUMMARY
        else:
            mtx_table_type = SUMMARY_TABLE_TYPES.SPECIES_DATASET_SUMMARY

        table = Summaries.get_table(mtx_table_type, data_datestr)
        # Look for uncompressed files in Read-only volume first
        mtx_filename, meta_filename, zip_filename = cls._get_matrix_input_filenames(
            table, INPUT_DATA_PATH)
        if os.path.exists(meta_filename) and os.path.exists(mtx_filename):
            # Read
            dataframe, meta_dict = SummaryMatrix.read_data(
                mtx_filename, meta_filename)
        else:
            # or Download to working path and read
            if not (os.path.exists(meta_filename) and os.path.exists(mtx_filename)):
                dataframe, meta_dict, _, errinfo = cls._retrieve_summary_matrix(
                    os.path.basename(zip_filename), WORKING_PATH)

        # Create
        summary_mtx = SummaryMatrix(
            dataframe, mtx_table_type, data_datestr, logger=None)

        return summary_mtx, errinfo


# .............................................................................
if __name__ == "__main__":
    pass
