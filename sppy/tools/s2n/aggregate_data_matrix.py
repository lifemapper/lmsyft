"""Matrix to summarize 2 dimensions of data by counts of a third in a sparse matrix."""
import json
from logging import ERROR, INFO
import numpy as np
import os
import pandas as pd
from pandas.api.types import CategoricalDtype
import random
import scipy.sparse
from zipfile import ZipFile

from sppy.tools.s2n.constants import (SNKeys, Summaries)
from sppy.tools.util.logtools import logit
from sppy.tools.util.utils import convert_np_vals_for_json, upload_to_s3

# .............................................................................
class _AggregateDataMatrix:
    """Class for managing computations for counts of aggregator0 x aggregator1."""

    # ...........................
    def __init__(self, table_type, data_datestr, logger=None):
        """Constructor for species by dataset comparisons.

        Args:
            table_type (aws_constants.SUMMARY_TABLE_TYPES): type of aggregated data
            data_datestr (str): date of the source data in YYYY_MM_DD format.
            logger (object): An optional local logger to use for logging output
                with consistent options

        Note: in the first implementation, because species are generally far more
            numerous, rows are always species, columns are datasets.  This allows
            easier exporting to other formats (i.e. Excel), which allows more rows than
            columns.
        """
        self._table_type = table_type
        self._data_datestr = data_datestr
        self._table = Summaries.get_table(table_type, datestr=data_datestr)
        self._keys = SNKeys.get_keys_for_table(table_type)
        self._logger = logger
        self._report = {}

    # ...........................
    @property
    def table_type(self):
        return self._table_type

    # ...........................
    @property
    def data_datestr(self):
        return self._data_datestr

    # ...............................................
    def _get_code_from_category(self, label, axis=0):
        if axis == 0:
            categ = self._row_categ
        elif axis == 1:
            categ = self._col_categ
        else:
            raise Exception(f"2D sparse array does not have axis {axis}")

        # returns a tuple of a single 1-dimensional array of locations
        arr = np.where(categ.categories == label)[0]
        try:
            # labels are unique in categories so there will be 0 or 1 value in the array
            code = arr[0]
        except IndexError:
            raise Exception(f"Category {label} does not exist in axis {axis}")
        return code

    # ...............................................
    def _get_category_from_code(self, code, axis=0):
        if axis == 0:
            categ = self._row_categ
        elif axis == 1:
            categ = self._col_categ
        else:
            raise Exception(f"2D sparse array does not have axis {axis}")
        category = categ.categories[code]
        return category

    # ...............................................
    def _export_categories(self, axis=0):
        if axis == 0:
            categ = self._row_categ
        elif axis == 1:
            categ = self._col_categ
        else:
            raise Exception(f"2D sparse array does not have axis {axis}")
        cat_lst = categ.categories.tolist()
        return cat_lst

    # ...............................................
    def _get_categories_from_code(self, code_list, axis=0):
        if axis == 0:
            categ = self._row_categ
        elif axis == 1:
            categ = self._col_categ
        else:
            raise Exception(f"2D sparse array does not have axis {axis}")
        category_labels = []
        for code in code_list:
            category_labels.append(categ.categories[code])
        return category_labels

    # ...............................................
    def _logme(self, msg, refname="", log_level=None):
        logit(self._logger, msg, refname=refname, log_level=log_level)


    # ...............................................
    def _get_input_files(self, local_path="/tmp", do_delete=True):
        """Return the files that comprise local input data, optionally delete.

        Args:
            local_path (str): Absolute path of local destination path
            do_delete (bool): Flag indicating whether to delete local files.

        Returns:
            mtx_fname (str): absolute path for local matrix data file.
            meta_fname (str): absolute path for local metadata file.
            zip_fname (str): absolute path for local compressed file.
        """
        basename = self._table["fname"]
        mtx_ext = self._table["matrix_extension"]
        mtx_fname = f"{local_path}/{basename}{mtx_ext}"
        meta_fname = f"{local_path}/{basename}.json"
        zip_fname = f"{local_path}/{basename}.zip"
        # Delete any local temp files
        if do_delete is True:
            for fname in [mtx_fname, meta_fname, zip_fname]:
                if os.path.exists(fname):
                    self._logme(f"Removing {fname}", log_level=INFO)
                    os.remove(fname)
        return mtx_fname, meta_fname, zip_fname

    # ...............................................
    @classmethod
    def _dump_metadata(self, metadata, meta_fname):
        """Write metadata to a json file.

        Args:
            metadata (dict): metadata about matrix
            meta_fname (str): local output filename for JSON metadata.

        Raises:
            Exception: on failure to serialize metadata as JSON.
            Exception: on failure to write metadata json string to file.
        """
        try:
            metastr = json.dumps(metadata)
        except Exception as e:
            msg = f"Failed to serialize metadata as JSON: {e}"
            self._logme(msg, log_level=ERROR)
            raise Exception(msg)
        try:
            with open(meta_fname, 'w') as outf:
                outf.write(metastr)
        except Exception as e:
            msg = f"Failed to write metadata to {meta_fname}: {e}"
            self._logme(msg, log_level=ERROR)
            raise Exception(msg)

    # ...............................................
    @classmethod
    def load_metadata(cls, meta_filename):
        """Read JSON metadata for a matrix.

        Args:
            meta_filename (str): Filename of metadata to read .

        Returns:
            meta_dict (dict): metadata for a matrix

        Raises:
            Exception: on failure to read file.
            Exception: on failure load JSON metadata into a dictionary
        """
        # Read JSON dictionary as string
        try:
            with open(meta_filename) as metaf:
                meta_str = metaf.read()
        except Exception as e:
            raise Exception(f"Failed to load {meta_filename}: {e}")
        # Load metadata from string
        try:
            meta_dict = json.loads(meta_str)
        except Exception as e:
            raise Exception(f"Failed to load {meta_filename}: {e}")

        return meta_dict

    # ...............................................
    def _compress_files(self, input_fnames, zip_fname):
        """Compress this SparseMatrix to a zipped npz and json file.

        Args:
            input_fnames (str): Absolute path of local data files
            zip_fname (str): Absolute path of destination zipfile.

        Raises:
            Exception: on failure to write sparse matrix and category files to zipfile.
        """
        try:
            with ZipFile(zip_fname, 'w') as zip:
                for fname in input_fnames:
                    zip.write(fname, os.path.basename(fname))
        except Exception as e:
            msg = f"Failed to write {zip_fname}: {e}"
            self._logme(msg, log_level=ERROR)
            raise Exception(msg)

    # .............................................................................
    @classmethod
    def _uncompress_files(
            cls, zip_filename, local_path, overwrite=False):
        """Uncompress a zipped SparseMatrix into a coo_array and row/column categories.

        Args:
            zip_filename (str): Filename of output data to write to S3.
            local_path (str): Absolute path of local destination path
            overwrite (bool): Flag indicating whether to use existing files unzipped
                from the zip_filename.

        Returns:
            sparse_coo (scipy.sparse.coo_array): Sparse Matrix containing data.
            row_categ (pandas.api.types.CategoricalDtype): row categories
            col_categ (pandas.api.types.CategoricalDtype): column categories
            table_type (aws.aws_constants.SUMMARY_TABLE_TYPES): type of table data
            data_datestr (str): date string in format YYYY_MM_DD

        Raises:
            Exception: on missing input zipfile
            Exception: on failure to parse filename
            Exception: on missing expected file from zipfile

        Note:
            All filenames have the same basename with extensions indicating which data
                they contain. The filename contains a string like YYYY-MM-DD which
                indicates which GBIF data dump the statistics were built upon.
        """
        if not os.path.exists(zip_filename):
            raise Exception(f"Missing file {zip_filename}")
        basename = os.path.basename(zip_filename)
        fname, _ext = os.path.splitext(basename)
        try:
            table_type, data_datestr = Summaries.get_tabletype_datestring_from_filename(
                zip_filename)
        except Exception:
            raise

        table = Summaries.get_table(table_type)
        mtx_ext = table["matrix_extension"]
        # Expected files from archive
        mtx_fname = f"{local_path}/{fname}{mtx_ext}"
        meta_fname = f"{local_path}/{fname}.json"

        # Delete local data files if overwrite
        for fname in [mtx_fname, meta_fname]:
            if os.path.exists(fname) and overwrite is True:
                os.remove(fname)

        # Unzip to local dir
        with ZipFile(zip_filename, mode="r") as archive:
            archive.extractall(f"{local_path}/")
        for fn in [mtx_fname, meta_fname]:
            if not os.path.exists(fn):
                raise Exception(f"Missing expected file {fn}")

        return mtx_fname, meta_fname, table_type, data_datestr

