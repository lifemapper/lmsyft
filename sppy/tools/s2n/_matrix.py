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
from sppy.tools.s2n.sparse_matrix import SparseMatrix
from sppy.tools.util.logtools import logit
from sppy.tools.util.utils import convert_np_vals_for_json, upload_to_s3

# .............................................................................
class _Matrix:
    """Class for managing computations for counts of aggregator0 x aggregator1."""

    # ...........................
    def __init__(self, table_type, data_datestr, logger=None):
        """Constructor for SpecifyNetwork matrix type.

        Args:
            table_type (aws_constants.SUMMARY_TABLE_TYPES): type of aggregated data
            data_datestr (str): date of the source data in YYYY_MM_DD format.
            logger (object): An optional local logger to use for logging output
                with consistent options
        """
        self._table_type = table_type
        self._data_datestr = data_datestr
        self._keys = SNKeys.get_keys_for_table(self._table_type)
        self._logger = logger

    # .............................................................................

