"""Matrix to summarize 2 dimensions of data by counts of a third in a sparse matrix."""
from logging import ERROR
import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype
import random
import scipy.sparse

from sppy.tools.s2n.aggregate_data_matrix import _AggregateDataMatrix
from sppy.tools.s2n.constants import (SNKeys, Summaries)


# .............................................................................
class SparseMatrix(_AggregateDataMatrix):
    """Class for managing computations for counts of aggregator0 x aggregator1."""

    # ...........................
    def __init__(
            self, sparse_coo_array, table_type, data_datestr, row_category,
            column_category, logger=None):
        """Constructor for species by dataset comparisons.

        Args:
            sparse_coo_array (scipy.sparse.coo_array): A 2d sparse array with count
                values for one aggregator0 (i.e. species) rows (axis 0) by another
                aggregator1 (i.e. dataset) columns (axis 1) to use for computations.
            table_type (aws_constants.SUMMARY_TABLE_TYPES): type of aggregated data
            data_datestr (str): date of the source data in YYYY_MM_DD format.
            row_category (pandas.api.types.CategoricalDtype): ordered row labels used
                to identify axis 0/rows.
            column_category (pandas.api.types.CategoricalDtype): ordered column labels
                used to identify axis 1/columns.
            logger (object): An optional local logger to use for logging output
                with consistent options

        Note: in the first implementation, because species are generally far more
            numerous, rows are always species, columns are datasets.  This allows
            easier exporting to other formats (i.e. Excel), which allows more rows than
            columns.
        """
        self._coo_array = sparse_coo_array
        self._row_categ = row_category
        self._col_categ = column_category
        _AggregateDataMatrix.__init__(self, table_type, data_datestr, logger=logger)

    # ...........................
    @classmethod
    def init_from_stacked_data(
            cls, stacked_df, x_fld, y_fld, val_fld, table_type, data_datestr,
            logger=None):
        """Create a sparse matrix of rows by columns containing values from a table.

        Args:
            stacked_df (pandas.DataFrame): DataFrame of records containing columns to be
                used as the new rows, new columns, and values.
            x_fld: column in the input dataframe containing values to be used as
                columns (axis 1)
            y_fld: column in the input dataframe containing values to be used as rows
                (axis 0)
            val_fld: : column in the input dataframe containing values to be used as
                values for the intersection of x and y fields
            table_type (aws_constants.SUMMARY_TABLE_TYPES): table type of sparse matrix
                aggregated data
            data_datestr (str): date of the source data in YYYY_MM_DD format.
            logger (object): logger for saving relevant processing messages

        Returns:
            sparse_coo (scipy.coo_array): matrix of y values (rows, y axis=0) by
                x values (columnns, x axis=1), with values from another column.

        Note:
            The input dataframe must contain only one input record for any x and y value
                combination, and each record must contain another value for the dataframe
                contents.  The function was written for a table of records with
                datasetkey (for the column labels/x), species (for the row labels/y),
                and occurrence count.
        """
        # Get unique values to use as categories for scipy column and row indexes,
        # remove None
        unique_x_vals = list(stacked_df[x_fld].dropna().unique())
        unique_y_vals = list(stacked_df[y_fld].dropna().unique())
        # Categories allow using codes as the integer index for scipy matrix
        y_categ = CategoricalDtype(unique_y_vals, ordered=True)
        x_categ = CategoricalDtype(unique_x_vals, ordered=True)
        # Create a list of category codes matching original stacked data to replace
        #   column names from stacked data dataframe with integer codes for row and
        #   column indexes in the new scipy matrix
        col_idx = stacked_df[x_fld].astype(x_categ).cat.codes
        row_idx = stacked_df[y_fld].astype(y_categ).cat.codes
        # This creates a new matrix in Coordinate list (COO) format.  COO stores a list
        # of (row, column, value) tuples.  Convert to CSR or CSC for efficient Row or
        # Column slicing, respectively
        sparse_coo = scipy.sparse.coo_array(
            (stacked_df[val_fld], (row_idx, col_idx)),
            shape=(y_categ.categories.size, x_categ.categories.size))
        sparse_matrix = SparseMatrix(
            sparse_coo, table_type, data_datestr, y_categ, x_categ, logger=logger)
        return sparse_matrix

    # ...........................
    @property
    def row_category(self):
        """Return the data structure representing the row category.

        Returns:
            self._row_categ (pandas.api.types.CategoricalDtype): ordered row labels used
                to identify axis 0/rows.
        """
        return self._row_categ

    # ...........................
    @property
    def column_category(self):
        """Return the data structure representing the column category.

        Returns:
            self._col_categ (pandas.api.types.CategoricalDtype): ordered column labels
                used to identify axis 1/columns.
        """
        return self._col_categ

    # .............................................................................
    def _to_dataframe(self):
        sdf = pd.DataFrame.sparse.from_spmatrix(
            self._coo_array,
            index=self._row_categ.categories,
            columns=self._col_categ.categories)
        return sdf

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
            raise
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

    # ...........................
    def _to_csr(self):
        # Convert to CSR format for efficient row slicing
        csr = self._coo_array.tocsr()
        return csr

    # ...........................
    def _to_csc(self):
        # Convert to CSC format for efficient column slicing
        csc = self._coo_array.tocsr()
        return csc

    # ...............................................
    def get_random_labels(self, count, axis=0):
        """Get random values from the labels on an axis of a sparse matrix.

        Args:
            count (int): number of values to return
            axis (int): row (0) or column (1) header for labels to gather.

        Returns:
            x_vals (list): random values pulled from the column

        Raises:
            Exception: on axis not in (0, 1)
        """
        if axis == 0:
            categ = self._row_categ
        elif axis == 1:
            categ = self._col_categ
        else:
            raise Exception(f"2D sparse array does not have axis {axis}")
        # Get a random sample of category indexes
        idxs = random.sample(range(1, len(categ.categories)), count)
        labels = [self._get_category_from_code(i, axis=axis) for i in idxs]
        return labels

    # ...............................................
    @property
    def num_y_values(self):
        """Get the number of rows.

        Returns:
            int: The count of rows where the value > 0 in at least one column.

        Note:
            Also used as gamma diversity (species richness over entire landscape)
        Note: because the sparse data will only from contain unique rows and columns
            with data, this should ALWAYS equal the number of rows
        """
        return self._coo_array.shape[0]

    # ...............................................
    @property
    def num_x_values(self):
        """Get the number of columns.

        Returns:
            int: The count of columns where the value > 0 in at least one row

        Note: because the sparse data will only from contain unique rows and columns
            with data, this should ALWAYS equal the number of columns
        """
        return self._coo_array.shape[1]

    # ...............................................
    def get_vector_from_label(self, label, axis=0):
        """Return the row (axis 0) or column (axis 1) with label `label`.

        Args:
            label: label for row of interest
            axis (int): row (0) or column (1) header for vector and index to gather.

        Returns:
            vector (scipy.sparse.csr_array): 1-d array of the row/column for 'label'.
            idx (int): index for the vector (zeros and non-zeros) in the sparse matrix

        Raises:
            IndexError: on label does not exist in category
            Exception: on axis not in (0, 1)
        """
        try:
            idx = self._get_code_from_category(label, axis=axis)
        except IndexError:
            raise
        if axis == 0:
            vector = self._coo_array.getrow(idx)
        elif axis == 1:
            vector = self._coo_array.getcol(idx)
        else:
            raise Exception(f"2D sparse array does not have axis {axis}")
        idx = self.convert_np_vals_for_json(idx)
        return vector, idx

    # ...............................................
    def sum_vector(self, label, axis=0):
        """Get the total of values in a single row or column.

        Args:
            label: label on the row (axis 0) or column (axis 1) to total.
            axis (int): row (0) or column (1) header for vector to sum.

        Returns:
            int: The total of all values in one column

        Raises:
            IndexError: on label not present in vector header
        """
        try:
            vector, _idx = self.get_vector_from_label(label, axis=axis)
        except IndexError:
            raise
        total = vector.sum()
        return total

    # ...............................................
    def get_row_labels_for_data_in_column(self, col, value=None):
        """Get the minimum or maximum NON-ZERO value and row label(s) for a column.

        Args:
            col: column to find row labels in.
            value: filter data value to return row labels for.  If None, return labels
                for all non-zero rows.

        Returns:
            target: The minimum or maximum value for a column
            row_labels: The labels of the rows containing the target value
        """
        # Returns row_idxs, col_idxs, vals of NNZ values in row
        row_idxs, col_idxs, vals = scipy.sparse.find(col)
        if value is None:
            idxs_lst = [row_idxs[i] for i in range(len(row_idxs))]
        else:
            tmp_idxs = np.where(vals == value)[0]
            tmp_idx_lst = [tmp_idxs[i] for i in range(len(tmp_idxs))]
            # Row indexes of maxval in column
            idxs_lst = [row_idxs[i] for i in tmp_idx_lst]
        row_labels = [self._get_category_from_code(idx, axis=0) for idx in idxs_lst]
        return row_labels

    # ...............................................
    def get_extreme_val_labels_for_vector(self, vector, axis=0, is_max=True):
        """Get the minimum or maximum NON-ZERO value and row label(s) for a column.

        Args:
            vector (numpy.array): 1 dimensional array for a row or column.
            is_max (bool): flag indicating whether to get maximum (T) or minimum (F)
            axis (int): row (0) or column (1) header for extreme value and labels.

        Returns:
            target: The minimum or maximum value for a column
            row_labels: The labels of the rows containing the target value

        Raises:
            Exception: on axis not in (0, 1)
        """
        # Returns row_idxs, col_idxs, vals of NNZ values in row
        row_idxs, col_idxs, vals = scipy.sparse.find(vector)
        if is_max is True:
            target = vals.max()
        else:
            target = vals.min()
        target = self.convert_np_vals_for_json(target)

        # Get indexes of target value within NNZ vals
        tmp_idxs = np.where(vals == target)[0]
        tmp_idx_lst = [tmp_idxs[i] for i in range(len(tmp_idxs))]
        # Get actual indexes (within all zero/non-zero elements) of target in vector
        if axis == 0:
            # Column indexes of maxval in row
            idxs_lst = [col_idxs[i] for i in tmp_idx_lst]
            # Label axis is the opposite of the vector axis
            label_axis = 1
        elif axis == 1:
            # Row indexes of maxval in column
            idxs_lst = [row_idxs[j] for j in tmp_idx_lst]
            label_axis = 0
        else:
            raise Exception(f"2D sparse array does not have axis {axis}")

        # Convert from indexes to labels
        labels = [
            self._get_category_from_code(idx, axis=label_axis) for idx in idxs_lst]
        return target, labels

    # ...............................................
    def get_row_stats(self, row_label=None):
        """Get the statistics for one or all rows.

        Args:
            row_label (str): label for one row of data to examine.

        Returns:
            stats (dict): quantitative measures of one or all rows.

        Raises:
            IndexError: on row_label not found in data.
        """
        if row_label is None:
            try:
                stats = self.get_all_row_stats()
            except IndexError:
                raise
        else:
            stats = self.get_one_row_stats(row_label)
        return stats

    # ...............................................
    def get_one_row_stats(self, row_label):
        """Get a dictionary of statistics for the row with this row_label.

        Args:
            row_label: label on the row to gather stats for.

        Returns:
            stats (dict): quantitative measures of the row.

        Raises:
            IndexError: on row_label not found in data.

        Note:
            Inline comments are specific to a SUMMARY_TABLE_TYPES.SPECIES_DATASET_MATRIX
                with row/column/value = species/dataset/occ_count
        """
        # Get row (sparse array), and its index
        try:
            row, row_idx = self.get_vector_from_label(row_label, axis=0)
        except IndexError:
            raise
        # Largest Occurrence count for this Species, and datasets that contain it
        maxval, max_row_labels = self.get_extreme_val_labels_for_vector(
            row, axis=0, is_max=True)
        minval, min_row_labels = self.get_extreme_val_labels_for_vector(
            row, axis=0, is_max=False)
        stats = {
            self._keys[SNKeys.ROW_IDX]: row_idx,
            self._keys[SNKeys.ROW_LABEL]: row_label,
            # Total Occurrences for this Species
            self._keys[SNKeys.ROW_TOTAL]: self.convert_np_vals_for_json(row.sum()),
            # Count of Datasets containing this Species
            self._keys[SNKeys.ROW_COUNT]: self.convert_np_vals_for_json(row.nnz),
            # Return min/max count in this species and datasets for that count
            self._keys[SNKeys.ROW_MIN_COUNT]: minval,
            # TODO: is there a good way to optionally return many labels
            self._keys[SNKeys.ROW_MAX_COUNT]: maxval,
            self._keys[SNKeys.ROW_MAX_LABELS]: max_row_labels,
        }
        return stats

    # ...............................................
    def get_all_row_stats(self):
        """Return stats (min, max, mean, median) of totals and counts for all rows.

        Returns:
            all_row_stats (dict): counts and statistics about all rows.
            (numpy.ndarray): array of totals of all rows.
        """
        # Sum all rows to return a column (axis=1)
        all_totals = self._coo_array.sum(axis=1)
        # Get number of non-zero entries for every row (column, numpy.ndarray)
        all_counts = self._coo_array.getnnz(axis=1)
        # Count columns with at least one non-zero entry (all columns)
        row_count = self._coo_array.shape[1]
        all_row_stats = {
            self._keys[SNKeys.ROWS_COUNT]: row_count,
            self._keys[SNKeys.ROWS_TOTAL]:
                self.convert_np_vals_for_json(all_totals.sum()),
            self._keys[SNKeys.ROWS_MIN]:
                self.convert_np_vals_for_json(all_totals.min()),
            self._keys[SNKeys.ROWS_MAX]:
                self.convert_np_vals_for_json(all_totals.max()),
            self._keys[SNKeys.ROWS_MEAN]:
                self.convert_np_vals_for_json(all_totals.mean()),
            self._keys[SNKeys.ROWS_MEDIAN]: self.convert_np_vals_for_json(
                np.median(all_totals, axis=0)[0, 0]),

            self._keys[SNKeys.ROWS_COUNT_MIN]:
                self.convert_np_vals_for_json(all_counts.min()),
            self._keys[SNKeys.ROWS_COUNT_MAX]:
                self.convert_np_vals_for_json(all_counts.max()),
            self._keys[SNKeys.ROWS_COUNT_MEAN]:
                self.convert_np_vals_for_json(all_counts.mean()),
            self._keys[SNKeys.ROWS_COUNT_MEDIAN]:
                self.convert_np_vals_for_json(np.median(all_counts, axis=0)),
        }
        return all_row_stats

    # ...............................................
    def get_column_stats(self, col_label=None):
        """Return statistics for a one or all columns.

        Args:
            col_label (str): label of one column to get statistics for.

        Returns:
            stats (dict): quantitative measures of one or all columns.

        Raises:
            IndexError: on label not present in column header.
        """
        if col_label is None:
            stats = self.get_all_column_stats()
        else:
            try:
                stats = self.get_one_column_stats(col_label)
            except IndexError:
                raise
        return stats

    # ...............................................
    def get_one_column_stats(self, col_label):
        """Get a dictionary of statistics for this col_label or all columns.

        Args:
            col_label: label on the column to gather stats for.

        Returns:
            stats (dict): quantitative measures of the column.

        Raises:
            IndexError: on label not present in column header

        Note:
            Inline comments are specific to a SUMMARY_TABLE_TYPES.SPECIES_DATASET_MATRIX
                with row/column/value = species/dataset/occ_count
        """
        # Get column (sparse array), and its index
        try:
            col, col_idx = self.get_vector_from_label(col_label, axis=1)
        except IndexError:
            raise
        stats = {
            self._keys[SNKeys.COL_IDX]: col_idx,
            self._keys[SNKeys.COL_LABEL]: col_label,
        }
        # Count of non-zero rows (Species) within this column (Dataset)
        stats[self._keys[SNKeys.COL_COUNT]] = self.convert_np_vals_for_json(col.nnz)
        # Largest/smallest occ count for dataset (column), and species (row)
        # containing that count
        maxval, max_col_labels = self.get_extreme_val_labels_for_vector(
            col, axis=1, is_max=True)
        minval, min_col_labels = self.get_extreme_val_labels_for_vector(
            col, axis=1, is_max=False)

        # Total Occurrences for Dataset
        stats[self._keys[SNKeys.COL_TOTAL]] = self.convert_np_vals_for_json(col.sum())
        # Return min occurrence count in this dataset
        stats[self._keys[SNKeys.COL_MIN_COUNT]] = self.convert_np_vals_for_json(minval)
        # Return number of species containing same minimum count (too many to list)
        stats[self._keys[SNKeys.COL_MIN_LABELS]] = len(min_col_labels)
        # Return max occurrence count in this dataset
        stats[self._keys[SNKeys.COL_MAX_COUNT]] = self.convert_np_vals_for_json(maxval)
        # Return species containing same maximum count
        stats[self._keys[SNKeys.COL_MAX_LABELS]] = max_col_labels
        return stats

    # ...............................................
    def get_all_column_stats(self):
        """Return stats (min, max, mean, median) of totals and counts for all columns.

        Returns:
            all_col_stats (dict): counts and statistics about all columns.
        """
        # Sum all rows for each column to return a row (numpy.ndarray, axis=0)
        all_totals = self._coo_array.sum(axis=0)
        # Get number of non-zero rows for every column (row, numpy.ndarray)
        all_counts = self._coo_array.getnnz(axis=0)
        # Count rows with at least one non-zero entry (all rows)
        col_count = self._coo_array.shape[0]
        all_col_stats = {
            self._keys[SNKeys.COLS_COUNT]: col_count,
            self._keys[SNKeys.COLS_TOTAL]:
                self.convert_np_vals_for_json(all_totals.sum()),
            self._keys[SNKeys.COLS_MIN]:
                self.convert_np_vals_for_json(all_totals.min()),
            self._keys[SNKeys.COLS_MAX]:
                self.convert_np_vals_for_json(all_totals.max()),
            self._keys[SNKeys.COLS_MEAN]:
                self.convert_np_vals_for_json(all_totals.mean()),
            self._keys[SNKeys.COLS_MEDIAN]:
                self.convert_np_vals_for_json(np.median(all_totals, axis=1)[0, 0]),

            self._keys[SNKeys.COLS_COUNT_MIN]:
                self.convert_np_vals_for_json(all_counts.min()),
            self._keys[SNKeys.COLS_COUNT_MAX]:
                self.convert_np_vals_for_json(all_counts.max()),
            self._keys[SNKeys.COLS_COUNT_MEAN]:
                self.convert_np_vals_for_json(all_counts.mean()),
            self._keys[SNKeys.COLS_COUNT_MEDIAN]:
                self.convert_np_vals_for_json(np.median(all_counts, axis=0)),
        }
        return all_col_stats

    # ...............................................
    def get_totals(self, axis):
        """Get a list of totals along the requested axis, down axis 0, across axis 1.

        Args:
            axis (int): Axis to sum.

        Returns:
            all_totals (list): list of values for the axis.
        """
        mtx = self._coo_array.sum(axis=axis)
        # 2d Matrix is a list of rows
        # Axis 0 produces a matrix shape (1, col_count), 1 row
        # Axis 1 produces matrix shape (row_count, 1), row_count rows
        if axis == 0:
            all_totals = mtx.tolist()[0]
        elif axis == 1:
            all_totals = mtx.T.tolist()[0]
        return all_totals

    # ...............................................
    def get_counts(self, axis):
        """Count non-zero values along the requested axis, down axis 0, across axis 1.

        Args:
            axis (int): Axis to count non-zero values for.

        Returns:
            all_counts (list): list of values for the axis.
        """
        all_counts = self._coo_array.getnnz(axis=axis)
        return all_counts

    # ...............................................
    def compare_column_to_others(self, col_label, agg_type=None):
        """Compare the number of rows and counts in rows to those of other columns.

        Args:
            col_label: label on the column to compare.
            agg_type: return stats on rows or values.  If None, return both.
                (options: "axis", "value", None)

        Returns:
            comparisons (dict): comparison measures
        """
        # Get this column stats
        stats = self.get_one_column_stats(col_label)
        # Show this column totals and counts compared to min, max, mean of all columns
        all_stats = self.get_all_column_stats()
        comparisons = {self._keys[SNKeys.COL_TYPE]: col_label}
        if agg_type in ("value", None):
            comparisons["Occurrences"] = {
                self._keys[SNKeys.COL_TOTAL]: stats[self._keys[SNKeys.COL_TOTAL]],
                self._keys[SNKeys.COLS_TOTAL]: all_stats[self._keys[SNKeys.COLS_TOTAL]],
                self._keys[SNKeys.COLS_MIN]: all_stats[self._keys[SNKeys.COLS_MIN]],
                self._keys[SNKeys.COLS_MAX]: all_stats[self._keys[SNKeys.COLS_MAX]],
                self._keys[SNKeys.COLS_MEAN]: all_stats[self._keys[SNKeys.COLS_MEAN]],
                self._keys[SNKeys.COLS_MEDIAN]: all_stats[self._keys[SNKeys.COLS_MEDIAN]]
            }
        if agg_type in ("axis", None):
            comparisons["Species"] = {
                self._keys[SNKeys.COL_COUNT]: stats[self._keys[SNKeys.COL_COUNT]],
                self._keys[SNKeys.COLS_COUNT]: all_stats[self._keys[SNKeys.COLS_COUNT]],
                self._keys[SNKeys.COLS_COUNT_MIN]:
                    all_stats[self._keys[SNKeys.COLS_COUNT_MIN]],
                self._keys[SNKeys.COLS_COUNT_MAX]:
                    all_stats[self._keys[SNKeys.COLS_COUNT_MAX]],
                self._keys[SNKeys.COLS_COUNT_MEAN]:
                    all_stats[self._keys[SNKeys.COLS_COUNT_MEAN]],
                self._keys[SNKeys.COLS_COUNT_MEDIAN]:
                    all_stats[self._keys[SNKeys.COLS_COUNT_MEDIAN]]
            }
        return comparisons

    # ...............................................
    def compare_row_to_others(self, row_label, agg_type=None):
        """Compare the number of columns and counts in columns to those of other rows.

        Args:
            row_label: label on the row to compare.
            agg_type: return stats on rows or values.  If None, return both.
                (options: "axis", "value", None)

        Returns:
            comparisons (dict): comparison measures
        """
        stats = self.get_one_row_stats(row_label)
        # Show this column totals and counts compared to min, max, mean of all columns
        all_stats = self.get_all_row_stats()
        comparisons = {self._keys[SNKeys.ROW_TYPE]: row_label}
        if agg_type in ("value", None):
            comparisons["Occurrences"] = {
                self._keys[SNKeys.ROW_TOTAL]: stats[self._keys[SNKeys.ROW_TOTAL]],
                self._keys[SNKeys.ROWS_TOTAL]: all_stats[self._keys[SNKeys.ROWS_TOTAL]],
                self._keys[SNKeys.ROWS_MIN]: all_stats[self._keys[SNKeys.ROWS_MIN]],
                self._keys[SNKeys.ROWS_MAX]: all_stats[self._keys[SNKeys.ROWS_MAX]],
                self._keys[SNKeys.ROWS_MEAN]: all_stats[self._keys[SNKeys.ROWS_MEAN]],
                self._keys[SNKeys.ROWS_MEDIAN]:
                    all_stats[self._keys[SNKeys.ROWS_MEDIAN]],
            }
        if agg_type in ("axis", None):
            comparisons["Datasets"] = {
                self._keys[SNKeys.ROW_COUNT]: stats[self._keys[SNKeys.ROW_COUNT]],
                self._keys[SNKeys.ROWS_COUNT]: all_stats[self._keys[SNKeys.ROWS_COUNT]],
                self._keys[SNKeys.ROWS_COUNT_MIN]:
                    all_stats[self._keys[SNKeys.ROWS_COUNT_MIN]],
                self._keys[SNKeys.ROWS_COUNT_MAX]:
                    all_stats[self._keys[SNKeys.ROWS_COUNT_MAX]],
                self._keys[SNKeys.ROWS_COUNT_MEAN]:
                    all_stats[self._keys[SNKeys.ROWS_COUNT_MEAN]],
                self._keys[SNKeys.ROWS_COUNT_MEDIAN]:
                    all_stats[self._keys[SNKeys.ROWS_COUNT_MEDIAN]]
            }
        return comparisons

    # .............................................................................
    def compress_to_file(self, local_path="/tmp"):
        """Compress this SparseMatrix to a zipped npz and json file.

        Args:
            local_path (str): Absolute path of local destination path

        Returns:
            zip_fname (str): Local output zip filename.

        Raises:
            Exception: on failure to write sparse matrix to NPZ file.
            Exception: on failure to serialize or write metadata.
            Exception: on failure to write matrix and metadata files to zipfile.
        """
        # Always delete local files before compressing this data.
        [mtx_fname, meta_fname, zip_fname] = self._remove_expected_files(
            local_path=local_path)

        # Save matrix to npz locally
        try:
            scipy.sparse.save_npz(mtx_fname, self._coo_array, compressed=True)
        except Exception as e:
            msg = f"Failed to write {mtx_fname}: {e}"
            self._logme(msg, log_level=ERROR)
            raise Exception(msg)

        # Save table data and categories to json locally
        metadata = Summaries.get_table(self._table_type)
        metadata["row"] = self._row_categ.categories.tolist()
        metadata["column"] = self._col_categ.categories.tolist()
        try:
            self._dump_metadata(metadata, meta_fname)
        except Exception:
            raise

        # Compress matrix with metadata
        try:
            self._compress_files([mtx_fname, meta_fname], zip_fname)
        except Exception:
            raise

        return zip_fname

    # .............................................................................
    @classmethod
    def uncompress_zipped_data(
            cls, zip_filename, local_path="/tmp", overwrite=False):
        """Uncompress a zipped SparseMatrix into a coo_array and row/column categories.

        Args:
            zip_filename (str): Filename of zipped sparse matrix data to uncompress.
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
            Exception: on failure to uncompress files.
            Exception: on failure to load data from uncompressed files.

        Note:
            All filenames have the same basename with extensions indicating which data
                they contain. The filename contains a string like YYYY-MM-DD which
                indicates which GBIF data dump the statistics were built upon.
        """
        try:
            mtx_fname, meta_fname, table_type, data_datestr = cls._uncompress_files(
                zip_filename, local_path=local_path, overwrite=overwrite)
        except Exception:
            raise

        try:
            sparse_coo, row_categ, col_categ = cls.read_data(mtx_fname, meta_fname)
        except Exception:
            raise

        return sparse_coo, row_categ, col_categ, table_type, data_datestr

    # .............................................................................
    @classmethod
    def read_data(cls, mtx_filename, meta_filename):
        """Read SparseMatrix data files into a coo_array and row/column categories.

        Args:
            mtx_filename (str): Filename of scipy.sparse.coo_array data in npz format.
            meta_filename (str): Filename of JSON sparse matrix metadata.

        Returns:
            sparse_coo (scipy.sparse.coo_array): Sparse Matrix containing data.
            row_categ (pandas.api.types.CategoricalDtype): row categories
            col_categ (pandas.api.types.CategoricalDtype): column categories
            table_type (aws.aws_constants.SUMMARY_TABLE_TYPES): type of table data
            data_datestr (str): date string in format YYYY_MM_DD

        Raises:
            Exception: on unable to load NPZ file
            Exception: on unable to load JSON metadata file
            Exception: on missing row categories in JSON
            Exception: on missing column categories in JSON

        Note:
            All filenames have the same basename with extensions indicating which data
                they contain. The filename contains a string like YYYY-MM-DD which
                indicates which GBIF data dump the statistics were built upon.
        """
        # Read sparse matrix from npz file
        try:
            sparse_coo = scipy.sparse.load_npz(mtx_filename)
        except Exception as e:
            raise Exception(f"Failed to load {mtx_filename}: {e}")

        # Read JSON dictionary as string
        try:
            meta_dict = cls.load_metadata(meta_filename)
        except Exception:
            raise

        # Parse metadata into objects for matrix construction
        try:
            row_catlst = meta_dict.pop("row")
        except KeyError:
            raise Exception(f"Missing row categories in {meta_filename}")
        else:
            row_categ = CategoricalDtype(row_catlst, ordered=True)
        try:
            col_catlst = meta_dict.pop("column")
        except KeyError:
            raise Exception(f"Missing column categories in {meta_filename}")
        else:
            col_categ = CategoricalDtype(col_catlst, ordered=True)

        return sparse_coo, row_categ, col_categ
