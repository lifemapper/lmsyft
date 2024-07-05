"""Tools to compute dataset x species statistics from S3 data."""
from logging import INFO
import os

from sppy.aws.aws_constants import (LOCAL_OUTDIR, PROJ_BUCKET, REGION, SUMMARY_FOLDER)
from sppy.aws.aws_tools import (
    download_from_s3, get_current_datadate_str, get_today_str,
    read_s3_parquet_to_pandas
)
from sppy.tools.s2n.constants import (Summaries, SUMMARY_TABLE_TYPES)
from sppy.tools.s2n.sparse_matrix import SparseMatrix
from sppy.tools.s2n.summary_matrix import SummaryMatrix
from sppy.tools.util.logtools import Logger, logit
from sppy.tools.util.utils import upload_to_s3


# ...............................................
def get_extreme_val_and_attrs_for_column_from_stacked_data(
        stacked_df, filter_label, filter_value, attr_label, val_label, is_max=True):
    """Find the minimum or maximum value for rows where 'filter_label' = 'filter_value'.

    Args:
        stacked_df: dataframe containing stacked data records
        filter_label: column name for filtering.
        filter_value: column value for filtering.
        attr_label: column name of attribute to return.
        val_label: column name for attribute with min/max value.
        is_max (bool): flag indicating whether to get maximum (T) or minimum (F)

    Returns:
        target_val:  Minimum or maximum value for rows where
            'filter_label' = 'filter_value'.
        attr_vals: values for attr_label for rows with the minimum or maximum value.

    Raises:
        Exception: on min/max = 0.  Zeros should never be returned for min or max value.
    """
    # Create a dataframe of rows where column 'filter_label' = 'filter_value'.
    tmp_df = stacked_df.loc[stacked_df[filter_label] == filter_value]
    # Find the min or max value for those rows
    if is_max is True:
        target_val = tmp_df[val_label].max()
    else:
        target_val = tmp_df[val_label].min()
        # There should be NO zeros in these aggregated records
    if target_val == 0:
        raise Exception(
            f"Found value 0 in column {val_label} for rows where "
            f"{filter_label} == {filter_value}")
    # Get the attribute(s) in the row(s) with the max value
    attrs_containing_max_df = tmp_df.loc[tmp_df[val_label] == target_val]
    attr_vals = [rec for rec in attrs_containing_max_df[attr_label]]
    return target_val, attr_vals


# ...............................................
def sum_stacked_data_vals_for_column(stacked_df, filter_label, filter_value, val_label):
    """Sum the values for rows where column 'filter_label' = 'filter_value'.

    Args:
        stacked_df: dataframe containing stacked data records
        filter_label: column name for filtering.
        filter_value: column value for filtering.
        val_label: column name for summation.

    Returns:
        tmp_df: dataframe containing only rows with a value of filter_value in column
            filter_label.
    """
    # Create a dataframe of rows where column 'filter_label' = 'filter_value'.
    tmp_df = stacked_df.loc[stacked_df[filter_label] == filter_value]
    # Sum the values for those rows
    count = tmp_df[val_label].sum()
    return count


# ...............................................
def test_row_col_comparisons(agg_sparse_mtx, test_count=5, logger=None):
    """Test row comparisons between 1 and all, and column comparisons between 1 and all.

    Args:
        agg_sparse_mtx (SparseMatrix): object containing a scipy.sparse.coo_array
            with 3 columns from the stacked_df arranged as rows and columns with values
        test_count (int): number of rows and columns to test.
        logger (object): logger for saving relevant processing messages

    Postcondition:
        Printed information for successful or failed tests.

    Note: The aggregate_df must have been created from the stacked_df.
    """
    y_vals = agg_sparse_mtx.get_random_labels(test_count, axis=0)
    x_vals = agg_sparse_mtx.get_random_labels(test_count, axis=1)
    for y in y_vals:
        row_comps = agg_sparse_mtx.compare_row_to_others(y)
        logit(logger, "Row comparisons:", print_obj=row_comps)
    for x in x_vals:
        col_comps = agg_sparse_mtx.compare_column_to_others(x)
        logit(logger, "Column comparisons:", print_obj=col_comps)


# ...............................................
def test_stacked_to_aggregate_sum(
        stk_df, stk_axis_col_label, stk_val_col_label, agg_sparse_mtx, agg_axis=0,
        test_count=5, logger=None):
    """Test for equality of sums in stacked and aggregated dataframes.

    Args:
        stk_df: dataframe of stacked data, containing records with columns of
            categorical values and counts.
        stk_axis_col_label: column label in stacked dataframe to be used as the column
            labels of the axis in the aggregate sparse matrix.
        stk_val_col_label: column label in stacked dataframe for data to be used as
            value in the aggregate sparse matrix.
        agg_sparse_mtx (SparseMatrix): object containing a scipy.sparse.coo_array
            with 3 columns from the stacked_df arranged as rows and columns with values]
        agg_axis (int): Axis 0 (row) or 1 (column) that corresponds with the column
            label (stk_axis_col_label) in the original stacked data.
        test_count (int): number of rows and columns to test.
        logger (object): logger for saving relevant processing messages

    Postcondition:
        Printed information for successful or failed tests.

    Note: The aggregate_df must have been created from the stacked_df.
    """
    labels = agg_sparse_mtx.get_random_labels(test_count, axis=agg_axis)
    # Test stacked column totals against aggregate x columns
    for lbl in labels:
        stk_sum = sum_stacked_data_vals_for_column(
            stk_df, stk_axis_col_label, lbl, stk_val_col_label)
        agg_sum = agg_sparse_mtx.sum_vector(lbl, axis=agg_axis)
        logit(logger, f"Test axis {agg_axis}: {lbl}")
        if stk_sum == agg_sum:
            logit(
                logger, f"  Total {stk_sum}: Stacked data for "
                f"{stk_axis_col_label} == aggregate data in axis {agg_axis}: {lbl}"
            )
        else:
            logit(
                logger, f"  !!! {stk_sum} != {agg_sum}: Stacked data for "
                f"{stk_axis_col_label} != aggregate data in axis {agg_axis}: {lbl}"
            )
        logit(logger, "")
    logit(logger, "")


# ...............................................
def test_stacked_to_aggregate_extremes(
        stk_df, stk_col_label_for_axis0, stk_col_label_for_axis1, stk_col_label_for_val,
        agg_sparse_mtx, agg_axis=0, test_count=5, logger=None, is_max=True):
    """Test min/max counts for attributes in the sparse matrix vs. the stacked data.

    Args:
        stk_df: dataframe of stacked data, containing records with columns of
            categorical values and counts.
        stk_col_label_for_axis0: column label in stacked dataframe to be used as the
            row (axis 0) labels of the axis in the aggregate sparse matrix.
        stk_col_label_for_axis1: column label in stacked dataframe to be used as the
            column (axis 1) labels of the axis in the aggregate sparse matrix.
        stk_col_label_for_val: column label in stacked dataframe for data to be used as
            value in the aggregate sparse matrix.
        agg_sparse_mtx (SparseMatrix): object containing a scipy.sparse.coo_array
            with 3 columns from the stacked_df arranged as rows and columns with values]
        agg_axis (int): Axis 0 (row) or 1 (column) that corresponds with the column
            label (stk_axis_col_label) in the original stacked data.
        test_count (int): number of rows and columns to test.
        logger (object): logger for saving relevant processing messages
        is_max (bool): flag indicating whether to test maximum (T) or minimum (F)

    Postcondition:
        Printed information for successful or failed tests.

    Note: The aggregate_df must have been created from the stacked_df.
    """
    labels = agg_sparse_mtx.get_random_labels(test_count, axis=agg_axis)
    # for logging
    if is_max is True:
        extm = "Max"
    else:
        extm = "Min"

    # Get min/max of row (identified by filter_label, attr_label in axis 0)
    if agg_axis == 0:
        filter_label, attr_label = stk_col_label_for_axis0, stk_col_label_for_axis1
    # Get min/max of column (identified by label in axis 1)
    elif agg_axis == 1:
        filter_label, attr_label = stk_col_label_for_axis1, stk_col_label_for_axis0

    # Test dataset - get species with largest count and compare
    for lbl in labels:
        (stk_target_val,
         stk_attr_vals) = get_extreme_val_and_attrs_for_column_from_stacked_data(
            stk_df, filter_label, lbl, attr_label, stk_col_label_for_val, is_max=is_max)
        agg_target_val, agg_labels = agg_sparse_mtx.get_extreme_val_labels_for_vector(
            lbl, axis=agg_axis, is_max=is_max)
        logit(logger, f"Test vector {lbl} on axis {agg_axis}")
        if stk_target_val == agg_target_val:
            logit(logger, f"  {extm} values equal {stk_target_val}")
            if set(stk_attr_vals) == set(agg_labels):
                logit(
                    logger, f"  {extm} value labels equal; "
                    f"len={len(stk_attr_vals)}")
            else:
                logit(
                    logger, f"  !!! {extm} value labels NOT equal; "
                    f"stacked labels {stk_attr_vals} != agg labels {agg_labels}"
                )
        else:
            logit(
                logger, f"!!! {extm} stacked value {stk_target_val} != "
                f"{agg_target_val} agg value")
        logit(logger, "")
    logit(logger, "")


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    """Main script creates a SPECIES_DATASET_MATRIX from DATASET_SPECIES_LISTS."""
    data_datestr = get_current_datadate_str()
    # Create a logger
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    todaystr = get_today_str()
    log_name = f"{script_name}_{todaystr}"
    # Create logger with default INFO messages
    tst_logger = Logger(
        log_name, log_path=LOCAL_OUTDIR, log_console=True, log_level=INFO)

    stacked_table_type = SUMMARY_TABLE_TYPES.DATASET_SPECIES_LISTS
    mtx_table_type = SUMMARY_TABLE_TYPES.SPECIES_DATASET_MATRIX

    local_path = "/tmp"

    # Datasets in rows/x/axis 1
    table = Summaries.get_table(stacked_table_type, data_datestr)
    stk_col_label_for_axis1 = table["key_fld"]
    stk_col_label_for_val = table["value_fld"]
    # Dict of new fields constructed from existing fields, just 1 for species key/name
    fld_mods = table["combine_fields"]
    # Species (taxonKey + name) in columns/y/axis 0
    stk_col_label_for_axis0 = list(fld_mods.keys())[0]
    (fld1, fld2) = fld_mods[stk_col_label_for_axis0]
    pqt_fname = f"{table['fname']}.parquet"

    # Read stacked (record) data directly into DataFrame
    stk_df = read_s3_parquet_to_pandas(
        PROJ_BUCKET, SUMMARY_FOLDER, pqt_fname, tst_logger, s3_client=None
    )

    # .................................
    # Combine key and species fields to ensure uniqueness
    def _combine_columns(row):
        return str(row[fld1]) + ' ' + str(row[fld2])
    # ......................
    stk_df[stk_col_label_for_axis0] = stk_df.apply(_combine_columns, axis=1)
    # .................................

    # Create matrix from record data
    agg_sparse_mtx = SparseMatrix.init_from_stacked_data(
        stk_df, stk_col_label_for_axis1, stk_col_label_for_axis0, stk_col_label_for_val,
        mtx_table_type, data_datestr, logger=tst_logger)

    # Test raw counts between stacked data and sparse matrix
    for stk_lbl, axis in ((stk_col_label_for_axis0, 0), (stk_col_label_for_axis1, 1)):
        # Test stacked column used for axis 0/1 against sparse matrix axis 0/1
        test_stacked_to_aggregate_sum(
            stk_df, stk_lbl, stk_col_label_for_val, agg_sparse_mtx, agg_axis=axis,
            test_count=5, logger=tst_logger)

    # Test min/max values for rows/columns
    for is_max in (False, True):
        for axis in (0, 1):
            test_stacked_to_aggregate_extremes(
                stk_df, stk_col_label_for_axis0, stk_col_label_for_axis1,
                stk_col_label_for_val, agg_sparse_mtx, agg_axis=axis, test_count=5,
                logger=tst_logger, is_max=is_max)

    # .................................
    # Save matrix to S3
    out_filename = agg_sparse_mtx.compress_to_file()
    upload_to_s3(out_filename, PROJ_BUCKET, SUMMARY_FOLDER, REGION)
    # agg_sparse_mtx.write_to_s3(PROJ_BUCKET, SUMMARY_FOLDER, out_filename, REGION)

    # Copy logfile to S3
    upload_to_s3(tst_logger.filename, PROJ_BUCKET, SUMMARY_FOLDER, REGION)
    s3_logfile = agg_sparse_mtx.copy_logfile_to_s3(PROJ_BUCKET, SUMMARY_FOLDER, REGION)
    print(s3_logfile)

    # .................................
    table = Summaries.get_table(mtx_table_type, data_datestr)
    zip_fname = f"{table['fname']}.zip"
    # Only download if file does not exist
    zip_filename = download_from_s3(
        PROJ_BUCKET, SUMMARY_FOLDER, zip_fname, local_path=local_path,
        overwrite=False)

    # Only extract if files do not exist
    sparse_coo, row_categ, col_categ, table_type, _data_datestr = \
        SparseMatrix.uncompress_zipped_data(
            zip_filename, local_path=local_path, overwrite=False)
    # Create
    sp_mtx = SparseMatrix(
        sparse_coo, mtx_table_type, data_datestr, row_category=row_categ,
        column_category=col_categ, logger=tst_logger)

    sp_sum_mtx = SummaryMatrix.init_from_sparse_matrix(sp_mtx, axis=0, logger=tst_logger)

    out_filename = sp_sum_mtx.compress_to_file()
    upload_to_s3(out_filename, PROJ_BUCKET, SUMMARY_FOLDER, REGION)

    SummaryMatrix.uncompress_zipped_data()

"""
from sppy.aws.aggregate_matrix import *
from sppy.aws.sparse_matrix import *
from sppy.aws.summary_matrix import *

# Create a logger
script_name = "testing"
todaystr = get_today_str()
log_name = f"{script_name}_{todaystr}"
data_datestr = get_current_datadate_str()

# Create logger with default INFO messages
tst_logger = Logger(
    log_name, log_path=LOCAL_OUTDIR, log_console=True, log_level=INFO)

stacked_table_type = SUMMARY_TABLE_TYPES.DATASET_SPECIES_LISTS
mtx_table_type = SUMMARY_TABLE_TYPES.SPECIES_DATASET_MATRIX

local_path = "/tmp"

# Datasets in rows/x/axis 1
table = Summaries.get_table(stacked_table_type, data_datestr)
stk_col_label_for_axis1 = table["key_fld"]
stk_col_label_for_val = table["value_fld"]
# Dict of new fields constructed from existing fields, just 1 for species key/name
fld_mods = table["combine_fields"]
# Species (taxonKey + name) in columns/y/axis 0
stk_col_label_for_axis0 = list(fld_mods.keys())[0]
(fld1, fld2) = fld_mods[stk_col_label_for_axis0]
pqt_fname = f"{table['fname']}.parquet"

# Read stacked (record) data directly into DataFrame
stk_df = read_s3_parquet_to_pandas(
    PROJ_BUCKET, SUMMARY_FOLDER, pqt_fname, tst_logger, s3_client=None
)

# .................................
# Combine key and species fields to ensure uniqueness
def _combine_columns(row):
    return str(row[fld1]) + ' ' + str(row[fld2])

# ......................
stk_df[stk_col_label_for_axis0] = stk_df.apply(_combine_columns, axis=1)
# .................................

# Create matrix from record data
agg_sparse_mtx = SparseMatrix.init_from_stacked_data(
    stk_df, stk_col_label_for_axis1, stk_col_label_for_axis0, stk_col_label_for_val,
    mtx_table_type, data_datestr, logger=tst_logger)

# Test raw counts between stacked data and sparse matrix
for stk_lbl, axis in ((stk_col_label_for_axis0, 0), (stk_col_label_for_axis1, 1)):
    # Test stacked column used for axis 0/1 against sparse matrix axis 0/1
    test_stacked_to_aggregate_sum(
        stk_df, stk_lbl, stk_col_label_for_val, agg_sparse_mtx, agg_axis=axis,
        test_count=5, logger=tst_logger)

# Test min/max values for rows/columns
for is_max in (False, True):
    for axis in (0, 1):
        test_stacked_to_aggregate_extremes(
            stk_df, stk_col_label_for_axis0, stk_col_label_for_axis1,
            stk_col_label_for_val, agg_sparse_mtx, agg_axis=axis, test_count=5,
            logger=tst_logger, is_max=is_max)

# .................................
# Save matrix to S3
out_filename = agg_sparse_mtx.compress_to_file()
agg_sparse_mtx.write_to_s3(PROJ_BUCKET, SUMMARY_FOLDER, out_filename, REGION)

# Copy logfile to S3
s3_logfile = agg_sparse_mtx.write_to_s3(PROJ_BUCKET, SUMMARY_FOLDER, tst_logger.filename, REGION)
print(s3_logfile)

zip_filename = download_from_s3(
    PROJ_BUCKET, SUMMARY_FOLDER, zip_fname, local_path=local_path,
    overwrite=False)

# .................................
# Read matrix from S3
table = Summaries.get_table(mtx_table_type, data_datestr)
zip_fname = f"{table['fname']}.zip"
# Only download if file does not exist
zip_filename = download_from_s3(
    PROJ_BUCKET, SUMMARY_FOLDER, zip_fname, local_path=local_path,
    overwrite=False)

# Only extract if files do not exist
sparse_coo, row_categ, col_categ, table_type, new_data_datestr = \
    SparseMatrix.uncompress_zipped_data(
        zip_filename, local_path=local_path, overwrite=False)

# Create
sp_mtx = SparseMatrix(
    sparse_coo, mtx_table_type, data_datestr, row_category=row_categ,
    column_category=col_categ, logger=tst_logger)

# .................................
# Create summary matrix from sparse matrix


# --------------------------------------------------------------------------------------
# Testing
# --------------------------------------------------------------------------------------

x_vals = get_random_values_from_stacked_data(stk_df, x_col_label, test_count)
y_vals = get_random_values_from_stacked_data(stk_df, y_col_label, test_count)

y_vals = agg_sparse_mtx.get_random_labels(test_count, axis=0)
x_vals = agg_sparse_mtx.get_random_labels(test_count, axis=1)

x = x_vals[0]
y = y_vals[0]
row_label = y
col_label = x
sp_mtx.get_row_stats(y)
sp_mtx.get_column_stats(x)

"""
