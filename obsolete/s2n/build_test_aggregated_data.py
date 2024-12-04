"""Tools to compute dataset x species statistics from S3 data."""
from logging import INFO
import os

from spnet.aws.constants import (PROJ_BUCKET, REGION, SUMMARY_FOLDER)
from spnet.aws.tools import (
    create_parquet_lookup_from_tsv, download_dataset_lookup, download_from_s3,
    get_current_datadate_str, get_today_str, read_s3_parquet_to_pandas, upload_to_s3
)
from sppy.common.constants import (Summaries, SUMMARY_TABLE_TYPES, TMP_PATH)
from spnet.common.log import Logger, logit
from obsolete.s2n.sparse_matrix import SparseMatrix
from obsolete.s2n.summary_matrix import SummaryMatrix


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

    Raises:
        IndexError: on failure to find label in axis.

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
        # Get stacked data results
        (stk_target_val,
         stk_attr_vals) = get_extreme_val_and_attrs_for_column_from_stacked_data(
            stk_df, filter_label, lbl, attr_label, stk_col_label_for_val, is_max=is_max)
        # Get sparse matrix results
        try:
            # Get row/column (sparse array), and its index
            vector, vct_idx = agg_sparse_mtx.get_vector_from_label(lbl, axis=agg_axis)
        except IndexError:
            raise
        agg_target_val, agg_labels = agg_sparse_mtx.get_extreme_val_labels_for_vector(
            vector, axis=agg_axis, is_max=is_max)
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


# ...............................................
def read_stacked_data_records(data_datestr, logger):
    """Read stacked records from S3, aggregate into a sparse matrix of species x dataset.

    Args:
        data_datestr (str): date of the current dataset, in YYYY_MM_DD format
        logger (object): logger for saving relevant processing messages

    Returns:
        agg_sparse_mtx (sppy.tools.s2n.sparse_matrix.SparseMatrix): sparse matrix
            containing data separated into 2 dimensions
    """
    # Datasets in rows/x/axis 1
    stacked_record_table = Summaries.get_table(
        SUMMARY_TABLE_TYPES.DATASET_SPECIES_LISTS, data_datestr)
    stk_col_label_for_axis1 = stacked_record_table["key_fld"]
    stk_col_label_for_val = stacked_record_table["value_fld"]
    # Dict of new fields constructed from existing fields, just 1 for species key/name
    fld_mods = stacked_record_table["combine_fields"]
    # Species (taxonKey + name) in columns/y/axis 0
    stk_col_label_for_axis0 = list(fld_mods.keys())[0]
    (fld1, fld2) = fld_mods[stk_col_label_for_axis0]
    pqt_fname = f"{stacked_record_table['fname']}.parquet"
    # Read stacked (record) data directly into DataFrame
    stk_df = read_s3_parquet_to_pandas(
        PROJ_BUCKET, SUMMARY_FOLDER, pqt_fname, logger, s3_client=None
    )

    # .................................
    # Combine key and species fields to ensure uniqueness
    def _combine_columns(row):
        return str(row[fld1]) + ' ' + str(row[fld2])

    # Combine 2 columns to create a guaranteed unique column
    stk_df[stk_col_label_for_axis0] = stk_df.apply(_combine_columns, axis=1)
    return (
        stk_col_label_for_axis0, stk_col_label_for_axis1, stk_col_label_for_val, stk_df
    )


# ...............................................
def create_dataset_metadata():
    """Download GBIF Dataset metadata, write to parquet, upload to S3.

    Returns:
        s3_filename: the full path/identifier of the data on S3.

    Raises:
        Exception: on failure to download metadata from GBIF.
        Exception: on failure to create a local parquet file.
        Exception: on failure to upload parquet file to S3.
    """
    try:
        tsv_filename = download_dataset_lookup()
    except Exception:
        raise

    try:
        tmp_parquet_fname = create_parquet_lookup_from_tsv(tsv_filename)
    except Exception:
        raise
    _ = os.path.basename(tmp_parquet_fname)
    # output_s3_path = f"{SUMMARY_FOLDER}/{out_fname}"
    s3_filename = upload_to_s3(tmp_parquet_fname, PROJ_BUCKET, SUMMARY_FOLDER)
    return s3_filename


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    """Main script creates a SPECIES_DATASET_MATRIX from DATASET_SPECIES_LISTS."""
    data_datestr = get_current_datadate_str()
    overwrite = True
    mtx_table_type = SUMMARY_TABLE_TYPES.SPECIES_DATASET_MATRIX
    local_path = "/tmp"
    # .................................
    # Create a logger
    # .................................
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    todaystr = get_today_str()
    log_name = f"{script_name}_{todaystr}"
    # Create logger with default INFO messages
    logger = Logger(
        log_name, log_path=TMP_PATH, log_console=True, log_level=INFO)

    # .................................
    # Create a dataframe from stacked records
    # .................................
    stk_col_label_for_axis0, stk_col_label_for_axis1, stk_col_label_for_val, stk_df = \
        read_stacked_data_records(data_datestr, logger)

    # .................................
    # Create matrix from record data
    # .................................
    agg_sparse_mtx = SparseMatrix.init_from_stacked_data(
        stk_df, stk_col_label_for_axis1, stk_col_label_for_axis0, stk_col_label_for_val,
        mtx_table_type, data_datestr, logger=logger)

    # .................................
    # Test consistency between stacked data and sparse matrix
    # .................................
    # Test raw counts
    for stk_lbl, axis in ((stk_col_label_for_axis0, 0), (stk_col_label_for_axis1, 1)):
        # Test stacked column used for axis 0/1 against sparse matrix axis 0/1
        test_stacked_to_aggregate_sum(
            stk_df, stk_lbl, stk_col_label_for_val, agg_sparse_mtx, agg_axis=axis,
            test_count=5, logger=logger)

    # Test min/max values for rows/columns
    for is_max in (False, True):
        for axis in (0, 1):
            test_stacked_to_aggregate_extremes(
                stk_df, stk_col_label_for_axis0, stk_col_label_for_axis1,
                stk_col_label_for_val, agg_sparse_mtx, agg_axis=axis, test_count=5,
                logger=logger, is_max=is_max)

    # .................................
    # Save sparse matrix to S3 then clear
    # .................................
    out_filename = agg_sparse_mtx.compress_to_file()
    upload_to_s3(out_filename, PROJ_BUCKET, SUMMARY_FOLDER, REGION)
    # Copy logfile to S3
    upload_to_s3(logger.filename, PROJ_BUCKET, SUMMARY_FOLDER, REGION)
    agg_sparse_mtx = None

    # .................................
    # Download data and create sparse matrix from S3 file to ensure consistency
    # .................................
    table = Summaries.get_table(mtx_table_type, data_datestr)
    zip_fname = f"{table['fname']}.zip"
    # Only download if file does not exist
    zip_filename = download_from_s3(
        PROJ_BUCKET, SUMMARY_FOLDER, zip_fname, local_path=local_path,
        overwrite=overwrite)

    # Only extract if files do not exist
    sparse_coo, row_categ, col_categ, table_type, _data_datestr = \
        SparseMatrix.uncompress_zipped_data(
            zip_filename, local_path=local_path, overwrite=overwrite)

    # Create
    agg_sparse_mtx = SparseMatrix(
        sparse_coo, mtx_table_type, data_datestr, row_categ, col_categ,
        logger=logger)

    # .................................
    # Test consistency between stacked data and sparse matrix from S3 file
    # .................................
    # Test raw counts
    for stk_lbl, axis in ((stk_col_label_for_axis0, 0), (stk_col_label_for_axis1, 1)):
        # Test stacked column used for axis 0/1 against sparse matrix axis 0/1
        test_stacked_to_aggregate_sum(
            stk_df, stk_lbl, stk_col_label_for_val, agg_sparse_mtx, agg_axis=axis,
            test_count=5, logger=logger)

    # Test min/max values for rows/columns
    for is_max in (False, True):
        for axis in (0, 1):
            test_stacked_to_aggregate_extremes(
                stk_df, stk_col_label_for_axis0, stk_col_label_for_axis1,
                stk_col_label_for_val, agg_sparse_mtx, agg_axis=axis, test_count=5,
                logger=logger, is_max=is_max)

    # .................................
    # Create a summary matrix for each dimension of sparse matrix and upload
    # .................................
    sp_sum_mtx = SummaryMatrix.init_from_sparse_matrix(agg_sparse_mtx, axis=0, logger=logger)
    spsum_table_type = sp_sum_mtx.table_type
    sp_sum_filename = sp_sum_mtx.compress_to_file()

    ds_sum_mtx = SummaryMatrix.init_from_sparse_matrix(agg_sparse_mtx, axis=1, logger=logger)
    dssum_table_type = ds_sum_mtx.table_type
    ds_sum_filename = ds_sum_mtx.compress_to_file()

    # .................................
    # TODO: Test summary matrices against sparse matrix
    # .................................

    # .................................
    # Upload summary matrix files
    # .................................
    upload_to_s3(sp_sum_filename, PROJ_BUCKET, SUMMARY_FOLDER, REGION)
    upload_to_s3(ds_sum_filename, PROJ_BUCKET, SUMMARY_FOLDER, REGION)

    # .................................
    # Download summary matrix files and recreate 2 summary matrices to test for corruption
    # .................................
    # Species Summary
    sp_table = Summaries.get_table(spsum_table_type, data_datestr)
    sp_zip_fname = f"{sp_table['fname']}.zip"
    sp_zip_filename = download_from_s3(
        PROJ_BUCKET, SUMMARY_FOLDER, sp_zip_fname, local_path=local_path,
        overwrite=overwrite)

    sp_dataframe, sp_meta_dict, sp_table_type, data_datestr = \
        SummaryMatrix.uncompress_zipped_data(
            sp_zip_filename, local_path=local_path, overwrite=overwrite)

    # Dataset Summary
    ds_table = Summaries.get_table(dssum_table_type, data_datestr)
    ds_zip_fname = f"{ds_table['fname']}.zip"
    ds_zip_filename = download_from_s3(
        PROJ_BUCKET, SUMMARY_FOLDER, ds_zip_fname, local_path=local_path,
        overwrite=overwrite)

    ds_dataframe, ds_meta_dict, ds_table_type, data_datestr = \
        SummaryMatrix.uncompress_zipped_data(
            ds_zip_filename, local_path=local_path, overwrite=overwrite)

    # .................................
    # TODO: Test summary matrices
    # .................................

    # .................................
    # TODO: Create dataset title lookup and upload to S3
    # .................................
    dataset_lookup_s3_name = create_dataset_metadata()

    # .................................
    # TODO: Test dataset lookup for random labels in sparse matrix
    # .................................

"""
from sppy.tools.s2n.build_test_aggregated_data import *

data_datestr = get_current_datadate_str()
overwrite = True
mtx_table_type = SUMMARY_TABLE_TYPES.SPECIES_DATASET_MATRIX
local_path = "/tmp"
# .................................
# Create a logger
# .................................
script_name = "build_test_aggregated_data"
todaystr = get_today_str()
log_name = f"{script_name}_{todaystr}"
# Create logger with default INFO messages
logger = Logger(
    log_name, log_path=TMP_DIR, log_console=True, log_level=INFO)

# .................................
# Create a dataframe from stacked records
# .................................
stk_col_label_for_axis0, stk_col_label_for_axis1, stk_col_label_for_val, stk_df = \
    read_stacked_data_records(data_datestr, logger)

# .................................
# Create matrix from record data
# .................................
agg_sparse_mtx = SparseMatrix.init_from_stacked_data(
    stk_df, stk_col_label_for_axis1, stk_col_label_for_axis0, stk_col_label_for_val,
    mtx_table_type, data_datestr, logger=logger)

# .................................
# Test consistency between stacked data and sparse matrix
# .................................
# Test raw counts
for stk_lbl, axis in ((stk_col_label_for_axis0, 0), (stk_col_label_for_axis1, 1)):
    # Test stacked column used for axis 0/1 against sparse matrix axis 0/1
    test_stacked_to_aggregate_sum(
        stk_df, stk_lbl, stk_col_label_for_val, agg_sparse_mtx, agg_axis=axis,
        test_count=5, logger=logger)

# Test min/max values for rows/columns
for is_max in (False, True):
    for axis in (0, 1):
        test_stacked_to_aggregate_extremes(
            stk_df, stk_col_label_for_axis0, stk_col_label_for_axis1,
            stk_col_label_for_val, agg_sparse_mtx, agg_axis=axis, test_count=5,
            logger=logger, is_max=is_max)

# .................................
# Save sparse matrix to S3 then clear
# .................................
out_filename = agg_sparse_mtx.compress_to_file()
upload_to_s3(out_filename, PROJ_BUCKET, SUMMARY_FOLDER, REGION)
# Copy logfile to S3
upload_to_s3(logger.filename, PROJ_BUCKET, SUMMARY_FOLDER, REGION)
agg_sparse_mtx = None

# .................................
# Download data and recreate sparse matrix to test for corruption
# .................................
table = Summaries.get_table(mtx_table_type, data_datestr)
zip_fname = f"{table['fname']}.zip"
# Only download if file does not exist
zip_filename = download_from_s3(
    PROJ_BUCKET, SUMMARY_FOLDER, zip_fname, local_path=local_path,
    overwrite=overwrite)

# Only extract if files do not exist
sparse_coo, row_categ, col_categ, table_type, _data_datestr = \
    SparseMatrix.uncompress_zipped_data(
        zip_filename, local_path=local_path, overwrite=overwrite)

# Create
agg_sparse_mtx = SparseMatrix(
    sparse_coo, mtx_table_type, data_datestr, row_categ, col_categ,
    logger=logger)

# .................................
# Create a summary matrix for each dimension of sparse matrix and upload
# .................................
sp_sum_mtx = SummaryMatrix.init_from_sparse_matrix(agg_sparse_mtx, axis=0, logger=logger)
spsum_table_type = sp_sum_mtx.table_type
sp_sum_filename = sp_sum_mtx.compress_to_file()

ds_sum_mtx = SummaryMatrix.init_from_sparse_matrix(agg_sparse_mtx, axis=1, logger=logger)
dssum_table_type = ds_sum_mtx.table_type
ds_sum_filename = ds_sum_mtx.compress_to_file()

upload_to_s3(sp_sum_filename, PROJ_BUCKET, SUMMARY_FOLDER, REGION)
upload_to_s3(ds_sum_filename, PROJ_BUCKET, SUMMARY_FOLDER, REGION)

# .................................
# Download data and recreate 2 summary matrices to test for corruption
# .................................
# Species Summary
sp_table = Summaries.get_table(spsum_table_type, data_datestr)
sp_zip_fname = f"{sp_table['fname']}.zip"
sp_zip_filename = download_from_s3(
    PROJ_BUCKET, SUMMARY_FOLDER, sp_zip_fname, local_path=local_path,
    overwrite=overwrite)

sp_dataframe, sp_meta_dict, sp_table_type, data_datestr = \
    SummaryMatrix.uncompress_zipped_data(
        sp_zip_filename, local_path=local_path, overwrite=overwrite)

# Dataset Summary
ds_table = Summaries.get_table(dssum_table_type, data_datestr)
ds_zip_fname = f"{ds_table['fname']}.zip"
ds_zip_filename = download_from_s3(
    PROJ_BUCKET, SUMMARY_FOLDER, ds_zip_fname, local_path=local_path,
    overwrite=overwrite)

ds_dataframe, ds_meta_dict, ds_table_type, data_datestr = \
    SummaryMatrix.uncompress_zipped_data(
        ds_zip_filename, local_path=local_path, overwrite=overwrite)
"""
