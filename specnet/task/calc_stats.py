"""Create a matrix of occurrence or species counts by (geospatial) analysis dimension."""
import os

from spanalyst.aws.constants import REGION, S3_BUCKET, S3_SUMMARY_DIR
from spanalyst.aws.tools import S3
from spanalyst.common.constants import TMP_PATH
from spanalyst.common.log import logit
from spanalyst.common.util import get_current_datadate_str
from spanalyst.matrix.heatmap_matrix import HeatmapMatrix
from spanalyst.matrix.pam_matrix import PAM
from spanalyst.matrix.summary_matrix import SummaryMatrix

from specnet.common.constants import ANALYSIS_DIM, SUMMARY

"""
Note:
    The analysis dimension should be geospatial, and fully cover the landscape with no
        overlaps.  Each species/occurrence count applies to one and only one record in
        the analysis dimension.
"""


# .............................................................................
def create_heatmap_from_records(
        s3, stacked_table_type, mtx_table_type, datestr, logger=None):
    """Read stacked records from S3, aggregate into a sparse matrix of species x dim.

    Args:
        s3 (bison.tools.aws_util.S3): authenticated boto3 client for S3 interactions.
        stacked_table_type (str): code from bison.common.constants.SUMMARY with
            predefined type of data to download, indicating type and contents.
        mtx_table_type (code from bison.common.constants.SUMMARY): predefined type
            of data to create from the stacked data.
        datestr (str): date of the current dataset, in YYYY_MM_DD format
        logger (bison.common.log.Logger): for writing messages to file and console

    Returns:
        heatmap (bison.spanalyst.heatmap_matrix.HeatmapMatrix): sparse matrix
            containing data separated into 2 dimensions
        y_fld (str): column header from stacked input records containing values for
            sparse matrix row headers
        x_fld (str): column header from stacked input records containing values for
            sparse matrix column headers
        val_fld (str): column header from stacked input records containing values
            for sparse matrix cells

    Raises:
        Exception: on failure to read records from parquet into a Dataframe.
        Exception: on failure to convert Dataframe to a sparse matrix.
    """
    _, dim0, dim1, _ = SUMMARY.parse_table_type(mtx_table_type)
    # Download stacked records from S3 into a dataframe
    try:
        y_fld, x_fld, val_fld, stk_df = \
            _read_stacked_data_records(s3, stacked_table_type, datestr)
    except Exception as e:
        logit(
            f"Failed to read {stacked_table_type} to dataframe. ({e})",
            logger=logger
        )
        raise
    logit(f"Read stacked data {stacked_table_type}.", logger=logger)

    # Create matrix from record data, then test consistency and upload.
    try:
        heatmap = HeatmapMatrix.init_from_stacked_data(
            stk_df, y_fld, x_fld, val_fld, mtx_table_type, datestr)
    except Exception as e:
        logger.log(f"Failed to read {stacked_table_type} to sparse matrix. ({e})")
        raise(e)
    logit(f"Built {mtx_table_type} from stacked data.", logger=logger)

    return stk_df, heatmap


# ...............................................
def _read_stacked_data_records(s3, stacked_data_table_type, datestr):
    """Read stacked records from S3, aggregate into a sparse matrix of species x dataset.

    Args:
        s3 (bison.aws_util.S3): client connection for reading/writing data to AWS S3.
        stacked_data_table_type (str): table type for parquet data containing records of
            species lists for another dimension (i.e. region) with occurrence and
            species counts.
        datestr (str): date of the current dataset, in YYYY_MM_DD format

    Returns:
        heatmap (specnet.tools.s2n.heatmap_matrix.HeatmapMatrix): sparse matrix
            containing data separated into 2 dimensions
    """
    # Species in columns/x/axis1
    stacked_record_table = SUMMARY.get_table(stacked_data_table_type, datestr)
    pqt_fname = SUMMARY.get_filename(stacked_data_table_type, datestr)
    # Read stacked (record) data directly into DataFrame
    stk_df = s3.get_dataframe_from_parquet(S3_BUCKET, S3_SUMMARY_DIR, pqt_fname)

    axis0_fld = stacked_record_table["key_fld"]
    axis1_fld = stacked_record_table["species_fld"]
    val_fld = stacked_record_table["value_fld"]

    return (axis0_fld, axis1_fld, val_fld, stk_df)


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    """Main script creates a SPECIES_DATASET_MATRIX from county/species list."""
    overwrite = True
    datestr = get_current_datadate_str()
    s3 = S3(region=REGION)
    logger = None

    dim_region = ANALYSIS_DIM.DATASET["code"]
    dim_species = ANALYSIS_DIM.SPECIES["code"]
    stacked_data_table_type = SUMMARY.get_table_type(
        "list", dim_species, dim_region)
    # Species are always columns (for PAM)
    mtx_table_type = SUMMARY.get_table_type("matrix", dim_region, dim_species)

    # .................................
    # Build heatmap, upload
    # .................................
    stack_df, heatmap = create_heatmap_from_records(
        s3, stacked_data_table_type, mtx_table_type, datestr)

    out_filename = heatmap.compress_to_file(local_path=TMP_PATH)
    s3_mtx_key = f"{S3_SUMMARY_DIR}/{os.path.basename(out_filename)}"
    s3.upload(out_filename, S3_BUCKET, s3_mtx_key, overwrite=overwrite)

    # .................................
    # Create a summary matrix for each dimension of sparse matrix and upload
    # .................................
    sp_sum_mtx = SummaryMatrix.init_from_heatmap(heatmap, axis=0)
    spsum_table_type = sp_sum_mtx.table_type
    sp_sum_filename = sp_sum_mtx.compress_to_file()
    s3_spsum_key = f"{S3_SUMMARY_DIR}/{os.path.basename(sp_sum_filename)}"
    s3.upload(sp_sum_filename, S3_BUCKET, s3_spsum_key, overwrite=overwrite)

    od_sum_mtx = SummaryMatrix.init_from_heatmap(heatmap, axis=1)
    odsum_table_type = od_sum_mtx.table_type
    od_sum_filename = od_sum_mtx.compress_to_file()
    s3_odsum_key = f"{S3_SUMMARY_DIR}/{os.path.basename(od_sum_filename)}"
    s3.upload(od_sum_filename, S3_BUCKET, s3_odsum_key, overwrite=overwrite)

    # .................................
    # Create PAM from Heatmap
    # .................................
    min_count = 3
    pam = PAM.init_from_heatmap(heatmap, min_count)

    pam.calc_species_stats()
    pam.calc_site_stats()
    pam.calc_diversity_stats()
    # pam.calc_covariance_stats()

    stats_zip_filename = pam.compress_stats_to_file(local_path=TMP_PATH)
    stats_data_dict, stats_meta_dict, table_type, datestr = PAM.uncompress_zipped_data(
        stats_zip_filename)
    stats_key = f"{S3_SUMMARY_DIR}/{os.path.basename(stats_zip_filename)}"
    s3.upload(stats_zip_filename, S3_BUCKET, stats_key, overwrite=overwrite)

"""
"""
