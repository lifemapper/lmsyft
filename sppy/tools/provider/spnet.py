"""Class to query tabular summary Specify Network data in S3."""
import boto3
import json
import pandas as pd

from sppy.aws.aws_constants import (
    ENCODING, PROJ_BUCKET, REGION, SUMMARY_FOLDER, Summaries)
from sppy.aws.aws_tools import get_current_datadate_str
from sppy.tools.s2n.utils import get_traceback


# .............................................................................
class SpNetAnalyses():
    """Class for retrieving SpecifyNetwork summary data from AWS S3."""

    # ...............................................
    @classmethod
    def __init__(
            self, bucket, s3_summary_path=SUMMARY_FOLDER, region=REGION,
            encoding=ENCODING):
        """Object to query tabular data in S3.

        Args:
             bucket: S3 bucket containing data.
             s3_summary_path: path within the bucket for summary data.
             region: AWS region containing the data.
             encoding: encoding of the data.
        """
        self.bucket = bucket
        self.region = region
        self.encoding = encoding
        self.exp_type = 'SQL'
        self.datestr = get_current_datadate_str()
        self._summary_path = s3_summary_path
        # Data objects for query
        self._summary_tables = Summaries.update_summary_tables(self.datestr)

    # ----------------------------------------------------
    def _list_summaries(self):
        summary_objs = []
        s3 = boto3.client("s3", region_name=self.region)
        summ_objs = s3.list_objects_v2(Bucket=self.bucket, Prefix=self._summary_path)
        prefix = f"{self._summary_path}/"
        try:
            contents = summ_objs["Contents"]
        except KeyError:
            pass
        else:
            for item in contents:
                fname = item["Key"].strip(prefix)
                if len(fname) > 1:
                    summary_objs.append(fname)
        return summary_objs

    # ----------------------------------------------------
    def _dataset_metadata_exists(self):
        fnames = self._list_summaries()
        if self._summary_tables["dataset_meta"]["fname"] in fnames:
            return True
        return False

    # ----------------------------------------------------
    def _query_summary_table(self, table, query_str, format):
        """Query the S3 resource defined for this class.

        Args:
            table: summary object within the bucket and summary folder
            query_str: a SQL query for S3 select.
            format: output format, options "CSV" or "JSON"

        Returns:
             list of records matching the query

        Raises:
            Exception: on unsupported output format
            Exception: on failed select_object_content
        """
        if format not in ("JSON", "CSV"):
            raise(Exception(f"Unsupported output format {format}"))

        recs = []
        # Table properties
        tbl_format = table["table_format"]
        s3_path = f"{self._summary_path}/{table['fname']}"

        # Input S3 serialization
        if tbl_format == "CSV":
            in_serialization = {"CSV": {"FileHeaderInfo": "Use"}}
        else:
            in_serialization = {"Parquet": {}}

        # Output serialization
        if format == "JSON":
            out_serialization = {"JSON": {}}
        elif format == "CSV":
            out_serialization = {
                "CSV": {
                    "QuoteFields": "ASNEEDED",
                    "FieldDelimiter": ",",
                    "QuoteCharacter": '"'}
            }
        s3 = boto3.client("s3", region_name=self.region)
        try:
            resp = s3.select_object_content(
                Bucket=self.bucket,
                Key=s3_path,
                ExpressionType="SQL",
                Expression=query_str,
                InputSerialization=in_serialization,
                OutputSerialization=out_serialization
            )
        except Exception as e:
            print(e)
            raise
        for event in resp["Payload"]:
            if "Records" in event:
                recs_str = event["Records"]["Payload"].decode(ENCODING)
                rec_strings = recs_str.strip().split("\n")
                for rs in rec_strings:
                    if format == "JSON":
                        rec = json.loads(rs)
                    else:
                        rec = rs.split(",")
                    recs.append(rec)
        return recs

    # ----------------------------------------------------
    def _create_dataframe_from_s3obj(self, s3_path):
        """Read CSV data from S3 into a pandas DataFrame.

        Args:
            s3_path: the object name with enclosing S3 bucket folders.

        Returns:
            df: pandas DataFrame containing the CSV data.
        """
        # import pyarrow.parquet as pq
        s3_uri = f"s3://{self.bucket}/{s3_path}"
        df = pd.read_parquet(s3_uri)
        return df

    # ----------------------------------------------------
    def _query_order_summary_table(
            self, table, sort_field, order, limit, format):
        """Query the S3 resource defined for this class.

        Args:
            table: summary object within the bucket and summary folder
            sort_field: fieldname (column) to sort records on
            order: boolean flag indicating to sort ascending or descending
            limit: number of records to return, limit is 500
            format: output format, options "CSV" or "JSON"

        Returns:
             ordered list of records matching the query
        """
        s3_path = f"{self._summary_path}/{table['fname']}"
        df = self._create_dataframe_from_s3obj(s3_path)
        # Sort rows (Axis 0/index) by values in sort_field (column)
        sorted_df = df.sort_values(
            by=sort_field, axis=0, ascending=(order == "ascending"))
        recs_df = sorted_df.head(limit)
        if format == "JSON":
            recs = []
            tmpdict = recs_df.to_dict(orient='index')
            for _idx, rec in tmpdict.items():
                recs.append(rec)
        else:
            # Add column headings as first record in list
            recs = [list(recs_df.columns)]
            for rec in recs_df.values:
                recs.append(rec)
        return recs

# ----------------------------------------------------
    def get_simple_dataset_counts(self, dataset_key, format="JSON"):
        """Query the S3 resource for occurrence and species counts for this dataset.

        Args:
            dataset_key: unique GBIF identifier for dataset of interest.
            format: output format, options "CSV" or "JSON"

        Returns:
             records: empty list or list of 1 record (list)
        """
        table = self._summary_tables["dataset_counts"]
        query_str = (
            f"SELECT * FROM s3object s WHERE s.{table['key']} = '{dataset_key}'"
        )
        # Returns empty list or list of 1 record
        records = self._query_summary_table(table, query_str, format)
        if self._dataset_metadata_exists():
            self._add_dataset_lookup_vals(records, table, format)
        return records

    # ----------------------------------------------------
    def _add_dataset_lookup_vals(self, records, rec_table, format):
        """Add dataset metadata to records.

        Args:
            records: records to add dataset metadata to.
            rec_table: dictionary of fieldnames, filename, format for a summary table
            format: output format, options "CSV" or "JSON"
        """
        # Metadata table info
        meta_table = self._summary_tables["dataset_meta"]
        meta_fields = meta_table["fields"]
        meta_key_fld = meta_table["key_fld"]
        meta_key_idx = meta_fields.index(meta_key_fld)
        meta_fields.pop(meta_key_idx)
        qry_flds = ", ".join(meta_fields)

        # Record info
        rec_fields = rec_table["fields"]
        rec_key_fld = rec_table["key_fld"]
        rec_key_idx = rec_fields.index(rec_key_fld)

        for rec in records:
            # Get datasetkey by field or position
            if format == "JSON":
                val = rec[rec_key_fld]
            else:
                val = rec[rec_key_idx]

            query_str = (
                f"SELECT {qry_flds} FROM s3object s WHERE s.{meta_key_fld} = '{val}'"
            )
            # Returns empty list or list of 1 record
            meta_recs = self._query_summary_table(meta_table, query_str, format)
            try:
                meta = meta_recs[0]
            except IndexError:
                # Add placeholders for empty values, no entries for dictionary
                if format == "CSV":
                    rec.extend(["" for _ in qry_flds])
            else:
                if format == "JSON":
                    rec.update(meta)
                else:
                    rec.extend(meta)
        print(records)

    # # ----------------------------------------------------
    # def get_org_counts(self, pub_org_key):
    #     """Query S3 for occurrence and species counts for this organization.
    #
    #     Args:
    #         pub_org_key: unique GBIF identifier for organization of interest.
    #
    #     Returns:
    #          records: empty list or list of 1 record containing occ_count, species_count
    #
    #     TODO: implement this?
    #     """
    #     (occ_count, species_count) = (0,0)
    #     return (occ_count, species_count)

    # ----------------------------------------------------
    def rank_dataset_counts(self, count_by, order, limit, format="JSON"):
        """Return the top or bottom datasets, with counts, ranked by number of species.

        Args:
            count_by: string indicating rank datasets by counts of "species" or
                "occurrence" .
            order: string indicating whether to rank in "descending" or
                "ascending" order.
            limit: number of datasets to return, no more than 300.
            format: output format, options "CSV" or "JSON"

        Returns:
             records: list of limit records containing dataset_key, occ_count, species_count
        """
        records = []
        errors = {}
        table = self._summary_tables['dataset_counts']

        if count_by == "species":
            sort_field = "species_count"
        else:
            sort_field = "occ_count"
        try:
            records = self._query_order_summary_table(
                table, sort_field, order, limit, format)
        except Exception:
            errors = {"error": [get_traceback()]}
        # Add dataset title, etc if the lookup table exists in S3
        if self._dataset_metadata_exists():
            self._add_dataset_lookup_vals(records, table, format)

        return records, errors

    # # ----------------------------------------------------
    # def rank_species_counts(self, order, limit, format="JSON"):
    #     """Rank the occurrences of a species in a single dataset, most or least.
    #
    #     Args:
    #         order: string indicating whether to rank in "descending" or
    #             "ascending" order.
    #         limit: number of species occurrence counts to return, no more than 300.
    #         format: output format, options "CSV" or "JSON"
    #
    #     Returns:
    #          records: list of limit records containing dataset_key, occ_count, species_count
    #     """
    #     records = []
    #     errors = {}
    #     table = self._summary_tables["dataset_lists"]
    #     sort_fields = ["occ_count", "datasetkey"]
    #     try:
    #         records = self._query_order_summary_table(
    #             table, sort_fields, order, limit, format)
    #     except Exception:
    #         errors = {"error": [get_traceback()]}
    #
    #     if self._dataset_metadata_exists():
    #         self._add_dataset_lookup_vals(records, format)
    #     return records, errors


# .............................................................................
if __name__ == "__main__":
    format = "JSON"
    dataset_key = "0000e36f-d0e9-46b0-aa23-cc1980f00515"
    s3q = SpNetAnalyses(PROJ_BUCKET)
    recs = s3q.get_dataset_counts(dataset_key, format=format)
    for r in recs:
        print(r)
