"""Class to query tabular summary Specify Network data in S3"""
import boto3
import json
import pandas as pd

from sppy.aws.aws_constants import ENCODING, PROJ_BUCKET, REGION, SUMMARY_FOLDER
from sppy.aws.aws_tools import get_current_datadate_str
from sppy.tools.s2n.utils import get_traceback



# .............................................................................
class SpNetAnalyses():
    """Class for retrieving SpecifyNetwork summary data from AWS S3."""

    # ...............................................
    @classmethod
    def __init__(
            self, bucket, region=REGION, encoding=ENCODING):
        """Object to query tabular data in S3.

        Args:
             bucket: S3 bucket containing data.
             region: AWS region containing the data.
             encoding: encoding of the data.
        """
        self.bucket = bucket
        self.region = region
        self.encoding = encoding
        self.exp_type = 'SQL'
        self.datestr = get_current_datadate_str()
        self.datestr = "2024_02_01"
        self._summary_path = "summary"
        self._summary_tables = {
            "dataset_counts": {
                "fname": f"dataset_counts_{self.datestr}_000.parquet",
                "fields": ["datasetkey", "occ_count", "species_count"],
                "key": "datasetkey"
            },
            "dataset_species_lists": {
                "fname": f"dataset_lists_{self.datestr}_000.parquet",
                "fields": ["datasetkey", "taxonkey", "species", "occ_count"],
                "key": "datasetkey"
            },
            "dataset_meta": {
                "fname": f"dataset_meta_{self.datestr}.csv",
                "fields": [
                    "datasetKey", "publishingOrganizationKey", "title", "citation"],
                "key": "datasetKey"
            },
            "organization_meta": {
                "fname": f"organization_meta_{self.datestr}.csv",
                "fields": ["publishingOrganizationKey", "title"],
                "key": "publishingOrganizationKey"
            }
        }

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
    def _query_table(self, s3_path, query_str, format="CSV"):
        """Query the S3 resource defined for this class.

        Args:
            s3_path: S3 folder and filename within the bucket
            query_str: a SQL query for S3 select.
            format: output format, options "CSV" or "JSON"

        Returns:
             list of records matching the query
        """
        recs = []
        if format not in ("JSON", "CSV"):
            format = "JSON"
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
        resp = s3.select_object_content(
            Bucket=self.bucket,
            Key=s3_path,
            ExpressionType="SQL",
            Expression=query_str,
            InputSerialization={"Parquet": {}},
            OutputSerialization=out_serialization
        )
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
    def _query_order_s3_table(
            self, s3_path, sort_field, order, limit, format="CSV"):
        """Query the S3 resource defined for this class.

        Args:
            s3_path: S3 folder and filename within the bucket
            sort_field: fieldname (column) to sort records on
            order: boolean flag indicating to sort ascending or descending
            limit: number of records to return, limit is 500
            format: output format, options "CSV" or "JSON"

        Returns:
             ordered list of records matching the query
        """
        recs = []
        errors = {}
        df = self._create_dataframe_from_s3obj(s3_path)
        # Sort rows (Axis 0/index) by values in sort_field (column)
        sorted_df = df.sort_values(
            by=sort_field, axis=0, ascending=(order == "ascending"))
        rec_df = sorted_df.head(limit)

        for row in rec_df.itertuples():
            rec = {"datasetkey": row.datasetkey,
                   "species_count": row.species_count,
                   "occ_count": row.occ_count}
            recs.append(rec)
        return recs, errors

    # ----------------------------------------------------
    def get_dataset_counts(self, dataset_key, format="JSON"):
        """Query the S3 resource for occurrence and species counts for this dataset.

        Args:
            dataset_key: unique GBIF identifier for dataset of interest.
            format: output format, options "CSV" or "JSON"

        Returns:
             records: empty list or list of 1 record (list)
        """
        fields = self._summary_tables["dataset_counts"]["fields"]
        key_idx = fields.index(self._summary_tables["dataset_counts"]["key"])

        table_path = \
            f"{self._summary_path}/{self._summary_tables['dataset_counts']['fname']}"
        query_str = (
            f"SELECT * FROM s3object s WHERE s.datasetkey = '{dataset_key}'"
        )
        # Returns empty list or list of 1 record
        records = self._query_table(table_path, query_str, format=format)
        if self._dataset_metadata_exists():
            self.add_dataset_lookup_vals(records, key_idx=key_idx)
        return records

    # ----------------------------------------------------
    def add_dataset_lookup_vals(self, records, key_idx=0, format="JSON"):
        """Query the S3 resource for occurrence and species counts for this dataset.

        Args:
            key: unique GBIF identifier for object of interest.
            format: output format, options "CSV" or "JSON"

        Returns:
             records: empty list or list of 1 record (list)
        """
        table_path = \
            f"{self._summary_path}/{self._summary_tables['dataset_meta']['fname']}"
        fields = self._summary_tables["dataset_meta"]["fields"]
        key_fld = fields[0]
        new_flds = fields[1:]
        qry_flds = " ".join(new_flds)

        for rec in records:
            query_str = (
                f"SELECT {qry_flds} FROM s3object s WHERE s.{key_fld} = "
                f"'{rec[key_idx]}'"
            )
            # Returns empty list or list of 1 record
            meta_recs = self._query_table(table_path, query_str, format=format)
            try:
                meta = meta_recs[0]
            except IndexError:
                if format == "CSV":
                    # Add placeholders for empty values
                    rec.extend(["" for f in new_flds])
            else:
                for fld in new_flds:
                    if format == "JSON":
                        rec.update(meta)
                    else:
                        rec.extend(meta)

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
        table_path = \
            f"{self._summary_path}/{self._summary_tables['dataset_counts']['fname']}"
        fields = self._summary_tables["dataset_counts"]["fields"]
        key_idx = fields.index(self._summary_tables["dataset_counts"]["key"])
        if count_by == "species":
            sort_field = "species_count"
        else:
            sort_field = "occ_count"
        try:
            records, errors = self._query_order_s3_table(
                table_path, sort_field, order, limit)
        except Exception as e:
            errors = {"error": [get_traceback()]}

        if self._dataset_metadata_exists():
            self.add_dataset_lookup_vals(records, key_idx=key_idx)
        return records, errors

# .............................................................................
if __name__ == "__main__":
    format = "JSON"
    dataset_key = "0000e36f-d0e9-46b0-aa23-cc1980f00515"
    s3q = SpNetAnalyses(PROJ_BUCKET)
    recs = s3q.get_dataset_counts(dataset_key, format=format)
    for r in recs:
        print(r)

