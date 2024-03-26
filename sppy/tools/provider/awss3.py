"""Class to query tabular summary Specify Network data in S3"""
import boto3
import json
import pandas as pd

from sppy.aws.aws_constants import ENCODING, PROJ_BUCKET, REGION, SUMMARY_FOLDER
from sppy.aws.aws_tools import get_current_datadate_str
from sppy.tools.s2n.utils import get_traceback



# .............................................................................
class S3Query():
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
        datestr = get_current_datadate_str()
        datestr = "2024_02_01"
        self._dataset_counts_path = f"{SUMMARY_FOLDER}/dataset_counts_{datestr}_000.parquet"
        self._dataset_lists_path = f"{SUMMARY_FOLDER}/dataset_lists_{datestr}_000.parquet"
        self._dataset_names_path = f"{SUMMARY_FOLDER}/dataset_names_{datestr}_000.csv"

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
        query_str = (
            "SELECT datasetkey, occ_count, species_count FROM s3object s "
            f"WHERE s.datasetkey = '{dataset_key}'"
        )
        # Returns empty list or list of 1 record
        records = self._query_table(self._dataset_counts_path, query_str, format=format)
        return records

    # ----------------------------------------------------
    def get_org_counts(self, pub_org_key):
        """Query S3 for occurrence and species counts for this organization.

        Args:
            pub_org_key: unique GBIF identifier for organization of interest.

        Returns:
             records: empty list or list of 1 record containing occ_count, species_count

        TODO: implement this?
        """
        (occ_count, species_count) = (0,0)
        return (occ_count, species_count)

    # ----------------------------------------------------
    def rank_datasets(self, count_by, order, limit, format="JSON"):
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
        if count_by == "species":
            sort_field = "species_count"
        else:
            sort_field = "occ_count"
        try:
            records, errors = self._query_order_s3_table(
                self._dataset_counts_path, sort_field, order, limit)
        except Exception as e:
            errors = {"error": [get_traceback()]}
        return records, errors

# .............................................................................
if __name__ == "__main__":
    format = "JSON"
    dataset_key = "0000e36f-d0e9-46b0-aa23-cc1980f00515"
    s3q = S3Query(PROJ_BUCKET)
    recs = s3q.get_dataset_counts(dataset_key, format=format)
    for r in recs:
        print(r)

