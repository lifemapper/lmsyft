"""Class for the Specify Network Name API service."""
import boto3
from http import HTTPStatus

from flask_app.common.s2n_type import APIService, AnalystOutput
from flask_app.common.util import print_analyst_output
from flask_app.analyst.base import _AnalystService

from sppy.aws.aws_tools import query_s3_table
from sppy.tools.s2n.utils import get_traceback


# .............................................................................
class CountSvc(_AnalystService):
    """Specify Network API service for retrieving taxonomic information."""
    SERVICE_TYPE = APIService.Count
    ORDERED_FIELDNAMES = []

    # ...............................................
    @classmethod
    def get_counts(cls, dataset_key):
        """Get counts for datasetKey.

        Args:
            dataset_key: Unique identifier for GBIF datasets.

        Returns:
            a flask_app.broker.s2n_type.BrokerOutput object with optional records as a
            list of dictionaries of records corresponding to specimen occurrences in
            the provider database.

        Todo: Consider adding publishing organization queries with pub_org_key
        """
        try:
            output = cls._get_records(dataset_key, )
        except Exception:
            traceback = get_traceback()
            output = AnalystOutput(
                cls.SERVICE_TYPE["name"],
                description=cls.SERVICE_TYPE["description"],
                errors={"error": [HTTPStatus.INTERNAL_SERVER_ERROR, traceback]})
        return output.response

    # ...............................................
    @classmethod
    def _get_organization_counts(cls, pub_org_key):
        return {
            "Organization Raw Counts":
                {
                    pub_org_key: 1,
                    "org_id_2": 2,
                    "org_id_3": 3
                },
            f"{pub_org_key} to other orgs":
                {
                    "to total": "0.5",
                    "org_id_2": "1.2",
                    "org_id_3": "1.2"
                }
        }

    # ...............................................
    @classmethod
    def _get_dataset_counts(cls, dataset_key):
        s3 = boto3.client('s3')

        resp = s3.select_object_content(
            Bucket=PROJ_,
            Key='sample_data.csv',
            ExpressionType='SQL',
            Expression="SELECT * FROM s3object s where s.\"Name\" = 'Jane'",
            InputSerialization={'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'},
            OutputSerialization={'CSV': {}},
        )

        for event in resp['Payload']:
            if 'Records' in event:
                records = event['Records']['Payload'].decode('utf-8')
                print(records)
            elif 'Stats' in event:
                statsDetails = event['Stats']['Details']
                print("Stats details bytesScanned: ")
                print(statsDetails['BytesScanned'])
                print("Stats details bytesProcessed: ")
                print(statsDetails['BytesProcessed'])
                print("Stats details bytesReturned: ")
                print(statsDetails['BytesReturned'])

    # ...............................................
    @classmethod
    def _get_records(cls, dataset_key, pub_org_key):
        allrecs = []
        # for response metadata
        if dataset_key is not None:
            coll_data = cls._get_collection_counts(dataset_key)
            allrecs.append(coll_data)
        if pub_org_key is not None:
            org_data = cls._get_organization_counts(pub_org_key)
            allrecs.append(org_data)

        # Assemble
        full_out = AnalystOutput(
            cls.SERVICE_TYPE["name"], description=cls.SERVICE_TYPE["description"],
            records=allrecs, errors={})

        return full_out


# .............................................................................
if __name__ == "__main__":
    # from flask_app.broker.constants import import TST_VALUES
    # occids = TST_VALUES.GUIDS_WO_SPECIFY_ACCESS[0:3]
    occids = ["84fe1494-c378-4657-be15-8c812b228bf4",
              "04c05e26-4876-4114-9e1d-984f78e89c15",
              "2facc7a2-dd88-44af-b95a-733cc27527d4"]
    occids = ["01493b05-4310-4f28-9d81-ad20860311f3",
              "01559f57-62ca-45ba-80b1-d2aafdc46f44",
              "015f35b8-655a-4720-9b88-c1c09f6562cb",
              "016613ba-4e65-44d5-94d1-e24605afc7e1",
              "0170cead-c9cd-48ba-9819-6c5d2e59947e",
              "01792c67-910f-4ad6-8912-9b1341cbd983",
              "017ea8f2-fc5a-4660-92ec-c203daaaa631",
              "018728bb-c376-4562-9ccb-8e3c3fd70df6",
              "018a34a9-55da-4503-8aee-e728ba4be146",
              "019b547a-79c7-47b3-a5ae-f11d30c2b0de"]
    # This occ has 16 issues in IDB, 0 in GBIF
    occids = ["2facc7a2-dd88-44af-b95a-733cc27527d4",
              "2c1becd5-e641-4e83-b3f5-76a55206539a"]
    occids = ["bffe655b-ea32-4838-8e80-a80e391d5b11"]
    occids = ["db193603-1ed3-11e3-bfac-90b11c41863e"]

    svc = CountSvc()
    out = svc.get_endpoint()
    print_analyst_output(out, do_print_rec=True)

    coll_id = "a7156437-55ec-4c6f-89de-938f9361753d"
    org_id = None
    out = svc.get_counts(coll_id, org_id)
    print_analyst_output(out, do_print_rec=True)

    # for occid in occids:
    #     out = svc.get_occurrence_records(occid=occid, provider=None, count_only=False)
    #     outputs = out["records"]
    #     print_broker_output(out, do_print_rec=True)
