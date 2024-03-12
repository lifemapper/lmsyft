"""Class for the Specify Network Occurrence API service."""
from werkzeug.exceptions import (BadRequest, InternalServerError)

from flask_app.broker.base import _BrokerService
from flask_app.common.s2n_type import (
    APIEndpoint, APIService, BrokerOutput, BrokerSchema, S2nKey, ServiceProvider)
from flask_app.common.util import print_broker_output

from sppy.tools.provider.gbif import GbifAPI
from sppy.tools.provider.idigbio import IdigbioAPI
from sppy.tools.provider.mopho import MorphoSourceAPI
from sppy.tools.s2n.utils import get_traceback


class OccurrenceSvc(_BrokerService):
    """Specify Network API service for retrieving occurrence record information."""
    SERVICE_TYPE = APIService.Occurrence
    ORDERED_FIELDNAMES = BrokerSchema.get_s2n_fields(APIEndpoint.Occurrence)

    # ...............................................
    @classmethod
    def get_providers(cls, filter_params=None):
        """Get a list of provider values valid for this service.

        Args:
            filter_params (dict): dictionary of URL parameter keys provided by the user.

        Returns:
            provider parameter values acceptable to the Specify Network service.

        Note:
            Overrides _BrokerService.get_providers
        """
        provnames = set()
        if filter_params is None:
            for p in ServiceProvider.all():
                if cls.SERVICE_TYPE["name"] in p[S2nKey.SERVICES]:
                    provnames.add(p[S2nKey.PARAM])
        # Fewer providers by dataset
        elif "gbif_dataset_key" in filter_params.keys():
            provnames = {ServiceProvider.GBIF[S2nKey.PARAM]}
        return provnames

    # ...............................................
    @classmethod
    def _get_mopho_records(cls, occid, count_only):
        output = MorphoSourceAPI.get_occurrences_by_occid_page1(
            occid, count_only=count_only)
        output.set_value(
            S2nKey.RECORD_FORMAT, cls.SERVICE_TYPE[S2nKey.RECORD_FORMAT])
        output.format_records(cls.ORDERED_FIELDNAMES)
        return output.response

    # ...............................................
    @classmethod
    def _get_idb_records(cls, occid, count_only):
        output = IdigbioAPI.get_occurrences_by_occid(
            occid, count_only=count_only)
        output.set_value(
            S2nKey.RECORD_FORMAT, cls.SERVICE_TYPE[S2nKey.RECORD_FORMAT])
        output.format_records(cls.ORDERED_FIELDNAMES)
        return output.response

    # ...............................................
    @classmethod
    def _get_gbif_records(cls, occid, gbif_dataset_key, count_only):
        if not (occid is None and gbif_dataset_key is None):
            if occid is not None:
                output = GbifAPI.get_occurrences_by_occid(occid, count_only=count_only)
            else:
                output = GbifAPI.get_occurrences_by_dataset(
                    gbif_dataset_key, count_only)
            output.set_value(
                S2nKey.RECORD_FORMAT, cls.SERVICE_TYPE[S2nKey.RECORD_FORMAT])
            output.format_records(cls.ORDERED_FIELDNAMES)

        else:
            output = cls._get_badquery_output(
                "Must provide occurrence_id or gbif_dataset_key")
        return output.response

    # ...............................................
    @classmethod
    def _get_records(cls, occid, req_providers, count_only, gbif_dataset_key=None):
        allrecs = []
        # for response metadata
        query_term = None
        provstr = ",".join(req_providers)
        if occid is not None:
            query_term = f"occid={occid}&provider={provstr}&count_only={count_only}"
        elif gbif_dataset_key:
            query_term = \
                f"gbif_dataset_key={gbif_dataset_key}&provider={provstr}" \
                f"&count_only={count_only}"

        for pr in req_providers:
            # Address single record
            if occid is not None:
                # GBIF
                if pr == ServiceProvider.GBIF[S2nKey.PARAM]:
                    gbif_output = cls._get_gbif_records(
                        occid, gbif_dataset_key, count_only)
                    allrecs.append(gbif_output)
                # iDigBio
                elif pr == ServiceProvider.iDigBio[S2nKey.PARAM]:
                    idb_output = cls._get_idb_records(occid, count_only)
                    allrecs.append(idb_output)
                # MorphoSource
                elif pr == ServiceProvider.MorphoSource[S2nKey.PARAM]:
                    mopho_output = cls._get_mopho_records(occid, count_only)
                    allrecs.append(mopho_output)
                # Specify
                # elif pr == ServiceProvider.Specify[S2nKey.PARAM]:
                #     sp_output = cls._get_specify_records(occid, count_only)
                #     allrecs.append(sp_output)
            # Filter by parameters
            elif gbif_dataset_key:
                if pr == ServiceProvider.GBIF[S2nKey.PARAM]:
                    gbif_output = cls._get_gbif_records(
                        occid, gbif_dataset_key, count_only)
                    allrecs.append(gbif_output)

        prov_meta = cls._get_s2n_provider_response_elt(query_term=query_term)
        # Assemble
        # TODO: Why are errors retained from query to query!!!  Resetting to {} works.
        full_out = BrokerOutput(
            len(allrecs), cls.SERVICE_TYPE["name"], provider=prov_meta,
            records=allrecs, errors={})
        return full_out

    # ...............................................
    @classmethod
    def get_occurrence_records(
            cls, occid=None, provider=None, gbif_dataset_key=None, count_only=False,
            **kwargs):
        """Get one or more occurrence records from each occurrence provider.

        Args:
            occid: an occurrenceID, a DarwinCore field intended for a globally
                unique identifier (https://dwc.tdwg.org/list/#dwc_occurrenceID)
            provider: comma-delimited list of providers to query
            gbif_dataset_key: GBIF datasetKey for records to return from GBIF.
            count_only: flag to indicate whether to return only a count, or
                a count and records
            kwargs: any additional keyword arguments are ignored

        Raises:
            BadRequest: on invalid query parameters.
            BadRequest: on unknown exception parsing parameters.
            InternalServerError: on unknown exception when executing request

        Returns:
            a flask_app.broker.s2n_type.BrokerOutput object with optional records as a
            list of dictionaries of records corresponding to specimen occurrences in
            the provider database.
        """
        if occid is None and gbif_dataset_key is None:
            return cls.get_endpoint()
        else:
            try:
                good_params, errinfo = cls._standardize_params(
                    occid=occid, provider=provider, gbif_dataset_key=gbif_dataset_key,
                    count_only=count_only)
                # errinfo indicates bad parameters
                try:
                    error_description = "; ".join(errinfo["error"])
                    raise BadRequest(error_description)
                except KeyError:
                    pass

            except Exception:
                error_description = get_traceback()
                raise BadRequest(error_description)

            # Do Query!
            try:
                output = cls._get_records(
                    good_params["occid"], good_params["provider"],
                    good_params["count_only"],
                    gbif_dataset_key=good_params["gbif_dataset_key"])

                # Add message on invalid parameters to output
                try:
                    for err in errinfo["warning"]:
                        output.append_error("warning", err)
                except KeyError:
                    pass

            except Exception:
                error_description = get_traceback()
                raise InternalServerError(error_description)

        return output.response


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

    svc = OccurrenceSvc()
    out = svc.get_endpoint()
    out = svc.get_occurrence_records(occid="a7156437-55ec-4c6f-89de-938f9361753d")

    print_broker_output(out, do_print_rec=True)

    # for occid in occids:
    #     out = svc.get_occurrence_records(occid=occid, provider=None, count_only=False)
    #     outputs = out["records"]
    #     print_broker_output(out, do_print_rec=True)
