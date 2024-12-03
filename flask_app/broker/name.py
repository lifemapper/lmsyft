"""Class for the Specify Network Name API service."""
from werkzeug.exceptions import BadRequest

from flask_app.broker.base import _BrokerService
from flask_app.common.s2n_type import (
    APIEndpoint, APIService, BrokerOutput, BrokerSchema, S2nKey, ServiceProvider)

from sppy.provider.itis import ItisAPI
from sppy.provider.gbif import GbifAPI
from sppy.provider.worms import WormsAPI
from spnet.common.util import get_traceback


# .............................................................................
class NameSvc(_BrokerService):
    """Specify Network API service for retrieving taxonomic information."""
    SERVICE_TYPE = APIService.Name
    ORDERED_FIELDNAMES = BrokerSchema.get_s2n_fields(APIEndpoint.Name)

    # ...............................................
    @classmethod
    def _get_gbif_records(cls, namestr, is_accepted, gbif_count):
        output = GbifAPI.match_name(namestr, is_accepted=is_accepted)
        output.set_value(
            S2nKey.RECORD_FORMAT, cls.SERVICE_TYPE[S2nKey.RECORD_FORMAT])

        # Add occurrence count to name records
        if gbif_count is True:
            prov_query_list = output.provider_query
            keyfld = BrokerSchema.get_gbif_taxonkey_fld()
            cntfld = BrokerSchema.get_gbif_occcount_fld()
            urlfld = BrokerSchema.get_gbif_occurl_fld()
            for namerec in output.records:
                try:
                    taxon_key = namerec[keyfld]
                except Exception:
                    print(f"No usageKey for counting {namestr} records")
                else:
                    # Add more info to each record
                    try:
                        count_output = GbifAPI.count_occurrences_for_taxon(taxon_key)
                    except Exception:
                        traceback = get_traceback()
                        print(traceback)
                    else:
                        try:
                            count_query = count_output.provider[
                                S2nKey.PROVIDER_QUERY_URL][0]
                            namerec[cntfld] = count_output.count
                        except Exception:
                            traceback = get_traceback()
                            output.append_value(S2nKey.ERRORS, {"error": traceback})
                        else:
                            namerec[urlfld] = count_query
                            prov_query_list.append(count_query)
            # add count queries to list
            output.set_value(S2nKey.PROVIDER_QUERY_URL, prov_query_list)
            output.format_records(cls.ORDERED_FIELDNAMES)
        return output.response

    # ...............................................
    @classmethod
    def _get_itis_records(cls, namestr, is_accepted, kingdom):
        output = ItisAPI.match_name(
            namestr, is_accepted=is_accepted, kingdom=kingdom)
        output.set_value(
            S2nKey.RECORD_FORMAT, cls.SERVICE_TYPE[S2nKey.RECORD_FORMAT])
        output.format_records(cls.ORDERED_FIELDNAMES)
        return output.response

    # ...............................................
    @classmethod
    def _get_worms_records(cls, namestr, is_accepted):
        output = WormsAPI.match_name(namestr, is_accepted=is_accepted)
        output.set_value(
            S2nKey.RECORD_FORMAT, cls.SERVICE_TYPE[S2nKey.RECORD_FORMAT])
        output.format_records(cls.ORDERED_FIELDNAMES)
        return output.response

    # ...............................................
    @classmethod
    def _get_records(
            cls, namestr, req_providers, is_accepted, gbif_count, kingdom):
        allrecs = []
        # for response metadata
        query_term = ""
        if namestr is not None:
            query_term = \
                f"namestr={namestr}&provider={','.join(req_providers)}&" \
                f"is_accepted={is_accepted}&gbif_count={gbif_count}&kingdom={kingdom}"

        for pr in req_providers:
            # Address single record
            if namestr is not None:
                # GBIF
                if pr == ServiceProvider.GBIF[S2nKey.PARAM]:
                    goutput = cls._get_gbif_records(
                        namestr, is_accepted, gbif_count)
                    allrecs.append(goutput)
                #  ITIS
                elif pr == ServiceProvider.ITISSolr[S2nKey.PARAM]:
                    isoutput = cls._get_itis_records(
                        namestr, is_accepted, kingdom)
                    allrecs.append(isoutput)
                #  WoRMS
                elif pr == ServiceProvider.WoRMS[S2nKey.PARAM]:
                    woutput = cls._get_worms_records(
                        namestr, is_accepted)
                    allrecs.append(woutput)
            # TODO: enable filter parameters

        # Assemble
        prov_meta = cls._get_s2n_provider_response_elt(query_term=query_term)
        full_out = BrokerOutput(
            len(allrecs), cls.SERVICE_TYPE["name"], provider=prov_meta,
            records=allrecs, errors={})

        return full_out

    # ...............................................
    @classmethod
    def get_name_records(
            cls, namestr=None, provider=None, is_accepted=True,
            gbif_parse=True, gbif_count=True, kingdom=None, **kwargs):
        """Get taxon records for a scientific name from each requested name service.

        Args:
            namestr: a scientific name
            provider: comma-delimited list of requested provider codes.  Codes are
                delimited for each in flask_app.broker.constants ServiceProvider
            is_accepted: flag to indicate whether to limit to "valid" or  "accepted"
                taxa in the ITIS or GBIF Backbone Taxonomy
            gbif_parse: flag to indicate whether to first use the GBIF parser to parse a
                scientific name into canonical name
            gbif_count: flag to indicate whether to count GBIF occurrences of this taxon
            kingdom: not yet implemented
            **kwargs: additional keyword arguments are accepted and ignored

        Returns:
            A flask_app.broker.s2n_type.BrokerOutput object containing records for each
            provider.  Each provider element is a BrokerOutput object with records as a
            list of dictionaries following the flask_app.broker.s2n_type.BrokerSchema.NAME
            corresponding to names in the provider taxonomy.
        """
        if namestr is None:
            return cls.get_endpoint()
        else:
            # No filter_params defined for Name service yet
            try:
                good_params, errinfo = cls._standardize_params(
                    namestr=namestr, provider=provider, is_accepted=is_accepted,
                    gbif_parse=gbif_parse, gbif_count=gbif_count, kingdom=kingdom)

            except BadRequest as e:
                full_output = cls._get_badquery_output(e.description)

            else:
                try:
                    # Do Query!, returns BrokerOutput
                    full_output = cls._get_records(
                        good_params["namestr"], good_params["provider"],
                        good_params["is_accepted"], good_params["gbif_count"],
                        good_params["kingdom"])
                except Exception:
                    full_output = cls._get_badquery_output(get_traceback())

                # Combine with errors from parameters
                full_output.combine_errors(errinfo)

        return full_output.response


# .............................................................................
if __name__ == "__main__":
    pass
    # test_names = TST_VALUES.NAMES[0:4]
    test_names = [
        # "Notemigonus crysoleucas (Mitchill, 1814)",
        # "Artemisia ludoviciana",
        # "Mycteroperca microlepis",
        # "Plagiloecia patina Lamarck, 1816",
        # "Poa annua",
        # "Gnatholepis cauerensis (Bleeker, 1853)",
        # "Tulipa sylvestris",
        "Acer nigrum Michx.f",
        "Notemigonus crysoleucas (Mitchill, 1814)",
        # "Acer leucoderme Small"
    ]

    svc = NameSvc()
    for namestr in test_names:
        response = svc.get_name_records(
            namestr=namestr, provider=None, is_accepted=False,
            gbif_parse=True, gbif_count=True, kingdom=None)
        BrokerOutput.print_output(response, do_print_rec=True)
