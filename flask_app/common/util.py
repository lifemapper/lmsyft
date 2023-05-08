"""Utilities for repeated tasks."""
import os

# .....................................................................................
def get_host_url():
    protocol = "https://"
    host_url = os.getenv("FQDN")
    # default
    if host_url is None:
        host_url = f"{protocol}localhost"
    # Use SSL
    if not host_url.startswith(protocol):
        host_url = f"{protocol}{host_url}"
    # Remove trailing slash
    if host_url.endswith("/"):
        host_url = host_url[:-1]
    return host_url


# .............................................................................
def _print_sub_output(oneelt, do_print_rec):
    print("* One record of Specify Network Outputs *")
    for name, attelt in oneelt.items():
        try:
            if name == "records":
                print("   records")
                if do_print_rec is False:
                    print(f"      {name}: {len(attelt)} returned records")
                else:
                    for rec in attelt:
                        print("      record")
                        for k, v in rec.items():
                            print("         {}: {}".format(k, v))
            else:
                print("   {}: {}".format(name, attelt))
        except Exception:
            pass


# ....................................
def print_broker_output(response_dict, do_print_rec=False):
    """Print a formatted string of the elements in an S2nOutput query response.

    Args:
        response_dict: flask_app.broker.s2n_type.S2nOutput object
        do_print_rec: True to print each record in the response.

    TODO: move to a class method
    """
    print("*** Broker output ***")
    for name, attelt in response_dict.items():
        try:
            if name == "records":
                print(f"{name}: ")
                for respdict in attelt:
                    _print_sub_output(respdict, do_print_rec)
            else:
                print(f"{name}: {attelt}")
        except Exception:
            pass
    # outelts = set(response_dict.keys())
    # missing = S2nKey.broker_response_keys().difference(outelts)
    # extras = outelts.difference(S2nKey.broker_response_keys())
    # if missing:
    #     print(f"Missing elements: {missing}")
    # if extras:
    #     print(f"Extra elements: {extras}")
    print("")


# ....................................
def print_analyst_output(response_dict, do_print_rec=False):
    """Print a formatted string of the elements in an S2nOutput query response.

    Args:
        response_dict: flask_app.broker.s2n_type.S2nOutput object
        do_print_rec: True to print each record in the response.

    TODO: move to a class method
    """
    print("*** Analyst output ***")
    for name, attelt in response_dict.items():
        try:
            if name == "records":
                print(f"{name}: ")
                for respdict in attelt:
                    _print_sub_output(respdict, do_print_rec)
            else:
                print(f"{name}: {attelt}")
        except Exception:
            pass
