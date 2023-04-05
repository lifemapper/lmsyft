"""Solr backend controller module for Resolver."""
import pysolr

from flask_app.resolver.config import RESOLVER_URL


# Note: A possible evolution of this would be to integrate the Ark class with PySolr
#    encoding and decoding so that it could work with the class directly rather than
#    having a translation.  This pattern would be useful for all solr controllers


# .....................................................................................
def get_resolver_solr():
    """Get solr connection to resolver core.

    Returns:
        pysolr.Solr: A Solr connection to the collections Solr collection.

    Todo:
        Incorporate this into flask better.
    """
    return pysolr.Solr(RESOLVER_URL)


# .....................................................................................
def get_endpoint_metadata():
    """Get Solr endpoint metadata.

    Returns:
        dict: Metadata about the endpoint.
    """
    resolver_solr = get_resolver_solr()
    return {
        'count': resolver_solr.search('*').hits
    }


# .....................................................................................
def get_identifier(identifier):
    """Get information about an identifier from the Solr Resolver collection.

    Args:
        identifier (str): An identifier to attempt to retrieve.

    Returns:
        Ark: An Ark object from the Solr idex.
    """
    resolver_solr = get_resolver_solr()
    response = resolver_solr.search(identifier)
    if response.hits > 0:
        return response.docs[0]
    return None


# .....................................................................................
def post_identifiers(identifier_records):
    """Add new or updated records to the Solr Resolver collection.

    Args:
        identifier_records (list of Ark): A list of Ark objects to add to the index.
    """
    resolver_solr = get_resolver_solr()
    resolver_solr.add(
        [identifier.serialize_json() for identifier in identifier_records], commit=True
    )
