"""Solr backend controller module for Specify Cache."""
import pysolr

from flask_app.sp_cache.config import COLLECTIONS_URL, OCCURRENCES_URL, SPECIMENS_URL
from flask_app.sp_cache.models import SpecimenRecord


# Need an easy way to get solr classes
# Try using results_cls parameter to get and post proper results

# .....................................................................................
def get_collection_solr():
    """Get solr connection to collections core.

    Returns:
        pysolr.Solr: A Solr connection to the collections Solr collection.

    Todo:
        Incorporate this into flask better.
    """
    return pysolr.Solr(COLLECTIONS_URL)


# .....................................................................................
def get_specimen_solr():
    """Get solr connection to specimens core.

    Returns:
        pysolr.Solr: A Solr connection to the specimens Solr collection.

    Todo:
        Incorporate this into flask better.
    """
    return pysolr.Solr(OCCURRENCES_URL)
    # return pysolr.Solr(SPECIMENS_URL)

# .....................................................................................
def assemble_specimen_identifier(collection_id, occurrence_id):
    collection_occurrence_id = f"{collection_id}:{occurrence_id}"
    return collection_occurrence_id


# .....................................................................................
def specimen_identifier_fieldname():
    return "collection_occurrence_id"


# .....................................................................................
def post_collection(collection):
    """Post a new collection.

    Args:
        collection (Collection): A Collection object to add to the solr index.
    """
    solr_coll = get_collection_solr()
    solr_coll.add(collection.serialize_json(), commit=True)


# .....................................................................................
def get_collection(collection_id):
    """Get a collection from Solr.

    Args:
        collection_id (str): The identifier for the collection to retrieve.

    Returns:
        Collection: Collection information from the Solr index.
    """
    solr_coll = get_collection_solr()
    return solr_coll.search(collection_id)


# .....................................................................................
def update_collection(collection):
    """Update a collection in the solr index.

    Args:
        collection (dict): Updated collection information that replaces old.
    """
    solr_coll = get_collection_solr()
    retval = solr_coll.update(collection)
    return retval

# .....................................................................................
def get_summary():
    """Get server summary information."""
    pass


# .....................................................................................
def delete_collection(collection_id):
    """Delete a collection from the index.

    Args:
        collection_id (str): The identifer for the collection to remove.
    """
    solr_coll = get_collection_solr()
    retval = solr_coll.delete(id=collection_id, commit=True)
    return retval


# .....................................................................................
def update_collection_occurrences(collection_id, specimens):
    """Update collection occurrences.

    Args:
        collection_id (str): The identifer for the collection containing these
            specimens.
        specimens (list of dict): Specimen records to add or replace in the index.
    """
    solr_spec = get_specimen_solr()
    retval = solr_spec.add(specimens, commit=True)
    return retval


# .....................................................................................
def delete_collection_occurrences(collection_id, identifiers):
    """Delete specimens from the index.

    Args:
        collection_id (str): The identifier for the collection holding these records.
        identifiers (list of str): Identifiers for records to remove from the index.
    """
    solr_spec = get_specimen_solr()
    solr_spec.delete(
        q=[
            'collection_id:{}'.format(collection_id),
            'identifier:{}'.format(identifiers)
        ]
    )


# .....................................................................................
def get_specimen(collection_id, identifier):
    """Get a specimen from the index.

    Args:
        collection_id (str): The identifier for the collection holding this specimen.
        identifier (str): The identifier for the record to retrieve.

    Returns:
        SpecimenRecord: The specimen record from the index.
    """
    solr_spec = get_specimen_solr()
    rec = solr_spec.search(
        'identifier:{}'.format(identifier),
        fq='collection_id:{}'.format(collection_id)
    )
    if rec.hits > 0:
        return SpecimenRecord(rec.docs[0])
    return None
