"""Flask functions for Specify Cache."""
import datetime
import json
import os

from flask import Blueprint, request
from werkzeug.exceptions import NotFound

import flask_app.sp_cache.config as config
import flask_app.sp_cache.models as models
import flask_app.sp_cache.solr_controller as controller


bp = Blueprint('sp_cache', __name__, url_prefix='/sp_cache')


# .....................................................................................
@bp.route('/', methods=['GET'])
def sp_cache_status():
    """Get overall health of the cache.

    Returns:
        dict: A dictionary of status information for the server.
    """
    num_collections = 0
    num_records = 0
    system_status = 'In Development'
    return {
        'num_collections': num_collections,
        'num_records': num_records,
        'status': system_status
    }


# .....................................................................................
@bp.route('/collection', methods=['POST'])
def sp_cache_collection_post():
    """Post a new collection to the cache.

    Returns:
        dict: Collection information in dictionary format (JSON).
    """
    collection_json = request.get_json(force=True)
    collection = models.Collection(collection_json)
    controller.post_collection(collection)
    # Write collection information backup file
    collection_id = collection.attributes['collection_id']
    collection_filename = os.path.join(
        config.COLLECTION_BACKUP_PATH, '{}.json'.format(collection_id)
    )
    with open(collection_filename, mode='wt') as out_json:
        json.dump(collection_json, out_json)
    return controller.get_collection(collection_id).docs[0]


# .....................................................................................
@bp.route('/collection/<string:collection_id>', methods=['GET', 'PUT'])
def sp_cache_collection_get_or_put(collection_id):
    """Return information about a cached collection.

    Args:
        collection_id (str): An identifier for the collection to retrieve.

    Returns:
        dict: Collection information in JSON format.

    Raises:
        NotFound: Raised if the collection is not found.
    """
    if request.method.lower() == 'put':
        collection_json = request.get_json(force=True)
        collection = models.Collection(collection_json)
        controller.update_collection(collection)
    collection = controller.get_collection(collection_id)
    if collection.hits > 0:
        return collection.docs[0]
    raise NotFound()


# .....................................................................................
@bp.route(
    '/collection/<string:collection_id>/occurrences/',
    methods=['DELETE', 'POST', 'PUT']
)
def collection_occurrences_modify(collection_id):
    """Modify collection specimen holdings.

    Args:
        collection_id (str): An identifier associated with these specimens.

    Returns:
        tuple: Tuple of empty string and 204 indicating successful delete.
    """
    if request.method.lower() in ['post', 'put']:
        # Write data to file system for another process to pick up and handle
        now = datetime.datetime.now()
        date_string = now.strftime('%Y_%m_%d_%H_%M_%S')
        dwca_filename = os.path.join(
            config.DWCA_PATH, 'collection-{}-{}-{}.zip'.format(
                collection_id, request.method.lower(), date_string
            )
        )
        with open(dwca_filename, mode='wb') as dwca_out:
            dwca_out.write(request.data)
    elif request.method.lower() == 'delete':
        delete_identifiers = request.json['delete_identifiers']
        controller.delete_collection_occurrences(collection_id, delete_identifiers)
    return ('', 204)


# .....................................................................................
@bp.route(
    '/collection/<string:collection_id>/occurrences/<string:identifier>',
    methods=['DELETE', 'GET', 'PUT']
)
def collection_occurrence(collection_id, identifier):
    """Retrieve a specimen record.

    Args:
        collection_id (str): An identifer for the collection holding this specimen.
        identifier (str): An identifier for the specimen to retrieve.

    Returns:
        dict: Returned if the specimen is found and requested to retrieve (GET).
        None: Returned if the specimen is found and deleted (DELETE).

    Raises:
        NotFound: Raised if the desired specimen is not found.
    """
    if request.method.lower() == 'delete':
        return controller.delete_collection_occurrences(collection_id, [identifier])
    elif request.method.lower() == 'get':
        specimen = controller.get_specimen(collection_id, identifier)
        if specimen is not None:
            return specimen.serialize_json()
        raise NotFound()
    elif request.method.lower() == 'put':
        new_specimen_record = models.SpecimenRecord(request.json)
        controller.update_collection_occurrences(collection_id, [new_specimen_record])
        return controller.get_specimen(collection_id, identifier).serialize_json()
