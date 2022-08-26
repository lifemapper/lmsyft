"""Test Solr functions."""
import flask_app.sp_cache.solr_controller as app_solr


# .....................................................................................
def test_get_collection_solr():
    """Test that a collection core connection can be made."""
    _ = app_solr.get_collection_solr()


# .....................................................................................
def test_get_specimen_solr():
    """Test that a specimen core connection can be made."""
    _ = app_solr.get_specimen_solr()


# .....................................................................................
def test_post_get_delete_collection():
    """Test various collection functions."""
    collection_id = 'test_collection'
    collection_data = {
        'collection_id': collection_id,
        'institution_name': 'test institution',
        'last_updated': '2021-05-03T11:06:00Z',
        'public_key': 'specify_pub_key',
        'collection_location': 'Specify HQ',
        'contact_name': 'Test User',
        'contact_email': 'test@sfytorium.org',
    }
    app_solr.post_collection(collection_data)
    _ = app_solr.get_collection(collection_id)
    app_solr.delete_collection(collection_id)
    _ = app_solr.get_collection(collection_id)


# .....................................................................................
# def test_post_get_delete_specimen():
#     """Test various specimen operations."""
#     collection_id = 'test_collection'
#     known_identifier = 'bae4b1f5-df83-4183-8b6e-005abc5d97ad'
#     test_filename = '../../test_data/dwc_update.zip'
#     with open(test_filename, mode='rb') as in_file:
#         app_solr.process_occurrence_dca(collection_id, in_file)
#     _ = app_solr.get_specimen(collection_id, known_identifier)
#     app_solr.delete_collection_occurrences(collection_id, [known_identifier])
