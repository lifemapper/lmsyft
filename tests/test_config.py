"""Test our Solr config for sp_cache."""
import pysolr

from flask_app.sp_cache.config import COLLECTIONS_URL, SPECIMENS_URL


# .....................................................................................
def test_connect_to_collections_core():
    """Test getting a connection to collections index."""
    pysolr.Solr(COLLECTIONS_URL)


# .....................................................................................
def test_connect_to_specimens_core():
    """Test getting a connection to specimens index."""
    pysolr.Solr(SPECIMENS_URL)
