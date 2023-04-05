"""Solr configuration parameters."""
import os


RESOLVER_URL = '{}:{}/solr/spcoco'.format(
    os.environ['SOLR_SERVER'],
    os.environ['SOLR_PORT']
)

ARK_PATTERN = 'http://spcoco.org/ark:/<guid>'
