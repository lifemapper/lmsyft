Testing Specify Network elements
#####################################

**TODO**: update this document

Obsolete: solr
----------------------

* Solr commands at /opt/solr/bin/ (in PATH)

  * Create new core::

      su -c "/opt/solr/bin/solr create -c occurrences -d /var/solr/cores/occurrences/conf -s 2 -rf 2" solr

  * Delete core::

      /opt/solr/bin/solr delete -c occurrences

  * Options to populate solr data into newly linked core::

     /opt/solr/bin/post -c occurrences sp_network/tests/test_data/*csv
     curl -c occurrences sp_network/tests/test_data/*.csv

  * Options to search::

     curl "http://localhost:8983/solr/occurrences/select?q=*.*"

* Web UI for Solr admin:

  http://<fqdn>:8983/solr/#/occurrences/core-overview

Troubleshooting
----------------

  * /var/solr/logs/solr.log
