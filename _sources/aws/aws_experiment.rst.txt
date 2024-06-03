AWS strategy
###########################################################

Setup
=============================================
* install aws-cli, boto3

Workflow
=============================================
* Reference scripts:

  * Stored procedures in rs_stored_procedures.sql

* Redshift: Subset GBIF data from Amazon Registry of Open Data (AWS ODR) for processing
    * Use rs_subset_gbif.sql
    * Dynamically fill in current and previous dataset names using stored procedure
      sp_get_current_gbif_date
