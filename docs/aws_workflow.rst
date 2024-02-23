References
####################

* Scripts:

  * Stored procedures in rs_stored_procedures.sql


Steps
####################

1. Subset GBIF for Specify Network processing
**********************

* Redshift: Subset GBIF data from Amazon Registry of Open Data (AWS ODR) for processing
    * Use rs_subset_gbif.sql
    * Dynamically fill in current and previous dataset names using stored procedure
      sp_get_current_gbif_date

TODO
*******

* Consider annotating records for other summaries, climate, landcover, etc

2. Summarize species and occurrence counts
**********************

* Redshift: Subset GBIF data from Amazon Registry of Open Data (AWS ODR) for processing

3. Query counts for quantification and comparison
**************************

* Glue/RDS/Redshift???

