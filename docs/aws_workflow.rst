References
####################

* Scripts:

  * Stored procedures in rs_stored_procedures.sql


Steps
####################

1. Subset GBIF for Specify Network processing
**********************

* Redshift: Subset GBIF data from Amazon Registry of Open Data (AWS ODR) for processing
    * First run rs_create_stored_procedures.sql to create procedures for the subset script.
    * Next run rs_subset_gbif.sql to subset the data
    *

1.5 TODO
*******

* Consider annotating records for other summaries, climate, landcover, etc

2. Summarize species and occurrence counts
**********************

* Redshift: Subset GBIF data from Amazon Registry of Open Data (AWS ODR) for processing

3. Query counts for quantification and comparison
**************************

* Glue/RDS/Redshift???

