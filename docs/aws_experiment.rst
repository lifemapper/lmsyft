=============================================
AWS strategy
=============================================

Data
---------------------------
* on us-east-1
* Bucket bison-321942852011-us-east-1

gbif_extract: 303237553
gbif_parquet_extract: 301669806

Use python libs **awscli** and **boto3** to connect with AWS

* query (Occurrences only):

  https://www.gbif.org/occurrence/search?basis_of_record=OCCURRENCE&country=US&has_coordinate=true&has_geospatial_issue=false&occurrence_status=present

Test data
...............
* DwCA
* Citation: GBIF.org (09 October 2023) GBIF Occurrence Download  https://doi.org/10.15468/dl.4vqtdy
* 1,421,222 records
* download:


Setup
---------------------------
* install aws-cli

Workflow
---------------------------
* Reference scripts:

  * Stored procedures in rs_stored_procedures.sql


* Redshift: Subset GBIF data from Amazon Registry of Open Data (AWS ODR) for processing
    * Use rs_subset_gbif.sql
    * Dynamically fill in current and previous dataset names using stored procedure
      sp_get_current_gbif_date