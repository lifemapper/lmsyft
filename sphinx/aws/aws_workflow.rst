AWS Workflow
####################

Reference
===========================================================

* Use `Amazon Redshift Serverless
  <https://docs.aws.amazon.com/redshift/latest/mgmt/working-with-serverless.html>`_
* Redshift may require purchasing extra AWS tools that do not automatically come
  with your account.  The processes will only run once per month, so minimal
  capabilities should be adequate.
* All relevant functions/tools for data analysis workflow are in the sppy/aws directory
  of the repository (https://github.com/specifysystems/sp_network/tree/main/sppy/aws)
* Policies are needed for Roles performing actions, including permissions for S3 and
  Glue:  **permissions_for_redshift_s3.py**
* Stored procedures: **rs_stored_procedures.sql**


Steps
===========================================================

1. Redshift: Create namespace and workgroup for steps and temporary data
****************************************************************

TODO: fully document this process and the meaning of each component.

Note: This is a manual task, as it only needs to be performed the first time.

* Create a **Namespace** (collection of database objects and users) and a **Workgroup**
  (collection of compute resources) for the project.

  * On the AWS Dashboard, choose Amazon Redshift
  * Create a Workgroup and Namespace for this project in this account
  * Attach  an IAM Role to the Namespace that allows Redshift data creation, editing,
    deletion.  The Role must have Permissions Policies that allow Actions on Resources.

    * The KU account uses the AmazonRedshift-CommandsAccessRole-20231129T105842
      which contains the following policies.  These policies are defined in JSON in
      the file **permissions_for_redshift_s3.py**.  TODO: streamline these policies as
      there is some overlap.

      * AmazonRedshiftAllCommandsFullAccess (AWS managed) policy
      * AmazonRedshift-CommandsAccessPolicy-20231129T105842 (Customer managed) policy
      * aimee-glue-some (Customer inline) policy

2. Redshift: Subset GBIF for Specify Network processing
***********************************************************

Note: This task may be automated, triggered by new GBIF data available from the
Amazon Registry of Open Data (AWS ODR) on the first of every month, or simply monthly
on the first of the month.

* Redshift: Subset GBIF data from the AWS ODR for processing

  * If stored procedures have not yet been created in your Redshift namespace,
    run **rs_create_stored_procedures.sql** to create procedures for the subset script.
    Note: these procedures are not currently being used, but exist for use in
    the automated workflow.
  * This task is performed in Amazon Redshift, currently by running the commands in
    **rs_subset_gbif.sql** manually in the Redshift Query Editor.
  * The SQL commands will:

    * Create an external schema (only needed the first time), named redshift_spectrum,
      to link GBIF S3 data to Redshift Spectrum.
    * Create an external table, which mounts S3 data in Redshift for temporary use.
    * Drops the previous month's subset data table
    * Subsets the fields and records from the newly mounted GBIF data and creates a
      table.
    * Drops the mounted GBIF S3 data table.


2. Redshift: Summarize species and occurrence counts
***********************************************************

* Redshift: Create summary tables on the subsetted GBIF data and write to S3
* This task is performed in Amazon Redshift, currently by running the commands in
  **rs_summarize_data.sql** manually in the Redshift Query Editor.
* The SQL commands will:

    * Create a dataset counts table with records containing one record for every
      dataset, with fields **datasetkey, occurrence count, species count**
    * Create a dataset lists table with records containing one record for every
      taxon in each dataset, with
      fields **datasetkey, taxonkey, species, and occurrence count**
    * Write both of those tables to S3 in parquet format.
    * Drop the previous month's summary tables.

3. Python scripts: Summarize these tables by 2 data dimensions
******************************************************************************

* Further summarize these tables by 2 data dimensions (dataset and species) into
  summary matrices using Python scripts on a local or EC2 machine, then write to S3 for
  API query.

  * These Python scripts may be performed on a local machine or EC2 instance running
    a Docker instance.
  * The **build_test_aggregated_data.py** script

1.5 TODO
***********************************************************

* Annotate records for other summaries, climate, landcover, etc
