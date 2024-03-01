-- Create lists and summaries of occurrences, species, and RIIS designation for each region

-- -------------------------------------------------------------------------------------
-- Create counts of occurrences and species by RIIS status for each region
-- -------------------------------------------------------------------------------------
CREATE TABLE public.dataset_counts_2024_02_01 AS
    SELECT DISTINCT datasetkey,
           COUNT(*) AS occ_count, COUNT(DISTINCT taxonkey) AS species_count
    FROM  specnet_2024_02_01
    GROUP BY datasetkey;

-- Check counts
SELECT * from dataset_counts_2024_02_01 ORDER BY datasetkey, species_count, occ_count LIMIT 10;

-- -------------------------------------------------------------------------------------
-- Create lists of species for each dataset with counts
-- -------------------------------------------------------------------------------------
CREATE TABLE public.dataset_lists_2024_02_01 AS
    SELECT DISTINCT datasetkey, taxonkey, species, COUNT(*) AS occ_count
    FROM  specnet_2024_02_01
    GROUP BY datasetkey, taxonkey, species;

-- Check counts
SELECT * from dataset_lists_2024_02_01 ORDER BY datasetkey, occ_count, species LIMIT 10;

-- -------------------------------------------------------------------------------------
-- Write data summaries to S3 as CSV for data delivery
-- -------------------------------------------------------------------------------------
-- Option: add "PARTITION BY (region_field)" to separate regions into individual files;
--      if so, remove the trailing underscore from the target folder name
-- Note: include "PARALLEL OFF" so it writes serially and does not write many very small
--      files.  Default max filesize is 6.2 GB, can change with option, for example,
--      "MAXFILESIZE 1 gb".

--UNLOAD (
--    'SELECT * FROM dataset_counts_2024_02_01 ORDER BY datasetkey, species_count, occ_count')
--    TO 's3://specnet-us-east-1/summary/dataset_counts_2024_02_01_'
--    IAM_role DEFAULT
--    CSV DELIMITER AS '\t'
--    HEADER
--    PARALLEL OFF;
--UNLOAD (
--    'SELECT * FROM dataset_lists_2024_02_01 ORDER BY datasetkey, occ_count, species')
--    TO 's3://specnet-us-east-1/summary/dataset_counts_2024_02_01_'
--    IAM_role DEFAULT
--    CSV DELIMITER AS '\t'
--    HEADER
--    PARALLEL OFF;

-- Also write as Parquet for easy DataFrame loading
UNLOAD (
    'SELECT * FROM dataset_counts_2024_02_01 ORDER BY datasetkey, species_count, occ_count')
    TO 's3://specnet-us-east-1/summary/dataset_counts_2024_02_01_'
    IAM_role DEFAULT
    FORMAT AS PARQUET
    PARALLEL OFF;
UNLOAD (
    'SELECT * FROM dataset_lists_2024_02_01 ORDER BY datasetkey, occ_count, species')
    TO 's3://specnet-us-east-1/summary/dataset_lists_2024_02_01_'
    IAM_role DEFAULT
    FORMAT AS PARQUET
    PARALLEL OFF;

-- Cleanup Redshift data summaries
DROP TABLE public.dataset_counts_2024_02_01;
DROP TABLE public.dataset_lists_2024_02_01;
