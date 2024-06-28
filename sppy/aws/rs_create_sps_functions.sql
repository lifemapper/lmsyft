-- ----------------------------------------------------------------------------
-- SQL User defined functions
-- ----------------------------------------------------------------------------
-- Get first day of current month for latest GBIF Data in AWS ODR
CREATE OR REPLACE FUNCTION f_current_datestr ()
    RETURNS VARCHAR
STABLE AS
$$
    select SPLIT_PART(CURRENT_DATE, '-', 1) || '-' || SPLIT_PART(CURRENT_DATE, '-', 2) || '-01';
$$ LANGUAGE sql;

SELECT f_current_datestr();

-- ----------------------------------------------------------------------------
-- Python (2.7) User defined functions
-- ----------------------------------------------------------------------------
-- BAD, Get first day of current month for latest GBIF Data in AWS ODR
--CREATE OR REPLACE FUNCTION f_current_datestr_py()
--    RETURNS VARCHAR
--STABLE AS
--$$
--    import datetime
--    n = datetime.datetime.now()
--    date_str = "{}-{02d}-01".format(n.year, n.month)
--    return date_str
--$$ LANGUAGE plpythonu;

-- ----------------------------------------------------------------------------
-- Stored Procedures
-- ----------------------------------------------------------------------------
-- Get first day of previous month to find obsolete data
CREATE OR REPLACE PROCEDURE public.sp_get_previous_datestr(last_date OUT VARCHAR)
LANGUAGE plpgsql
AS $$
DECLARE
    yr VARCHAR;
    mo VARCHAR;
BEGIN
    yr := SPLIT_PART(CURRENT_DATE, '-', 1);
    mo := SPLIT_PART(CURRENT_DATE, '-', 2);
    IF mo = '01' THEN
        yr := CAST (yr AS INTEGER) - 1;
        mo := '12';
    else
        BEGIN
            mo := CAST (mo AS INTEGER) - 1;
            IF mo < 10 THEN
                mo := '0' || mo;
            END IF;
        END;
    END IF;
    SELECT INTO last_date yr || '-' || mo || '-01';
END;
$$

-- CALL public.sp_get_previous_datestr();

-- -------------------------------------------------------
-- Mount latest GBIF data in Amazon Registry of Open Data
-- Note that this uses GBIF fieldnames, including DATASET_GBIF_KEY defined in
--     sppy.aws.aws_constants
CREATE OR REPLACE PROCEDURE public.mount_gbif ()
AS $$
    DECLARE datestr VARCHAR := CALL public.sp_get_current_gbif_date();
BEGIN
    EXECUTE 'CREATE EXTERNAL TABLE redshift_spectrum.gbif_occurrence_' || datestr || '_parquet ('
    || 'gbifid	VARCHAR(max),'
    || 'datasetkey	VARCHAR(max),'
    || 'occurrenceid	VARCHAR(max),'
    || 'kingdom	VARCHAR(max),'
    || 'phylum	VARCHAR(max),'
	|| 'class	VARCHAR(max),'
	|| '_order	VARCHAR(max),'
	|| 'family	VARCHAR(max),'
	|| 'genus	VARCHAR(max),'
	|| 'species	VARCHAR(max),'
	|| 'infraspecificepithet	VARCHAR(max),'
	|| 'taxonrank	VARCHAR(max),'
	|| 'scientificname	VARCHAR(max),'
	|| 'verbatimscientificname	VARCHAR(max),'
	|| 'verbatimscientificnameauthorship	VARCHAR(max),'
	|| 'countrycode	VARCHAR(max),'
	|| 'locality	VARCHAR(max),'
	|| 'stateprovince	VARCHAR(max),'
	|| 'occurrencestatus	VARCHAR(max),'
	|| 'individualcount     INT,'
    || 'publishingorgkey	VARCHAR(max),'
	|| 'decimallatitude	DOUBLE PRECISION,'
	|| 'decimallongitude	DOUBLE PRECISION,'
	|| 'coordinateuncertaintyinmeters	DOUBLE PRECISION,'
	|| 'coordinateprecision	DOUBLE PRECISION,'
	|| 'elevation	DOUBLE PRECISION,'
	|| 'elevationaccuracy	DOUBLE PRECISION,'
	|| 'depth	DOUBLE PRECISION,'
	|| 'depthaccuracy	DOUBLE PRECISION,'
	|| 'eventdate	TIMESTAMP,'
	|| 'day	INT,'
	|| 'month	INT,'
	|| 'year	INT,'
	|| 'taxonkey	INT,'
	|| 'specieskey	INT,'
	|| 'basisofrecord	VARCHAR(max),'
	|| 'institutioncode	VARCHAR(max),'
	|| 'collectioncode	VARCHAR(max),'
	|| 'catalognumber	VARCHAR(max),'
	|| 'recordnumber	VARCHAR(max),'
    || 'identifiedby	SUPER,'
	|| 'dateidentified	TIMESTAMP,'
	|| 'license	VARCHAR(max),'
	|| 'rightsholder	VARCHAR(max),'
	|| 'recordedby	SUPER,'
	|| 'typestatus	SUPER,'
	|| 'establishmentmeans	VARCHAR(max),'
	|| 'lastinterpreted	TIMESTAMP,'
	|| 'mediatype	SUPER,'
	|| 'issue    SUPER )'
    || 'STORED AS PARQUET'
    || 'LOCATION \'s3://gbif-open-data-us-east-1/occurrence/' || datestr || '/occurrence.parquet/';

END;
$$ LANGUAGE plpgsql;
