-- ----------------------------------------------------------------------------
-- SQL User defined functions
-- ----------------------------------------------------------------------------
-- Get first day of current month for latest GBIF Data in AWS ODR
CREATE OR REPLACE FUNCTION s_current_data ()
    RETURNS VARCHAR
STABLE AS
$$
    select SPLIT_PART(CURRENT_DATE, '-', 1) || '-' || SPLIT_PART(CURRENT_DATE, '-', 2) || '-01';
$$ LANGUAGE sql;

SELECT s_current_data();

-- ----------------------------------------------------------------------------
-- Python (2.7) User defined functions
-- ----------------------------------------------------------------------------
-- BAD, Get first day of current month for latest GBIF Data in AWS ODR
CREATE OR REPLACE FUNCTION p_current_data()
    RETURNS VARCHAR
STABLE AS
$$
    import datetime
    n = datetime.datetime.now()
    date_str = "{}-{02d}-01".format(n.year, n.month)
    return date_str
$$ LANGUAGE plpythonu;

-- ----------------------------------------------------------------------------
-- Stored Procedures
-- ----------------------------------------------------------------------------
-- Get first day of current month to for latest data
CREATE OR REPLACE PROCEDURE public.sp_get_current_gbif_date(this_date OUT VARCHAR)
LANGUAGE plpgsql
AS $$
DECLARE
    yr VARCHAR;
    mo VARCHAR;
BEGIN
    yr := SPLIT_PART(CURRENT_DATE, '-', 1);
    mo := SPLIT_PART(CURRENT_DATE, '-', 2);
    SELECT INTO this_date yr || '-' || mo || '-01';
END;
$$

-- Get first day of previous month to find obsolete data
CREATE OR REPLACE PROCEDURE public.sp_get_previous_gbif_date(last_date OUT VARCHAR)
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
    END IF;
    SELECT INTO last_date yr || '-' || mo || '-01';
END;
$$

-- CALL public.sp_get_previous_gbif_date();


