-- This SQL script is used to upload the ADL data to into Snowflake
-- Somewhat based on: https://docs.snowflake.com/en/user-guide/script-data-load-transform-parquet.html
-- Note that you need a integration called `s3_integration` if you intend to upload data from cloud storage
  -- For s3 follow this guide: https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration.html
-- run this within `snowsql` using: `!source /path/to/upload.sql`
-- run this using `snowsql` using: `$ snowsql -c <connection_name> -f /path/to/upload.sql`
SET warehouse_name = 'TEST_WAREHOUSE';
SET database_name = 'TEST';
SET schema_name = 'ADL';

CREATE DATABASE IF NOT EXISTS identifier($database_name);
USE DATABASE identifier($database_name);

CREATE SCHEMA IF NOT EXISTS identifier($schema_name);
USE SCHEMA identifier($schema_name);

USE WAREHOUSE identifier($warehouse_name);

-- We add the parquet data to Snowflake
-- Step 1) Create the temp stages
CREATE OR REPLACE TEMPORARY STAGE ssb_data_customer
storage_integration = s3_integration
URL = '<PATH>/customer.tbl'
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' REPLACE_INVALID_CHARACTERS = TRUE SKIP_HEADER = 0 RECORD_DELIMITER = '\n' );

CREATE OR REPLACE TEMPORARY STAGE ssb_data_date
storage_integration = s3_integration
URL = '<PATH>/date.tbl'
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' REPLACE_INVALID_CHARACTERS = TRUE SKIP_HEADER = 0 RECORD_DELIMITER = '\n' );

CREATE OR REPLACE TEMPORARY STAGE ssb_data_lineorder
storage_integration = s3_integration
URL = '<PATH>/lineorder.tbl/'
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' REPLACE_INVALID_CHARACTERS = TRUE SKIP_HEADER = 0 RECORD_DELIMITER = '\n' );

CREATE OR REPLACE TEMPORARY STAGE ssb_data_part
storage_integration = s3_integration
URL = '<PATH>/part.tbl'
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' REPLACE_INVALID_CHARACTERS = TRUE SKIP_HEADER = 0 RECORD_DELIMITER = '\n' );

CREATE OR REPLACE TEMPORARY STAGE ssb_data_supplier
storage_integration = s3_integration
URL = '<PATH>/supplier.tbl'
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' REPLACE_INVALID_CHARACTERS = TRUE SKIP_HEADER = 0 RECORD_DELIMITER = '\n' ); --  FIELD_OPTIONALLY_ENCLOSED_BY = '"'

-- Step 2) Create the tables and populate them with the staged data
CREATE OR REPLACE TABLE customer (
  c_custkey    INTEGER, --numeric identifier
  c_name       VARCHAR(25),     -- VARCHAR(50), --variable text, size 25 'customer'||custkey
  c_address    VARCHAR(25),     -- VARCHAR(50), --variable text, size 25 (city below)
  c_city       VARCHAR(10),     -- varchar(10), --fixed text, size 10 (10/nation: nation_prefix||(0-9)
  c_nation     VARCHAR(15),     -- varchar(15), --fixed text(15) (25 values, longest united kingdom)
  c_region     VARCHAR(12),     -- varchar(12), --fixed text, size 12 (5 values: longest middle east)
  c_phone      VARCHAR(15),     -- varchar(15), --fixed text, size 15 (many values, format: 43-617-354-1222)
  c_mktsegment VARCHAR(10)      -- varchar(10) --fixed text, size 10 (longest is automobile)
) AS
  SELECT
    $1::INTEGER,
    $2::VARCHAR(25),
    $3::VARCHAR(25),
    $4::VARCHAR(10),
    $5::VARCHAR(15),
    $6::VARCHAR(12),
    $7::VARCHAR(15),
    $8::VARCHAR(10)
  FROM @ssb_data_customer;


CREATE OR REPLACE TABLE date (
  d_datekey          INTEGER,     -- identifier, unique id -- e.g. 19980327 (what we use)
  d_date             VARCHAR(18),  -- varchar(18), --fixed STRING, size 18, longest: december 22, 1998
  d_dayofweek        VARCHAR(9),  -- varchar(8), --fixed STRING, size 8, sunday, monday, ..., saturday)
  d_month            VARCHAR(9),  -- varchar(9), --fixed STRING, size 9: january, ..., december
  d_year             INTEGER,     -- unique value 1992-1998
  d_yearmonthnum     INTEGER,     -- numeric (yyyymm) -- e.g. 199803
  d_yearmonth        VARCHAR(7),  -- varchar(7), --fixed STRING, size 7: mar1998 for example
  d_daynuminweek     INTEGER,     -- numeric 1-7
  d_daynuminmonth    INTEGER,     -- numeric 1-31
  d_daynuminyear     INTEGER,     -- numeric 1-366
  d_monthnuminyear   INTEGER,     -- numeric 1-12
  d_weeknuminyear    INTEGER,     -- numeric 1-53
  d_sellingseason    VARCHAR(12),  -- varchar(12), --STRING, size 12 (christmas, summer,...)
  d_lastdayinweekfl  INTEGER,     -- 1 bit
  d_lastdayinmonthfl INTEGER,     -- 1 bit
  d_holidayfl        INTEGER,     -- 1 bit
  d_weekdayfl        INTEGER     -- 1 bit
) AS
  SELECT
  $1::INTEGER,
  $2::VARCHAR(18),
  $3::VARCHAR(9),
  $4::VARCHAR(9),
  $5::INTEGER,
  $6::INTEGER,
  $7::VARCHAR(7),
  $8::INTEGER,
  $9::INTEGER,
  $10::INTEGER,
  $11::INTEGER,
  $12::INTEGER,
  $13::VARCHAR(12),
  $14::INTEGER,
  $15::INTEGER,
  $16::INTEGER,
  $17::INTEGER
FROM @ssb_data_date;

CREATE OR REPLACE TABLE lineorder (
  lo_orderkey      INTEGER,     -- numeric (INTEGER up to sf 300) first 8 of each 32 keys used
  lo_linenumber    INTEGER,     -- numeric 1-7
  lo_custkey       INTEGER,     -- numeric identifier foreign key reference to c_custkey
  lo_partkey       INTEGER,     -- identifier foreign key reference to p_partkey
  lo_suppkey       INTEGER,     -- numeric identifier foreign key reference to s_suppkey
  lo_orderdate     INTEGER,     -- identifier foreign key reference to d_datekey
  lo_orderpriority VARCHAR(15),  -- varchar(15), --fixed text, size 15 (5 priorities: 1-urgent, etc.)
  lo_shippriority  VARCHAR(1),  -- varchar(1), --fixed text, size 1
  lo_quantity      INTEGER,     -- numeric 1-50 (for part)
  lo_extendedprice INTEGER,     -- numeric, max about 55,450 (for part)
  lo_ordtotalprice INTEGER,     -- numeric, max about 388,000 (for order)
  lo_discount      INTEGER,     -- numeric 0-10 (for part) -- (represents percent)
  lo_revenue       INTEGER,     -- numeric (for part: (extendedprice*(100-discount))/100)
  lo_supplycost    INTEGER,     -- numeric (for part, cost from supplier, max = ?)
  lo_tax           INTEGER,     -- numeric 0-8 (for part)
  lo_commitdate    INTEGER,     -- foreign key reference to d_datekey
  lo_shipmode      VARCHAR(10)  -- varchar(10) --fixed text, size 10 (modes: reg air, air, etc.)
) AS
  SELECT
  $1::INTEGER,
  $2::INTEGER,
  $3::INTEGER,
  $4::INTEGER,
  $5::INTEGER,
  $6::INTEGER,
  $7::VARCHAR(15),
  $8::VARCHAR(1),
  $9::INTEGER,
  $10::INTEGER,
  $11::INTEGER,
  $12::INTEGER,
  $13::INTEGER,
  $14::INTEGER,
  $15::INTEGER,
  $16::INTEGER,
  $17::VARCHAR(10)
  FROM @ssb_data_lineorder (PATTERN => '.*');

CREATE OR REPLACE TABLE part (
  p_partkey   INTEGER,        -- identifier
  p_name      VARCHAR(22),     -- varchar(22), --variable text, size 22 (not unique per part but never was)
  p_mfgr      VARCHAR(15),     -- varchar(6), --fixed text, size 6 (mfgr#1-5, card = 5)
  p_category  VARCHAR(15),     -- varchar(7), --fixed text, size 7 ('mfgr#'||1-5||1-5: card = 25)
  p_brand1    VARCHAR(15),     -- varchar(9), --fixed text, size 9 (category||1-40: card = 1000)
  p_color     VARCHAR(11),     -- varchar(11), --variable text, size 11 (card = 94)
  p_type      VARCHAR(25),     -- VARCHAR(50), --variable text, size 25 (card = 150)
  p_size      INTEGER,        -- numeric 1-50 (card = 50)
  p_container VARCHAR(15)     -- varchar(15) --fixed text(10) (card = 40)
) AS
  SELECT
    $1::INTEGER,
    $2::VARCHAR(22),
    $3::VARCHAR(15),
    $4::VARCHAR(15),
    $5::VARCHAR(15),
    $6::VARCHAR(11),
    $7::VARCHAR(25),
    $8::INTEGER,
    $9::VARCHAR(15)
  FROM @ssb_data_part;

CREATE OR REPLACE TABLE supplier (
  s_suppkey INTEGER,     -- identifier
  s_name    VARCHAR(25),  -- VARCHAR(50), --fixed text, size 25: 'supplier'||suppkey
  s_address VARCHAR(25),  -- VARCHAR(50), --variable text, size 25 (city below)
  s_city    VARCHAR(10),  -- varchar(10), --fixed text, size 10 (10/nation: nation_prefix||(0-9))
  s_nation  VARCHAR(15),  -- varchar(15), --fixed text(15) (25 values, longest united kingdom)
  s_region  VARCHAR(12),  -- varchar(12), --fixed text, size 12 (5 values: longest middle east)
  s_phone   VARCHAR(15)  -- varchar(15) --fixed text, size 15 (many values, format: 43-617-354-1222)
) AS
  SELECT
    $1::INTEGER,
    $2::VARCHAR(25),
    $3::VARCHAR(25),
    $4::VARCHAR(10),
    $5::VARCHAR(15),
    $6::VARCHAR(12),
    $7::VARCHAR(15)
  FROM @ssb_data_supplier;