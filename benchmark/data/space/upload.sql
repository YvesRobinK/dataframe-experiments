-- This SQL script is used to upload the ADL data to into Snowflake
-- Somewhat based on: https://docs.snowflake.com/en/user-guide/script-data-load-transform-parquet.html
-- Note that you need a integration called `s3_integration` if you intend to upload data from cloud storage
  -- For s3 follow this guide: https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration.html
-- run this within `snowsql` using: `!source /path/to/upload.sql`
-- run this using `snowsql` using: `$ snowsql -c <connection_name> -f /path/to/upload.sql`
SET warehouse_name = 'xsmall';
SET database_name = 'TEST';
SET schema_name = 'ADL';

CREATE DATABASE IF NOT EXISTS identifier($database_name);
USE DATABASE identifier($database_name);

CREATE SCHEMA IF NOT EXISTS identifier($schema_name);
USE SCHEMA identifier($schema_name);

USE WAREHOUSE identifier($warehouse_name);


GRANT CREATE STAGE ON SCHEMA identifier($schema_name) TO ROLE accountadmin;

GRANT USAGE ON INTEGRATION s3_integration TO ROLE accountadmin;

-- We add the parquet data to Snowflake
-- Step 1) Create the temp stages
CREATE OR REPLACE TEMPORARY STAGE train_data
storage_integration = s3_integration 
URL = '<PATH>/train.csv'
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' REPLACE_INVALID_CHARACTERS = TRUE SKIP_HEADER = 0 RECORD_DELIMITER = '\n' );

CREATE OR REPLACE TEMPORARY STAGE test_data
storage_integration = s3_integration 
URL = '<PATH>/test.csv'
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' REPLACE_INVALID_CHARACTERS = TRUE SKIP_HEADER = 0 RECORD_DELIMITER = '\n' );


-- Step 2) Create the tables and populate them with the staged data
CREATE OR REPLACE TABLE train (
  passengerID VARCHAR(10), --numeric identifier
  HomePlanet  VARCHAR(50),     -- VARCHAR(50), --variable text, size 25 'customer'||custkey
  CryoSleep VARCHAR,     -- VARCHAR(50), --variable text, size 25 (city below)
  Cabin VARCHAR(8),     -- varchar(10), --fixed text, size 10 (10/nation: nation_prefix||(0-9)
  Destination VARCHAR(15),     -- varchar(15), --fixed text(15) (25 values, longest united kingdom)
  AGE FLOAT,     -- varchar(12), --fixed text, size 12 (5 values: longest middle east)
  VIP VARCHAR(5),     -- varchar(15), --fixed text, size 15 (many values, format: 43-617-354-1222)
  RoomService FLOAT,      -- varchar(10) --fixed text, size 10 (longest is automobile)
  FoodCourt FLOAT,
  ShoppingMall FLOAT,
  Spa FLOAT,
  VRDeck FLOAT,
  Name VARCHAR(50),
  Transported VARCHAR(5)
) AS
  SELECT 
    $1::VARCHAR(7),
    $2::VARCHAR(50),	
    $3::VARCHAR,
    $4::VARCHAR(8),
    $5::VARCHAR(15),
    $6::FLOAT,
    $7::VARCHAR(5),
    $8::FLOAT,
    $9::FLOAT,
    $10::FLOAT,
    $11::FLOAT,
    $12::FLOAT,
    $13::VARCHAR(50),
    $14::VARCHAR(5)
  FROM @train_data;
  
  
CREATE OR REPLACE TABLE test (
  passengerID VARCHAR(10), --numeric identifier
  HomePlanet  VARCHAR(50),     -- VARCHAR(50), --variable text, size 25 'customer'||custkey
  CryoSleep VARCHAR(5),     -- VARCHAR(50), --variable text, size 25 (city below)
  Cabin VARCHAR(8),     -- varchar(10), --fixed text, size 10 (10/nation: nation_prefix||(0-9)
  Destination VARCHAR(15),     -- varchar(15), --fixed text(15) (25 values, longest united kingdom)
  AGE FLOAT,     -- varchar(12), --fixed text, size 12 (5 values: longest middle east)
  VIP VARCHAR(5),     -- varchar(15), --fixed text, size 15 (many values, format: 43-617-354-1222)
  RoomService FLOAT,      -- varchar(10) --fixed text, size 10 (longest is automobile)
  FoodCourt FLOAT,
  ShoppingMall FLOAT,
  Spa FLOAT,
  VRDeck FLOAT,
  Name VARCHAR(50)
) AS
  SELECT 
    $1::VARCHAR(7),
    $2::VARCHAR(50),	
    $3::VARCHAR(5),
    $4::VARCHAR(8),
    $5::VARCHAR(15),
    $6::FLOAT,
    $7::VARCHAR(5),
    $8::FLOAT,
    $9::FLOAT,
    $10::FLOAT,
    $11::FLOAT,
    $12::INTEGER,
    $13::VARCHAR(50)
  FROM @test_data;

