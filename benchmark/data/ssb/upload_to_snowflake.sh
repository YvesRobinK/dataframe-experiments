#!/usr/bin/env bash

data_path=${1:-'s3://rumbledb-snowflake-adl/ssb/sf1'}  # can be 's3://hep-adl-ethz/hep-parquet/native/Run2012B_SingleMu-1000.parquet' or 's3://rumbledb-snowflake-adl/SF1/'
warehouse_name=${2:-'SMALL'}
schema_name=${3:-'ssb_sf1'}  # can be 'adl_1000' or adl'
database_name=${4:-'ssb'}

sed "s/'TEST_WAREHOUSE'/'$warehouse_name'/" "upload.sql" \
| sed "s/'TEST'/'$database_name'/" \
| sed "s/'ADL'/'$schema_name'/" \
| sed "s,<PATH>,$data_path,g" \
> upload_temp.sql

snowsql -c small -o log_level=DEBUG -f upload_temp.sql
