#! /bin/bash

rm -f query.sql

warehouse=$1
echo "ALTER WAREHOUSE ${warehouse} SUSPEND;" >> susp_warehouse.sql
echo "ALTER WAREHOUSE ${warehouse} RESUME IF SUSPENDED;" >> susp_warehouse.sql


snowsql -c $1 -f $(pwd)/susp_warehouse.sql;
rm -f $(pwd)/susp_warehouse.sql

#snowsql -c $1 -q ALTER WAREHOUSE ${var} RESUME IF SUSPENDED;;