#! /bin/bash

rm -f query.sql

warehouse=$1
echo "ALTER WAREHOUSE ${warehouse} SUSPEND;" >> query.sql
echo "ALTER WAREHOUSE ${warehouse} RESUME IF SUSPENDED;" >> query.sql

snowsql -c $1 -f query.sql;
#snowsql -c $1 -q ALTER WAREHOUSE ${var} RESUME IF SUSPENDED;;

