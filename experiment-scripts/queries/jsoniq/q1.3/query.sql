SELECT sum((lo_lo_extendedprice * lo_lo_discount)) FROM ( SELECT  *  FROM ( SELECT  *  FROM (( SELECT "LO_LO_ORDERKEY", "LO_LO_LINENUMBER", "LO_LO_CUSTKEY", "LO_LO_PARTKEY", "LO_LO_SUPPKEY", "LO_LO_ORDERDATE", "LO_LO_ORDERPRIORITY", "LO_LO_SHIPPRIORITY", "LO_LO_QUANTITY", "LO_LO_EXTENDEDPRICE", "LO_LO_ORDTOTALPRICE", "LO_LO_DISCOUNT", "LO_LO_REVENUE", "LO_LO_SUPPLYCOST", "LO_LO_TAX", "LO_LO_COMMITDATE", "LO_LO_SHIPMODE" FROM ( SELECT "LO_LO_ORDERKEY", "LO_LO_LINENUMBER", "LO_LO_CUSTKEY", "LO_LO_PARTKEY", "LO_LO_SUPPKEY", "LO_LO_ORDERDATE", "LO_LO_ORDERPRIORITY", "LO_LO_SHIPPRIORITY", "LO_LO_QUANTITY", "LO_LO_EXTENDEDPRICE", "LO_LO_ORDTOTALPRICE", "LO_LO_DISCOUNT", "LO_LO_REVENUE", "LO_LO_SUPPLYCOST", "LO_LO_TAX", "LO_LO_COMMITDATE", "LO_SHIPMODE" AS "LO_LO_SHIPMODE" FROM ( SELECT "LO_LO_ORDERKEY", "LO_LO_LINENUMBER", "LO_LO_CUSTKEY", "LO_LO_PARTKEY", "LO_LO_SUPPKEY", "LO_LO_ORDERDATE", "LO_LO_ORDERPRIORITY", "LO_LO_SHIPPRIORITY", "LO_LO_QUANTITY", "LO_LO_EXTENDEDPRICE", "LO_LO_ORDTOTALPRICE", "LO_LO_DISCOUNT", "LO_LO_REVENUE", "LO_LO_SUPPLYCOST", "LO_LO_TAX", "LO_COMMITDATE" AS "LO_LO_COMMITDATE", "LO_SHIPMODE" FROM ( SELECT "LO_LO_ORDERKEY", "LO_LO_LINENUMBER", "LO_LO_CUSTKEY", "LO_LO_PARTKEY", "LO_LO_SUPPKEY", "LO_LO_ORDERDATE", "LO_LO_ORDERPRIORITY", "LO_LO_SHIPPRIORITY", "LO_LO_QUANTITY", "LO_LO_EXTENDEDPRICE", "LO_LO_ORDTOTALPRICE", "LO_LO_DISCOUNT", "LO_LO_REVENUE", "LO_LO_SUPPLYCOST", "LO_TAX" AS "LO_LO_TAX", "LO_COMMITDATE", "LO_SHIPMODE" FROM ( SELECT "LO_LO_ORDERKEY", "LO_LO_LINENUMBER", "LO_LO_CUSTKEY", "LO_LO_PARTKEY", "LO_LO_SUPPKEY", "LO_LO_ORDERDATE", "LO_LO_ORDERPRIORITY", "LO_LO_SHIPPRIORITY", "LO_LO_QUANTITY", "LO_LO_EXTENDEDPRICE", "LO_LO_ORDTOTALPRICE", "LO_LO_DISCOUNT", "LO_LO_REVENUE", "LO_SUPPLYCOST" AS "LO_LO_SUPPLYCOST", "LO_TAX", "LO_COMMITDATE", "LO_SHIPMODE" FROM ( SELECT "LO_LO_ORDERKEY", "LO_LO_LINENUMBER", "LO_LO_CUSTKEY", "LO_LO_PARTKEY", "LO_LO_SUPPKEY", "LO_LO_ORDERDATE", "LO_LO_ORDERPRIORITY", "LO_LO_SHIPPRIORITY", "LO_LO_QUANTITY", "LO_LO_EXTENDEDPRICE", "LO_LO_ORDTOTALPRICE", "LO_LO_DISCOUNT", "LO_REVENUE" AS "LO_LO_REVENUE", "LO_SUPPLYCOST", "LO_TAX", "LO_COMMITDATE", "LO_SHIPMODE" FROM ( SELECT "LO_LO_ORDERKEY", "LO_LO_LINENUMBER", "LO_LO_CUSTKEY", "LO_LO_PARTKEY", "LO_LO_SUPPKEY", "LO_LO_ORDERDATE", "LO_LO_ORDERPRIORITY", "LO_LO_SHIPPRIORITY", "LO_LO_QUANTITY", "LO_LO_EXTENDEDPRICE", "LO_LO_ORDTOTALPRICE", "LO_DISCOUNT" AS "LO_LO_DISCOUNT", "LO_REVENUE", "LO_SUPPLYCOST", "LO_TAX", "LO_COMMITDATE", "LO_SHIPMODE" FROM ( SELECT "LO_LO_ORDERKEY", "LO_LO_LINENUMBER", "LO_LO_CUSTKEY", "LO_LO_PARTKEY", "LO_LO_SUPPKEY", "LO_LO_ORDERDATE", "LO_LO_ORDERPRIORITY", "LO_LO_SHIPPRIORITY", "LO_LO_QUANTITY", "LO_LO_EXTENDEDPRICE", "LO_ORDTOTALPRICE" AS "LO_LO_ORDTOTALPRICE", "LO_DISCOUNT", "LO_REVENUE", "LO_SUPPLYCOST", "LO_TAX", "LO_COMMITDATE", "LO_SHIPMODE" FROM ( SELECT "LO_LO_ORDERKEY", "LO_LO_LINENUMBER", "LO_LO_CUSTKEY", "LO_LO_PARTKEY", "LO_LO_SUPPKEY", "LO_LO_ORDERDATE", "LO_LO_ORDERPRIORITY", "LO_LO_SHIPPRIORITY", "LO_LO_QUANTITY", "LO_EXTENDEDPRICE" AS "LO_LO_EXTENDEDPRICE", "LO_ORDTOTALPRICE", "LO_DISCOUNT", "LO_REVENUE", "LO_SUPPLYCOST", "LO_TAX", "LO_COMMITDATE", "LO_SHIPMODE" FROM ( SELECT "LO_LO_ORDERKEY", "LO_LO_LINENUMBER", "LO_LO_CUSTKEY", "LO_LO_PARTKEY", "LO_LO_SUPPKEY", "LO_LO_ORDERDATE", "LO_LO_ORDERPRIORITY", "LO_LO_SHIPPRIORITY", "LO_QUANTITY" AS "LO_LO_QUANTITY", "LO_EXTENDEDPRICE", "LO_ORDTOTALPRICE", "LO_DISCOUNT", "LO_REVENUE", "LO_SUPPLYCOST", "LO_TAX", "LO_COMMITDATE", "LO_SHIPMODE" FROM ( SELECT "LO_LO_ORDERKEY", "LO_LO_LINENUMBER", "LO_LO_CUSTKEY", "LO_LO_PARTKEY", "LO_LO_SUPPKEY", "LO_LO_ORDERDATE", "LO_LO_ORDERPRIORITY", "LO_SHIPPRIORITY" AS "LO_LO_SHIPPRIORITY", "LO_QUANTITY", "LO_EXTENDEDPRICE", "LO_ORDTOTALPRICE", "LO_DISCOUNT", "LO_REVENUE", "LO_SUPPLYCOST", "LO_TAX", "LO_COMMITDATE", "LO_SHIPMODE" FROM ( SELECT "LO_LO_ORDERKEY", "LO_LO_LINENUMBER", "LO_LO_CUSTKEY", "LO_LO_PARTKEY", "LO_LO_SUPPKEY", "LO_LO_ORDERDATE", "LO_ORDERPRIORITY" AS "LO_LO_ORDERPRIORITY", "LO_SHIPPRIORITY", "LO_QUANTITY", "LO_EXTENDEDPRICE", "LO_ORDTOTALPRICE", "LO_DISCOUNT", "LO_REVENUE", "LO_SUPPLYCOST", "LO_TAX", "LO_COMMITDATE", "LO_SHIPMODE" FROM ( SELECT "LO_LO_ORDERKEY", "LO_LO_LINENUMBER", "LO_LO_CUSTKEY", "LO_LO_PARTKEY", "LO_LO_SUPPKEY", "LO_ORDERDATE" AS "LO_LO_ORDERDATE", "LO_ORDERPRIORITY", "LO_SHIPPRIORITY", "LO_QUANTITY", "LO_EXTENDEDPRICE", "LO_ORDTOTALPRICE", "LO_DISCOUNT", "LO_REVENUE", "LO_SUPPLYCOST", "LO_TAX", "LO_COMMITDATE", "LO_SHIPMODE" FROM ( SELECT "LO_LO_ORDERKEY", "LO_LO_LINENUMBER", "LO_LO_CUSTKEY", "LO_LO_PARTKEY", "LO_SUPPKEY" AS "LO_LO_SUPPKEY", "LO_ORDERDATE", "LO_ORDERPRIORITY", "LO_SHIPPRIORITY", "LO_QUANTITY", "LO_EXTENDEDPRICE", "LO_ORDTOTALPRICE", "LO_DISCOUNT", "LO_REVENUE", "LO_SUPPLYCOST", "LO_TAX", "LO_COMMITDATE", "LO_SHIPMODE" FROM ( SELECT "LO_LO_ORDERKEY", "LO_LO_LINENUMBER", "LO_LO_CUSTKEY", "LO_PARTKEY" AS "LO_LO_PARTKEY", "LO_SUPPKEY", "LO_ORDERDATE", "LO_ORDERPRIORITY", "LO_SHIPPRIORITY", "LO_QUANTITY", "LO_EXTENDEDPRICE", "LO_ORDTOTALPRICE", "LO_DISCOUNT", "LO_REVENUE", "LO_SUPPLYCOST", "LO_TAX", "LO_COMMITDATE", "LO_SHIPMODE" FROM ( SELECT "LO_LO_ORDERKEY", "LO_LO_LINENUMBER", "LO_CUSTKEY" AS "LO_LO_CUSTKEY", "LO_PARTKEY", "LO_SUPPKEY", "LO_ORDERDATE", "LO_ORDERPRIORITY", "LO_SHIPPRIORITY", "LO_QUANTITY", "LO_EXTENDEDPRICE", "LO_ORDTOTALPRICE", "LO_DISCOUNT", "LO_REVENUE", "LO_SUPPLYCOST", "LO_TAX", "LO_COMMITDATE", "LO_SHIPMODE" FROM ( SELECT "LO_LO_ORDERKEY", "LO_LINENUMBER" AS "LO_LO_LINENUMBER", "LO_CUSTKEY", "LO_PARTKEY", "LO_SUPPKEY", "LO_ORDERDATE", "LO_ORDERPRIORITY", "LO_SHIPPRIORITY", "LO_QUANTITY", "LO_EXTENDEDPRICE", "LO_ORDTOTALPRICE", "LO_DISCOUNT", "LO_REVENUE", "LO_SUPPLYCOST", "LO_TAX", "LO_COMMITDATE", "LO_SHIPMODE" FROM ( SELECT "LO_ORDERKEY" AS "LO_LO_ORDERKEY", "LO_LINENUMBER", "LO_CUSTKEY", "LO_PARTKEY", "LO_SUPPKEY", "LO_ORDERDATE", "LO_ORDERPRIORITY", "LO_SHIPPRIORITY", "LO_QUANTITY", "LO_EXTENDEDPRICE", "LO_ORDTOTALPRICE", "LO_DISCOUNT", "LO_REVENUE", "LO_SUPPLYCOST", "LO_TAX", "LO_COMMITDATE", "LO_SHIPMODE" FROM ( SELECT  *  FROM (lineorder)))))))))))))))))))) AS SNOWPARK_TEMP_TABLE_6XMLFESTL64NJNW CROSS JOIN ( SELECT "DT_D_DATEKEY", "DT_D_DATE", "DT_D_DAYOFWEEK", "DT_D_MONTH", "DT_D_YEAR", "DT_D_YEARMONTHNUM", "DT_D_YEARMONTH", "DT_D_DAYNUMINWEEK", "DT_D_DAYNUMINMONTH", "DT_D_DAYNUMINYEAR", "DT_D_MONTHNUMINYEAR", "DT_D_WEEKNUMINYEAR", "DT_D_SELLINGSEASON", "DT_D_LASTDAYINWEEKFL", "DT_D_LASTDAYINMONTHFL", "DT_D_HOLIDAYFL", "DT_D_WEEKDAYFL" FROM ( SELECT "DT_D_DATEKEY", "DT_D_DATE", "DT_D_DAYOFWEEK", "DT_D_MONTH", "DT_D_YEAR", "DT_D_YEARMONTHNUM", "DT_D_YEARMONTH", "DT_D_DAYNUMINWEEK", "DT_D_DAYNUMINMONTH", "DT_D_DAYNUMINYEAR", "DT_D_MONTHNUMINYEAR", "DT_D_WEEKNUMINYEAR", "DT_D_SELLINGSEASON", "DT_D_LASTDAYINWEEKFL", "DT_D_LASTDAYINMONTHFL", "DT_D_HOLIDAYFL", "D_WEEKDAYFL" AS "DT_D_WEEKDAYFL" FROM ( SELECT "DT_D_DATEKEY", "DT_D_DATE", "DT_D_DAYOFWEEK", "DT_D_MONTH", "DT_D_YEAR", "DT_D_YEARMONTHNUM", "DT_D_YEARMONTH", "DT_D_DAYNUMINWEEK", "DT_D_DAYNUMINMONTH", "DT_D_DAYNUMINYEAR", "DT_D_MONTHNUMINYEAR", "DT_D_WEEKNUMINYEAR", "DT_D_SELLINGSEASON", "DT_D_LASTDAYINWEEKFL", "DT_D_LASTDAYINMONTHFL", "D_HOLIDAYFL" AS "DT_D_HOLIDAYFL", "D_WEEKDAYFL" FROM ( SELECT "DT_D_DATEKEY", "DT_D_DATE", "DT_D_DAYOFWEEK", "DT_D_MONTH", "DT_D_YEAR", "DT_D_YEARMONTHNUM", "DT_D_YEARMONTH", "DT_D_DAYNUMINWEEK", "DT_D_DAYNUMINMONTH", "DT_D_DAYNUMINYEAR", "DT_D_MONTHNUMINYEAR", "DT_D_WEEKNUMINYEAR", "DT_D_SELLINGSEASON", "DT_D_LASTDAYINWEEKFL", "D_LASTDAYINMONTHFL" AS "DT_D_LASTDAYINMONTHFL", "D_HOLIDAYFL", "D_WEEKDAYFL" FROM ( SELECT "DT_D_DATEKEY", "DT_D_DATE", "DT_D_DAYOFWEEK", "DT_D_MONTH", "DT_D_YEAR", "DT_D_YEARMONTHNUM", "DT_D_YEARMONTH", "DT_D_DAYNUMINWEEK", "DT_D_DAYNUMINMONTH", "DT_D_DAYNUMINYEAR", "DT_D_MONTHNUMINYEAR", "DT_D_WEEKNUMINYEAR", "DT_D_SELLINGSEASON", "D_LASTDAYINWEEKFL" AS "DT_D_LASTDAYINWEEKFL", "D_LASTDAYINMONTHFL", "D_HOLIDAYFL", "D_WEEKDAYFL" FROM ( SELECT "DT_D_DATEKEY", "DT_D_DATE", "DT_D_DAYOFWEEK", "DT_D_MONTH", "DT_D_YEAR", "DT_D_YEARMONTHNUM", "DT_D_YEARMONTH", "DT_D_DAYNUMINWEEK", "DT_D_DAYNUMINMONTH", "DT_D_DAYNUMINYEAR", "DT_D_MONTHNUMINYEAR", "DT_D_WEEKNUMINYEAR", "D_SELLINGSEASON" AS "DT_D_SELLINGSEASON", "D_LASTDAYINWEEKFL", "D_LASTDAYINMONTHFL", "D_HOLIDAYFL", "D_WEEKDAYFL" FROM ( SELECT "DT_D_DATEKEY", "DT_D_DATE", "DT_D_DAYOFWEEK", "DT_D_MONTH", "DT_D_YEAR", "DT_D_YEARMONTHNUM", "DT_D_YEARMONTH", "DT_D_DAYNUMINWEEK", "DT_D_DAYNUMINMONTH", "DT_D_DAYNUMINYEAR", "DT_D_MONTHNUMINYEAR", "D_WEEKNUMINYEAR" AS "DT_D_WEEKNUMINYEAR", "D_SELLINGSEASON", "D_LASTDAYINWEEKFL", "D_LASTDAYINMONTHFL", "D_HOLIDAYFL", "D_WEEKDAYFL" FROM ( SELECT "DT_D_DATEKEY", "DT_D_DATE", "DT_D_DAYOFWEEK", "DT_D_MONTH", "DT_D_YEAR", "DT_D_YEARMONTHNUM", "DT_D_YEARMONTH", "DT_D_DAYNUMINWEEK", "DT_D_DAYNUMINMONTH", "DT_D_DAYNUMINYEAR", "D_MONTHNUMINYEAR" AS "DT_D_MONTHNUMINYEAR", "D_WEEKNUMINYEAR", "D_SELLINGSEASON", "D_LASTDAYINWEEKFL", "D_LASTDAYINMONTHFL", "D_HOLIDAYFL", "D_WEEKDAYFL" FROM ( SELECT "DT_D_DATEKEY", "DT_D_DATE", "DT_D_DAYOFWEEK", "DT_D_MONTH", "DT_D_YEAR", "DT_D_YEARMONTHNUM", "DT_D_YEARMONTH", "DT_D_DAYNUMINWEEK", "DT_D_DAYNUMINMONTH", "D_DAYNUMINYEAR" AS "DT_D_DAYNUMINYEAR", "D_MONTHNUMINYEAR", "D_WEEKNUMINYEAR", "D_SELLINGSEASON", "D_LASTDAYINWEEKFL", "D_LASTDAYINMONTHFL", "D_HOLIDAYFL", "D_WEEKDAYFL" FROM ( SELECT "DT_D_DATEKEY", "DT_D_DATE", "DT_D_DAYOFWEEK", "DT_D_MONTH", "DT_D_YEAR", "DT_D_YEARMONTHNUM", "DT_D_YEARMONTH", "DT_D_DAYNUMINWEEK", "D_DAYNUMINMONTH" AS "DT_D_DAYNUMINMONTH", "D_DAYNUMINYEAR", "D_MONTHNUMINYEAR", "D_WEEKNUMINYEAR", "D_SELLINGSEASON", "D_LASTDAYINWEEKFL", "D_LASTDAYINMONTHFL", "D_HOLIDAYFL", "D_WEEKDAYFL" FROM ( SELECT "DT_D_DATEKEY", "DT_D_DATE", "DT_D_DAYOFWEEK", "DT_D_MONTH", "DT_D_YEAR", "DT_D_YEARMONTHNUM", "DT_D_YEARMONTH", "D_DAYNUMINWEEK" AS "DT_D_DAYNUMINWEEK", "D_DAYNUMINMONTH", "D_DAYNUMINYEAR", "D_MONTHNUMINYEAR", "D_WEEKNUMINYEAR", "D_SELLINGSEASON", "D_LASTDAYINWEEKFL", "D_LASTDAYINMONTHFL", "D_HOLIDAYFL", "D_WEEKDAYFL" FROM ( SELECT "DT_D_DATEKEY", "DT_D_DATE", "DT_D_DAYOFWEEK", "DT_D_MONTH", "DT_D_YEAR", "DT_D_YEARMONTHNUM", "D_YEARMONTH" AS "DT_D_YEARMONTH", "D_DAYNUMINWEEK", "D_DAYNUMINMONTH", "D_DAYNUMINYEAR", "D_MONTHNUMINYEAR", "D_WEEKNUMINYEAR", "D_SELLINGSEASON", "D_LASTDAYINWEEKFL", "D_LASTDAYINMONTHFL", "D_HOLIDAYFL", "D_WEEKDAYFL" FROM ( SELECT "DT_D_DATEKEY", "DT_D_DATE", "DT_D_DAYOFWEEK", "DT_D_MONTH", "DT_D_YEAR", "D_YEARMONTHNUM" AS "DT_D_YEARMONTHNUM", "D_YEARMONTH", "D_DAYNUMINWEEK", "D_DAYNUMINMONTH", "D_DAYNUMINYEAR", "D_MONTHNUMINYEAR", "D_WEEKNUMINYEAR", "D_SELLINGSEASON", "D_LASTDAYINWEEKFL", "D_LASTDAYINMONTHFL", "D_HOLIDAYFL", "D_WEEKDAYFL" FROM ( SELECT "DT_D_DATEKEY", "DT_D_DATE", "DT_D_DAYOFWEEK", "DT_D_MONTH", "D_YEAR" AS "DT_D_YEAR", "D_YEARMONTHNUM", "D_YEARMONTH", "D_DAYNUMINWEEK", "D_DAYNUMINMONTH", "D_DAYNUMINYEAR", "D_MONTHNUMINYEAR", "D_WEEKNUMINYEAR", "D_SELLINGSEASON", "D_LASTDAYINWEEKFL", "D_LASTDAYINMONTHFL", "D_HOLIDAYFL", "D_WEEKDAYFL" FROM ( SELECT "DT_D_DATEKEY", "DT_D_DATE", "DT_D_DAYOFWEEK", "D_MONTH" AS "DT_D_MONTH", "D_YEAR", "D_YEARMONTHNUM", "D_YEARMONTH", "D_DAYNUMINWEEK", "D_DAYNUMINMONTH", "D_DAYNUMINYEAR", "D_MONTHNUMINYEAR", "D_WEEKNUMINYEAR", "D_SELLINGSEASON", "D_LASTDAYINWEEKFL", "D_LASTDAYINMONTHFL", "D_HOLIDAYFL", "D_WEEKDAYFL" FROM ( SELECT "DT_D_DATEKEY", "DT_D_DATE", "D_DAYOFWEEK" AS "DT_D_DAYOFWEEK", "D_MONTH", "D_YEAR", "D_YEARMONTHNUM", "D_YEARMONTH", "D_DAYNUMINWEEK", "D_DAYNUMINMONTH", "D_DAYNUMINYEAR", "D_MONTHNUMINYEAR", "D_WEEKNUMINYEAR", "D_SELLINGSEASON", "D_LASTDAYINWEEKFL", "D_LASTDAYINMONTHFL", "D_HOLIDAYFL", "D_WEEKDAYFL" FROM ( SELECT "DT_D_DATEKEY", "D_DATE" AS "DT_D_DATE", "D_DAYOFWEEK", "D_MONTH", "D_YEAR", "D_YEARMONTHNUM", "D_YEARMONTH", "D_DAYNUMINWEEK", "D_DAYNUMINMONTH", "D_DAYNUMINYEAR", "D_MONTHNUMINYEAR", "D_WEEKNUMINYEAR", "D_SELLINGSEASON", "D_LASTDAYINWEEKFL", "D_LASTDAYINMONTHFL", "D_HOLIDAYFL", "D_WEEKDAYFL" FROM ( SELECT "D_DATEKEY" AS "DT_D_DATEKEY", "D_DATE", "D_DAYOFWEEK", "D_MONTH", "D_YEAR", "D_YEARMONTHNUM", "D_YEARMONTH", "D_DAYNUMINWEEK", "D_DAYNUMINMONTH", "D_DAYNUMINYEAR", "D_MONTHNUMINYEAR", "D_WEEKNUMINYEAR", "D_SELLINGSEASON", "D_LASTDAYINWEEKFL", "D_LASTDAYINMONTHFL", "D_HOLIDAYFL", "D_WEEKDAYFL" FROM ( SELECT  *  FROM (date)))))))))))))))))))) AS SNOWPARK_TEMP_TABLE_TDUKDVEVBRPJSBY)) WHERE (((((((lo_lo_orderdate = dt_d_datekey) AND (dt_d_weeknuminyear = 6:: INTEGER)) AND (dt_d_year = 1994:: INTEGER)) AND (lo_lo_discount >= 5:: INTEGER)) AND (lo_lo_discount <= 7:: INTEGER)) AND (lo_lo_quantity >= 36:: INTEGER)) AND (lo_lo_quantity <= 40:: INTEGER)));