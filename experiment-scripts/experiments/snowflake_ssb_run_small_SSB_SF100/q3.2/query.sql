ALTER WAREHOUSE small SUSPEND;
ALTER WAREHOUSE small RESUME IF SUSPENDED;
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
!set execution_only=true;
select c_city, s_city, d_year, sum(lo_revenue) as revenue
from customer, lineorder, supplier, date
where lo_custkey = c_custkey
and lo_suppkey = s_suppkey
and lo_orderdate = d_datekey
and c_nation = 'UNITED STATES'
and s_nation = 'UNITED STATES'
and d_year >= 1992 and d_year <= 1997
group by c_city, s_city, d_year
order by d_year asc, revenue desc;
!set execution_only=false;
SELECT LAST_QUERY_ID();
