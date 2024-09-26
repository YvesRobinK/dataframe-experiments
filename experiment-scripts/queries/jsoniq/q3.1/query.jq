for $c in collection("customer")
for $lo in collection("lineorder")
for $s in collection("supplier")
for $dt in collection("date")
where 
  $lo.lo_custkey eq $c.c_custkey
  and $lo.lo_suppkey eq $s.s_suppkey
  and $lo.lo_orderdate eq $dt.d_datekey
  and $c.c_region eq "ASIA"
  and $s.s_region eq "ASIA"
  and $dt.d_year ge 1992 and $dt.d_year le 1997
let $rev := $lo.lo_revenue 
group by $cn := $c.c_nation, $sn := $s.s_nation, $dy := $dt.d_year
order by $dy ascending, sum($rev) descending
return { "c_nation": $cn, "s_nation": $sn, "d_year": $dy, "revenue": sum($rev) }