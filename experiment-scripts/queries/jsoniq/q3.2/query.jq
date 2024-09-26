for $c in collection("customer")
for $lo in collection("lineorder")
for $s in collection("supplier")
for $dt in collection("date")
where 
  $lo.lo_custkey eq $c.c_custkey
  and $lo.lo_suppkey eq $s.s_suppkey
  and $lo.lo_orderdate eq $dt.d_datekey
  and $c.c_nation eq "UNITED STATES"
  and $s.s_nation eq "UNITED STATES"
  and $dt.d_year ge 1992 and $dt.d_year le 1997
let $rev := $lo.lo_revenue 
group by $cc := $c.c_city, $sc := $s.s_city, $dy := $dt.d_year
order by $dy ascending, sum($rev) descending
return { "c_city": $cc, "s_city": $sc, "d_year": $dy, "revenue": sum($rev) }