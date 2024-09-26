for $c in collection("customer")
for $lo in collection("lineorder")
for $s in collection("supplier")
for $dt in collection("date")
where 
  $lo.lo_custkey eq $c.c_custkey
  and $lo.lo_suppkey eq $s.s_suppkey
  and $lo.lo_orderdate eq $dt.d_datekey
  and $c.c_nation eq "UNITED KINGDOM"
  and ($c.c_city eq "UNITED KI1" or $c.c_city eq "UNITED KI5")
  and ($s.s_city eq "UNITED KI1" or $s.s_city eq "UNITED KI5")
  and $s.s_nation eq "UNITED KINGDOM"
  and $dt.d_yearmonth eq "Dec1997"
let $rev := $lo.lo_revenue 
group by $cc := $c.c_city, $sc := $s.s_city, $dy := $dt.d_year
order by $dy ascending, sum($rev) descending
return { "c_city": $cc, "s_city": $sc, "d_year": $dy, "revenue": sum($rev) }