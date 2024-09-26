for $dt in collection("date")
for $c in collection("customer")
for $s in collection("supplier")
for $p in collection("part")
for $lo in collection("lineorder")
where 
  $lo.lo_custkey eq $c.c_custkey
  and $lo.lo_suppkey eq $s.s_suppkey
  and $lo.lo_partkey eq $p.p_partkey
  and $lo.lo_orderdate eq $dt.d_datekey
  and $c.c_region eq "AMERICA"
  and $s.s_nation eq "UNITED STATES"
  and ($dt.d_year eq 1997 or $dt.d_year eq 1998)
  and $p.p_category eq "MFGR#14"
let $profit := $lo.lo_revenue - $lo.lo_supplycost 
group by $dy := $dt.d_year, $sc := $s.s_city, $pb := $p.p_brand1
order by $dy, $sc, $pb
return { "d_year": $dy, "s_city": $sc, "p_brand1": $pb, "profit": sum($profit) }