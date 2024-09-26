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
  and $s.s_region eq "AMERICA"
  and ($p.p_mfgr eq "MFGR#1" or $p.p_mfgr eq "MFGR#2")
let $profit := $lo.lo_revenue - $lo.lo_supplycost 
group by $dy := $dt.d_year, $cn := $c.c_nation 
order by $dy, $cn
return { "d_year": $dy, "c_nation": $cn, "profit": sum($profit) }