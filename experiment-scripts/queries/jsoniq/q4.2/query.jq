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
  and ($dt.d_year eq 1997 or $dt.d_year eq 1998)
  and ($p.p_mfgr eq "MFGR#1" or $p.p_mfgr eq "MFGR#2")
let $profit := $lo.lo_revenue - $lo.lo_supplycost 
group by $dy := $dt.d_year, $sn := $s.s_nation, $pc := $p.p_category
order by $dy, $sn, $pc
return { "d_year": $dy, "s_nation": $sn, "p_category": $pc, "profit": sum($profit) }