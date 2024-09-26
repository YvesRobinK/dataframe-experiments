for $lo in collection("lineorder")
for $dt in collection("date")
for $p in collection("part")
for $s in collection("supplier")
where 
  $lo.lo_orderdate eq $dt.d_datekey
  and $lo.lo_partkey eq $p.p_partkey
  and $lo.lo_suppkey eq $s.s_suppkey
  and $p.p_brand1 eq "MFGR#2221"
  and $s.s_region eq "EUROPE"
let $rev := $lo.lo_revenue 
group by $dy := $dt.d_year, $pb := $p.p_brand1  
order by $dy, $pb
return { "d_year": $dy, "p_brand": $pb, "revenue": sum($rev) }