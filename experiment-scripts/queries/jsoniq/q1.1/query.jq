for $lo in collection("lineorder")
for $dt in collection("date")
where 
  $lo.lo_orderdate eq $dt.d_datekey
  and $dt.d_year eq 1993
  and $lo.lo_discount ge 1 and $lo.lo_discount le 3
  and $lo.lo_quantity lt 25
return sum($lo.lo_extendedprice * $lo.lo_discount)