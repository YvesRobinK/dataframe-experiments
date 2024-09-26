for $lo in collection("lineorder")
for $dt in collection("date")
where 
  $lo.lo_orderdate eq $dt.d_datekey
  and $dt.d_yearmonthnum eq 199401
  and $lo.lo_discount ge 4 and $lo.lo_discount le 6
  and $lo.lo_quantity ge 26 and $lo.lo_quantity le 35
return sum($lo.lo_extendedprice * $lo.lo_discount)