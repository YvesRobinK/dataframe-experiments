for $lo in collection("lineorder")
for $dt in collection("date")
where 
  $lo.lo_orderdate eq $dt.d_datekey
  and $dt.d_weeknuminyear eq 6 
  and $dt.d_year eq 1994
  and $lo.lo_discount ge 5 and $lo.lo_discount le 7
  and $lo.lo_quantity ge 36 and $lo.lo_quantity le 40
return sum($lo.lo_extendedprice * $lo.lo_discount)