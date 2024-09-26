#! /bin/bash

queries=("SelectionQuery" "FilterQuery" "SortQuery" "GroupByQuery" "ColumOpAssignmentQuery" "JoinQuery")
runs=1
sf="SF100"
format="parquet"

for i in {1..${runs}}; do
  for query in ${queries[@]}; do
	  echo ${query}
	  python3 benchmark.py True ${query} ${sf} ${format}
  done
done
