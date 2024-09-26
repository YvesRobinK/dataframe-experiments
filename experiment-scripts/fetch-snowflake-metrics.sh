#!/usr/bin/env bash

if [ "$#" -eq "0" ]; then
  echo "./fetch-snowflake-metrics.sh experiment-path-1 [other-experiment-path]*"
  exit
fi

LOG_FILE_NAME="experiment.log.csv"
SUMMARY_FILE_NAME="./summary.log.csv"

create_summary() {
  file_location=$1

echo "dateTime,warehouseName,databaseName,schemaName,disableDataCache,disableResultCache,experimentID,queryTag,currentRunCount,queryGenerationTimeMean,queryGenerationTimeStd,queryExecutionTime,snowflakeQueryID","QUERY_ID","QUERY_TEXT","DATABASE_ID","DATABASE_NAME","SCHEMA_ID","SCHEMA_NAME","QUERY_TYPE","SESSION_ID","USER_NAME","ROLE_NAME","WAREHOUSE_ID","WAREHOUSE_NAME","WAREHOUSE_SIZE","WAREHOUSE_TYPE","CLUSTER_NUMBER","QUERY_TAG","EXECUTION_STATUS","ERROR_CODE","ERROR_MESSAGE","START_TIME","END_TIME","TOTAL_ELAPSED_TIME","BYTES_SCANNED","PERCENTAGE_SCANNED_FROM_CACHE","BYTES_WRITTEN","BYTES_WRITTEN_TO_RESULT","BYTES_READ_FROM_RESULT","ROWS_PRODUCED","ROWS_INSERTED","ROWS_UPDATED","ROWS_DELETED","ROWS_UNLOADED","BYTES_DELETED","PARTITIONS_SCANNED","PARTITIONS_TOTAL","BYTES_SPILLED_TO_LOCAL_STORAGE","BYTES_SPILLED_TO_REMOTE_STORAGE","BYTES_SENT_OVER_THE_NETWORK","COMPILATION_TIME","EXECUTION_TIME","QUEUED_PROVISIONING_TIME","QUEUED_REPAIR_TIME","QUEUED_OVERLOAD_TIME","TRANSACTION_BLOCKED_TIME","OUTBOUND_DATA_TRANSFER_CLOUD","OUTBOUND_DATA_TRANSFER_REGION","OUTBOUND_DATA_TRANSFER_BYTES","INBOUND_DATA_TRANSFER_CLOUD","INBOUND_DATA_TRANSFER_REGION","INBOUND_DATA_TRANSFER_BYTES","LIST_EXTERNAL_FILES_TIME","CREDITS_USED_CLOUD_SERVICES","RELEASE_VERSION","EXTERNAL_FUNCTION_TOTAL_INVOCATIONS","EXTERNAL_FUNCTION_TOTAL_SENT_ROWS","EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS","EXTERNAL_FUNCTION_TOTAL_SENT_BYTES","EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES","QUERY_LOAD_PERCENT","IS_CLIENT_GENERATED_STATEMENT","QUERY_ACCELERATION_BYTES_SCANNED","QUERY_ACCELERATION_PARTITIONS_SCANNED","QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR,TRANSACTION_ID,CHILD_QUERIES_WAIT_TIME,ROLE_TYPE" > ${file_location}
}


for path in "$@"; do
  create_summary ${SUMMARY_FILE_NAME}
  line_count=0
  while IFS= read -r line; do
    if [ ! "$line_count" -eq "0" ]; then
      query_id=$( echo $line | awk -F',' '{print $13}' )
      snowsql -c xsmall -d SSB -s SSB_SF1 -q "SELECT * FROM snowflake.account_usage.query_history WHERE QUERY_ID = '${query_id}';" \
        -o output_file=./output.csv -o quiet=true -o friendly=false -o header=true -o output_format=csv
      echo ${line},$( cat ./output.csv | tail -n 1 ) >> ${SUMMARY_FILE_NAME}
    fi
    (( line_count++ ))
done < ${path}/${LOG_FILE_NAME}
  mv ${SUMMARY_FILE_NAME} ${path}
  echo "Summary finished and is available at: ${path}"
done

