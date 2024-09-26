#!/usr/bin/env bash

# Deployment parameters
system=${1:-'snowflake'}  # can be 'snowflake' or 'rumbledb'; only changes the queries
warehouse_name=${2:-'xsmall'} # 'xsmall' or 'large'
schema_name=${3:-'SSB_SF1'}  # 'adl' or 'adl_1000'

QUERY_PATHS="/home/yves/Desktop/MasterThesis/master_thesis/snowflake scipts/rumblebd-snowflake-scripts-master/workloads/ssb/queries/sql"

# queries=("q1.1" "q2.1" "q3.1" "q4.1")
# queries=("q1.1" "q1.2" "q1.3" "q2.1" "q2.2" "q2.3" "q3.1" "q3.2" "q3.3" "q3.4" "q4.1" "q4.2" "q4.3")
queries=("q1.1" "q1.2" "q1.3" "q2.2" "q2.3" "q3.2" "q3.3" "q3.4" "q4.2" "q4.3")

# We prepare the experiment folder and log
EXPERIEMNT_NAME="${system}_ssb_run_$(date +%F_%H_%M_%S_%3N)"
EXPERIMENT_FOLDER="experiments/${EXPERIEMNT_NAME}"
mkdir -p ${EXPERIMENT_FOLDER}

echo "dateTime,warehouseName,databaseName,schemaName,disableDataCache,disableResultCache,experimentID,queryTag,currentRunCount,queryGenerationTimeMean,queryGenerationTimeStd,\
  queryExecutionTime,snowflakeQueryID" > ${EXPERIMENT_FOLDER}/experiment.log.csv

# We now execute the experiments
idx=0
BASE_COMMAND="snowsql -c ${warehouse_name} -d ${database_name} -s ${schema_name}"
for query in ${queries[@]}; do
  # Create run folder and generate query text with appropriate cache disables and query id retrieval
  mkdir -p ${EXPERIMENT_FOLDER}/${query}
  QUERY_FILE=${EXPERIMENT_FOLDER}/${query}/query.sql

  if [ "${disable_data_cache}" = "yes" ]; then
    echo "ALTER WAREHOUSE ${warehouse_name} SUSPEND;" >> ${QUERY_FILE}
    echo "ALTER WAREHOUSE ${warehouse_name} RESUME IF SUSPENDED;" >> ${QUERY_FILE}
  fi

  if [ "${disable_result_cache}" = "yes" ]; then
    echo "ALTER SESSION SET USE_CACHED_RESULT = FALSE;" >> ${QUERY_FILE}
  fi

  echo "!set execution_only=true;" >> ${QUERY_FILE} 

  cat ${QUERY_PATHS}/${query}/query.sql >> ${QUERY_FILE}

  echo "!set execution_only=false;" >> ${QUERY_FILE}

  echo "SELECT LAST_QUERY_ID();" >> ${QUERY_FILE}


  
