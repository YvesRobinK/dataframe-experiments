#!/usr/bin/env bash

# Deployment parameters
system=${1:-'snowflake'}  # can be 'snowflake' or 'rumbledb'; only changes the queries
warehouse_name=${2:-'small'} # 'xsmall' or 'large'
schema_name=${3:-'SSB_SF1'}  # 'adl' or 'adl_1000'

# Experiment parameters
warmups_per_query=(2 2 2 2 2 2 2 2 2 2 2 2 2)
#warmups_per_query=(1 1 1 1 1 1 1 1 1)
runs_per_query=(3 3 3 3 3 3 3 3 3 3 3 3 3)
#runs_per_query=(1 1 1 1 1 1 1 1 1)
disable_result_cache=${4:-"yes"} # can also be 'no'
disable_data_cache=${5:-"yes"}    # can also be 'yes'
database_name=${6:-'SSB'}
RUN_WARMUP=${7:-"yes"}  # can also be 'no'

# Point to the right system's query path
if [ "${system}" = "snowflake" ]; then
  QUERY_PATHS="/home/ec2-user/dataframe-experiments/experiment-scripts/queries/sql"
else
  QUERY_PATHS="/home/dan/data/projects/java/rumbledb-snowflake/workloads/ssb/queries/jsoniq"
fi

# queries=("q1.1" "q2.1" "q3.1" "q4.1")
queries=("q1.1" "q1.2" "q1.3" "q2.1" "q2.2" "q2.3" "q3.1" "q3.2" "q3.3" "q3.4" "q4.1" "q4.2" "q4.3")
#queries=("q1.2" "q1.3" "q2.2" "q2.3" "q3.2" "q3.3" "q3.4" "q4.2" "q4.3")

# We prepare the experiment folder and log
EXPERIEMNT_NAME="${system}_ssb_run_${warehouse_name}_${schema_name}"
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


  # Run the warmup
  if [ "${RUN_WARMUP}" = "yes" ]; then
    for i in $(seq 1 ${warmups_per_query[${idx}]}); do
      ${BASE_COMMAND} -f ${QUERY_FILE}
    done
  fi

  # Execute the query several times
  for i in $(seq 1 ${runs_per_query[${idx}]}); do
    ${BASE_COMMAND} -f ${QUERY_FILE} | tee ${EXPERIMENT_FOLDER}/${query}/run_${i}.log
    # ${BASE_COMMAND} -q "SELECT LAST_QUERY_ID();" | tee -a ${EXPERIMENT_FOLDER}/${query}/run_${i}.log

    # Get the execution time (and convert from s to ms); also get the last query id
    execution_datetime=$( date +%F-%H-%M-%S )
    query_id=$( cat ${EXPERIMENT_FOLDER}/${query}/run_${i}.log | tail -n 4 | head -n 1 | awk '{print $2}' )
    
    exec_time=$( cat ${EXPERIMENT_FOLDER}/${query}/run_${i}.log | tail -n 8 | head -n 1 | awk '{print $6}' )
    exec_time=${exec_time%s}

    # Log this experiment to disk 
    echo "${execution_datetime},${warehouse_name},${database_name},${schema_name},${disable_data_cache},${disable_result_cache},\
      ${EXPERIEMNT_NAME},${query},${i},-1,-1,$( bc <<< "${exec_time} * 1000" ),${query_id}" >> ${EXPERIMENT_FOLDER}/experiment.log.csv
  done
  (( idx++ ))
done

# Echo the final status of the experiment
echo "NOTE: Experiment finished; the resulting logs are in `pwd`/${EXPERIMENT_FOLDER}"

