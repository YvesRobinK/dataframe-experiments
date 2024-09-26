#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Deployment parameters
warehouse_name=${1:-'large'} # 'xsmall' or 'large'
schema_name=${2:-'ssb_sf1'}      # 'adl' or 'adl_1000'
database_name=${3:-'ssb'}

# Experiment parameters
warmups_per_query=(1 1 1 1)  # (3 3 3 3 3 1 3 3)
runs_per_query=(2 2 2 2)  # (3 3 3 3 3 3 3 3)
generate_query_only=${4:-"no"}   # can also be 'yes'
disable_result_cache=${5:-"yes"} # can also be 'no'
disable_data_cache=${6:-"no"}    # can also be 'yes'

# Path parameters
REBUILD_RUMBLE=${7:-'yes'}       # can be 'yes' or something else
SNOWFLAKE_CONFIGURATION_FILE=${8:-'/home/dan/data/projects/java/rumbledb-snowflake/workloads/adl/experiment-scripts/snowflake.properties'}
QUERY_PATHS=${9:-'/home/dan/data/projects/java/rumbledb-snowflake/workloads/ssb/queries/jsoniq'}
RUMBLE_PATH=${10:-'/home/dan/data/projects/java/rumble'}

# Rumble default JAR name
RUMBLE_JAR_NAME=${11:-'rumbledb-1.14.0-jar-with-dependencies.jar'}

# Cleanup parameters
DELETE_JAR=${12:-"yes"}

# Warmup parameters
RUN_WARMUP=${13:-"yes"}  # can also be 'no'

# This should build the JAR and move to the JAR location
if [ "${REBUILD_RUMBLE}" = "yes" ]; then
  cd ${RUMBLE_PATH} && mvn clean compile assembly:single
fi
cd ${RUMBLE_PATH}/target

# Copy the Snowflake config file and bring it locally 
cp ${SNOWFLAKE_CONFIGURATION_FILE} ./snowflake.original.properties && \
sed "s/WAREHOUSENAME/$warehouse_name/" snowflake.original.properties \
| sed "s/DBNAME/$database_name/" \
| sed "s/SCHEMANAME/$schema_name/" \
> snowflake.updated.properties

queries=("q1.1" "q2.1" "q3.1" "q4.1")

# We now execute the experiments
EXPERIEMNT_NAME="RumbleDB_ssb_run_$(date +%F_%H_%M_%S_%3N)"
EXPERIMENT_FOLDER="${SCRIPT_DIR}/experiments/${EXPERIEMNT_NAME}"
mkdir -p ${EXPERIMENT_FOLDER}

# Copy the jar to the experiment folder and cd there
cp ${RUMBLE_JAR_NAME} snowflake.updated.properties ${EXPERIMENT_FOLDER}
cd ${EXPERIMENT_FOLDER}

run_query() {
  local run_count=$1
  local query=$2
  local is_warmup=$3

  java -jar ${RUMBLE_JAR_NAME} \
        --show-error-info no \
        --print-iterator-tree no \
        --output-path temp.out \
        --snowflake-session-config snowflake.updated.properties \
        --exec-mode eval \
        --warehouse-name ${warehouse_name} \
        --disable-data-cache ${disable_data_cache} \
        --disable-result-cache ${disable_result_cache} \
        --experiment-id ${EXPERIEMNT_NAME} \
        --query-tag ${query} \
        --current-run-count ${run_count} \
        --log-dump-file experiment.log.csv \
        --query-path ${QUERY_PATHS}/${query}/query.jq \
        --is-warmup ${is_warmup} \
        --generate-query-only ${generate_query_only}
}

idx=0
for query in ${queries[@]}; do
  mkdir -p ${EXPERIMENT_FOLDER}/${query}

  # Run the warmup
  if [ "${RUN_WARMUP}" = "yes" ]; then
    for i in $(seq 1 ${warmups_per_query[${idx}]}); do
      run_query "0" "${query}" "yes"
    done
  fi

  # Execute the query several times
  for i in $(seq 1 ${runs_per_query[${idx}]}); do
    run_query "${i}" "${query}" "no" \
    | tee ${EXPERIMENT_FOLDER}/${query}/run_${i}.log
  done
  (( idx++ ))
done

# Delete jar if requested
if [ "${DELETE_JAR}" = "yes" ]; then 
  rm ${RUMBLE_JAR_NAME}
fi

# Echo the final status of the experiment
echo "NOTE: Experiment finished; the resulting logs are in ${EXPERIMENT_FOLDER}"