#!/usr/bin/env bash

# Deployment parameters
system_type=${1:-'all'}  # 'snowflake' or 'rumbledb' or 'all'
warehouse_name=${2:-'large'} # 'xsmall' or 'large'
database_name=${3:-'SSB'}

# Experiment parameters
disable_result_cache=${4:-"yes"} # can also be 'no'
disable_data_cache=${5:-"no"}    # can also be 'yes'

SCRIPT_PATH="/home/dan/data/projects/java/rumbledb-snowflake/workloads/ssb/experiment-scripts/evaluate-snowflake-queries.sh"
RUN_WARMUP="yes"

# Create the schema names we will go over
# scales=( "SSB_SF1" "SSB_SF10" "SSB_SF100" "SSB_SF1000")
scales=( "SSB_SF1" "SSB_SF10" "SSB_SF100" "SSB_SF1000")

execute_system_experiments() {
  local system=$1

  # Create an experiment path
  EXPERIMENT_FOLDER="experiments/${system}_ssb_sweep_run_$(date +%F_%H_%M_%S_%3N)"
  mkdir -p ${EXPERIMENT_FOLDER}

  for i in ${scales[@]}; do 
    # Schedule a run at a given scale
    ${SCRIPT_PATH} \
      ${system} \
      ${warehouse_name} \
      ${i} \
      ${disable_result_cache} \
      ${disable_data_cache} \
      ${database_name} \
      ${RUN_WARMUP} \
      | tee -a ${EXPERIMENT_FOLDER}/console.log

     # Find where the results have been stored and copy the data over
    run_path=$( cat ${EXPERIMENT_FOLDER}/console.log | tail -n 1 | awk '{print $9;}')
    mkdir -p ${EXPERIMENT_FOLDER}/${i} && cp -r ${run_path}/* ${EXPERIMENT_FOLDER}/${i}
  done

  # Bring the individual experiment logs into one place and concatenate the summary
  for p in $( ls ${EXPERIMENT_FOLDER} ); do
    if [ -d "${EXPERIMENT_FOLDER}/${p}" ]; then
      if [ -f "${EXPERIMENT_FOLDER}/experiment.log.csv" ]; then
        cat ${EXPERIMENT_FOLDER}/${p}/experiment.log.csv | tail -n +2 >> ${EXPERIMENT_FOLDER}/experiment.log.csv
      else 
        cp ${EXPERIMENT_FOLDER}/${p}/experiment.log.csv ${EXPERIMENT_FOLDER}
      fi
    fi
  done

  # Report the final results as being done
  echo "NOTE: Experiment finished; the resulting logs are in `pwd`/${EXPERIMENT_FOLDER}"
}

# execute_system_experiments ${system_type}

if [ "${system_type}" = "all" ]; then
  snowflake_output=$( execute_system_experiments "snowflake" | tail -n 1 )
  rumbledb_output=$( execute_system_experiments "rumbledb"   | tail -n 1 )

  echo "Snowflake ${snowflake_output}"
  echo "RumbleDB  ${rumbledb_output}"
else 
  execute_system_experiments "${system_type}"
fi
