#! /bin/bash

source /home/ec2-user/anaconda3/etc/profile.d/conda.sh # put your path to conda
export PYTHONPATH=$PYTHONPATH:/home/ec2-user/dataframe-experiments


tasks=("ssb_1.1" "ssb_1.2" "ssb_1.3" "ssb_2.1" "ssb_2.2" "ssb_2.3" "ssb_3.1" "ssb_3.2" "ssb_3.3" "ssb_3.4" "ssb_4.1" "ssb_4.2" "ssb_4.3") #("ssb_1.1" "ssb_1.2" "ssb_1.3" "ssb_2.1" "ssb_2.2" "ssb_2.3" "ssb_3.1" "ssb_3.2" "ssb_3.3" "ssb_3.4" "ssb_4.1" "ssb_4.2" "ssb_4.3" "Fork" "Endeavor" "Catboost" "HousePricePrediction")
executors=("ModinDask")  #("Pandas" "Spark" "Snowpandas" "Polars" "ModinRay" "ModinDask" "Dask" "Vaex" "SnowparkPandas")
sf=("SF1" "SF10" "SF100" "SF1000")
warmup=1
measure=3
runs=$((warmup + measure))
format="parquet"
logfile="results/res.csv"
timeout="720"

declare -A environment_dict
environment_dict["Pandas"]="Pandas_env"
environment_dict["Modin"]="Modin_env"
environment_dict["Spark"]="Spark_env"
environment_dict["Snowpandas"]="omnisci-dev"
environment_dict["Polars"]="Polars_env"
environment_dict["ModinRay"]="Modin_env"
environment_dict["ModinDask"]="Modin_env"
environment_dict["Dask"]="Modin_env"
environment_dict["Vaex"]="Vaex_env"
environment_dict["SnowparkPandas"]="snowpark_pandas_env"

log_timeout_error() {
    local query_name=$1           # First argument: query name
    local scaling_factor=$2       # Second argument: scaling factor
    local engine=$3               # Third argument: engine

    local execution_end=$(date +%s)  # Start time in seconds since epoch
    local start_time=$(echo "$execution_end - $timeout" | bc)
    # Convert start time to formatted string
    local formatted_time=$(date -d @$start_time +"%Y-%m-%d-%H:%M:%S")
    # Construct the query_info string
    local query_info="start time: $formatted_time,\n"
    query_info+="query name: $query_name,\n"
    query_info+="scaling factor: $scaling_factor,\n"
    query_info+="engine: ${engine,,},\n"
    query_info+="error: TIMEOUT $timeout seconds exceeded\n"
    # Write the query_info to the error_log.txt file
    {
        echo -e "$query_info"
        echo "=================================="
        echo
    } >> results/error_log.txt
}

log_execution_details() {
    local logfile=$1             # First argument: log file path
    local query_name=$2          # Fourth argument: query name
    local scaling_factor=$3      # Fifth argument: scaling factor
    local format=$4              # Sixth argument: format
    local engine=$5              # Seventh argument: engine

    local execution_end=$(date +%s)  # Start time in seconds since epoch
    local start_time=$(echo "$execution_end - $timeout" | bc)
    # Convert times to formatted strings
    local formatted_start_time=$(date -d @$start_time +"%Y-%m-%d-%H:%M:%S")
    local formatted_execution_end=$(date -d @$execution_end +"%Y-%m-%d-%H:%M:%S")

    # Calculate durations
    local download_duration=$(echo "$execution_end - $start_time" | bc)
    local execution_duration=$(echo "$execution_end - $download_duration" | bc)
    local total_duration=$(echo "$execution_end - $start_time" | bc)

    # Construct the log entry
    local log_entry="${formatted_start_time},"
    log_entry+="${formatted_execution_end},"
    log_entry+="${query_name},"
    log_entry+="${scaling_factor},"
    log_entry+="${format},"
    log_entry+="${engine,,},"
    log_entry+="None,"
    log_entry+="None,"
    log_entry+="${timeout},"
    log_entry+="None,"
    log_entry+="None,"
    log_entry+="None,"
    log_entry+="None,"
    log_entry+="None,"
    log_entry+="True"
    # Write the log entry to the specified logfile
    echo "$log_entry" >> "$logfile"
}

for executor in ${executors[@]}; do
    conda activate ${environment_dict[${executor}]};
    for query in ${tasks[@]}; do
        for sfi in ${sf[@]}; do
            echo 'WARMUP STARTED'
            for ((i=0; i<$warmup; i++)); do
                if [[ "${executor}" == "Pandas" && ${#sfi} -gt 6 ]]; then
                    echo "SKIPPED"
                    echo "Executor:  ${executor}, Query: ${query}, Scaling-factor: ${sfi}, Run: ${i}"
                else
                    echo "Executor:  ${executor}, Query: ${query}, Scaling-factor: ${sfi}, Run: ${i}"
                    timeout ${timeout} python3 runScript.py --modin_flag Bash --query ${query} --sf ${sfi} \
                            --format ${format} --executor ${executor} --timeout ${timeout}
                    if [ $? -eq 124 ]; then
                        # The script timed out
                        echo -e '\n\n'
                        echo "SCRIPT TIMED OUT! timeout: $timeout seconds"
                        echo "SKIPPING THE NEXT SCALING FACTORS ..."
                        echo "+++++++++++++++++++++++++++++"
                        break 2
                    fi
                fi
            done
            echo "WARMUP FINISHED"
            echo "MEASURE STARTED"
            for ((i=0; i<$measure; i++)); do
                if [[ "${executor}" == "Pandas" && ${#sfi} -gt 6 ]]; then
                    echo "SKIPPED"
                    echo "Executor:  ${executor}, Query: ${query}, Scaling-factor: ${sfi}, Run: ${i}"
                else
                    echo "Executor:  ${executor}, Query: ${query}, Scaling-factor: ${sfi}, Run: ${i}"
                    timeout ${timeout} python3 runScript.py --modin_flag Bash --query ${query} --sf ${sfi} \
                            --format ${format} --executor ${executor} --logfile ${logfile} --timeout ${timeout}
                    if [ $? -eq 124 ]; then
                        # The script timed out
                        log_execution_details ${logfile} ${query} ${sfi} ${format} ${executor}
                        log_timeout_error ${query} ${sfi} ${executor}
                        echo -e '\n\n'
                        echo "SCRIPT TIMED OUT! timeout: $timeout seconds"
                        echo "SKIPPING THE NEXT SCALING FACTORS ..."
                        echo "+++++++++++++++++++++++++++++"
                        break 2
                    fi
                fi
            done
            echo "MEASURE ENDED"
        done
    done
    conda deactivate
done

#python3 spaceShip/space_benchmark.py --modin_flag Fetch --logfile ${logfile}
