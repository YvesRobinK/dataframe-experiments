#! /bin/bash

python3 ./../../upload_data_to_s3.py --d space --o csv --sf 0 --is_snowflake_data False
python3 ./../../upload_data_to_s3.py --d space --o parquet --sf 0 --is_snowflake_data False
python3 ./../../upload_data_to_s3.py --d space --o csv --sf 0 --is_snowflake_data True

python3 ./../../upload_data_to_s3.py --d space --o csv --sf 5 --is_snowflake_data False
python3 ./../../upload_data_to_s3.py --d space --o parquet --sf 5 --is_snowflake_data False
python3 ./../../upload_data_to_s3.py --d space --o csv --sf 5 --is_snowflake_data True

python3 ./../../upload_data_to_s3.py --d space --o csv --sf 10 --is_snowflake_data False
python3 ./../../upload_data_to_s3.py --d space --o parquet --sf 10 --is_snowflake_data False
python3 ./../../upload_data_to_s3.py --d space --o csv --sf 10 --is_snowflake_data True

python3 ./../../upload_data_to_s3.py --d space --o csv --sf 11 --is_snowflake_data False
python3 ./../../upload_data_to_s3.py --d space --o parquet --sf 11 --is_snowflake_data False
python3 ./../../upload_data_to_s3.py --d space --o csv --sf 11 --is_snowflake_data True

python3 ./../../upload_data_to_s3.py --d space --o parquet --sf 14 --is_snowflake_data False

SPACE_FACTORS=(13 15 17 19 20 21)

bucket=$(jq -r '.S3_BUCKET_SPACESHIP' ./../../../credentials.json)
PATH_SPACE_H_CSV="s3://${bucket}/SF11/train_h.csv/train_h.csv"
PATH_SPACE_CSV="s3://${bucket}/SF11/train.csv/train.csv"

for factor in ${SPACE_FACTORS[@]}; do
  r=$((2**($factor-11)))
  for i in $(seq 0 $(($r-1))); do  # Add 'do' here and use 'seq' for generating the sequence
    aws s3 cp \
        "$PATH_SPACE_H_CSV" \
        "s3://${bucket}/SF${factor}/train_h.csv/train_h_${i}.csv" &
    aws s3 cp \
        "$PATH_SPACE_CSV" \
        "s3://${bucket}/SF${factor}/train.csv/train_${i}.csv" &

    num_jobs=$(jobs -r | wc -l)

    if [ "$num_jobs" -gt 50 ]; then
        # Calculate sleep time (increases with the number of jobs)
        sleep_time=10

        echo "There are $num_jobs jobs running. Sleeping for $sleep_time seconds."

        # Sleep for the calculated time
        sleep $sleep_time
        num_jobs=$(jobs -r | wc -l)
        echo "There are $num_jobs jobs running after sleep."
    fi

  done
  wait
done

wait  # Wait for all background processes to complete

SPACE_FACTORS=(15 17 19 20 21)

PATH_SPACE_H_PARQUET="s3://${bucket}/SF14/train_h.parquet/train_h_0.parquet"

for factor in ${SPACE_FACTORS[@]}; do
  r=$((2**($factor-14)))
  for i in $(seq 0 $(($r-1))); do  # Add 'do' here and use 'seq' for generating the sequence
    aws s3 cp \
        "$PATH_SPACE_H_PARQUET" \
        "s3://${bucket}/SF${factor}/train_h.parquet/train_h_${i}.parquet" &
    num_jobs=$(jobs -r | wc -l)

    if [ "$num_jobs" -gt 50 ]; then
        # Calculate sleep time (increases with the number of jobs)
        sleep_time=10

        echo "There are $num_jobs jobs running. Sleeping for $sleep_time seconds."

        # Sleep for the calculated time
        sleep $sleep_time
        num_jobs=$(jobs -r | wc -l)
        echo "There are $num_jobs jobs running after sleep."
    fi
  done
  wait
done

wait  # Wait for all background processes to complete
