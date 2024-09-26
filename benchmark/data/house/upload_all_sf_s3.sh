#! /bin/bash

python3 ./../../upload_data_to_s3.py --d house --o csv --sf 0 --is_snowflake_data False
python3 ./../../upload_data_to_s3.py --d house --o parquet --sf 0 --is_snowflake_data False
python3 ./../../upload_data_to_s3.py --d house --o csv --sf 0 --is_snowflake_data True

python3 ./../../upload_data_to_s3.py --d house --o csv --sf 4 --is_snowflake_data False
python3 ./../../upload_data_to_s3.py --d house --o parquet --sf 4 --is_snowflake_data False
python3 ./../../upload_data_to_s3.py --d house --o csv --sf 4 --is_snowflake_data True

python3 ./../../upload_data_to_s3.py --d house --o csv --sf 8 --is_snowflake_data False
python3 ./../../upload_data_to_s3.py --d house --o parquet --sf 8 --is_snowflake_data False
python3 ./../../upload_data_to_s3.py --d house --o csv --sf 8 --is_snowflake_data True

python3 ./../../upload_data_to_s3.py --d house --o csv --sf 12 --is_snowflake_data False
python3 ./../../upload_data_to_s3.py --d house --o parquet --sf 12 --is_snowflake_data False
python3 ./../../upload_data_to_s3.py --d house --o csv --sf 12 --is_snowflake_data True

python3 ./../../upload_data_to_s3.py --d house --o parquet --sf 14 --is_snowflake_data True

SPACE_FACTORS=(15 18 20 21 22)
bucket=$(jq -r '.S3_BUCKET_HOUSEPRICE' ./../../../credentials.json)
PATH_SPACE_H_CSV="s3://${bucket}/SF12/train_h.csv/train_h.csv"
PATH_SPACE_CSV="s3://${bucket}/SF12/train.csv/train.csv"

for factor in ${SPACE_FACTORS[@]}; do
  r=$((2**($factor-12)))
  for i in $(seq 0 $(($r-1))); do
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

SPACE_FACTORS=(15 18 20 21 22)

PATH_SPACE_H_PARQUET="s3://${bucket}/SF14/train_h.parquet"

for factor in ${SPACE_FACTORS[@]}; do
  r=$((2**($factor-14)))
  for i in $(seq 0 $(($r-1))); do
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
