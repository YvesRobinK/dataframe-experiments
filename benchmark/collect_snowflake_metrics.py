import argparse
from datetime import datetime
import pytz
import csv
import os

from snowflake.snowpark import Session
import pandas as pd

from credentials import WAREHOUSE_NAME, SNOWFLAKE_CONNECTION_DICT

SESSION = Session.builder.configs(SNOWFLAKE_CONNECTION_DICT).create()
SESSION.use_warehouse(WAREHOUSE_NAME)
ALL_METRICS_LOG_FILE = 'results/all_snowflake_metrics.csv'
DEFAULT_LOG_FILE = 'results/res_sp.csv'

def dump_all_snowflake_metrics(metrics: list):
    file_exists = os.path.isfile(ALL_METRICS_LOG_FILE)

    with open(ALL_METRICS_LOG_FILE, mode='a', newline='') as file:
        csv_writer = csv.DictWriter(file, fieldnames=metrics[0].as_dict().keys())
        if not file_exists:
            csv_writer.writeheader()

        for metric in metrics:
            metric_dict = metric.as_dict()
            csv_writer.writerow(metric_dict)

def collect_snowpandas_metrics(df: pd.DataFrame):
    def set_time(row):
        query_id = row['query_id']
        try:
            metrics = SESSION.sql(f"SELECT * FROM snowflake.account_usage.query_history WHERE query_id = '{query_id}';").collect()
            dump_all_snowflake_metrics(metrics)
            metrics = metrics[0]
            sf_execuion_time = int(metrics['EXECUTION_TIME']) / 1000 # convert to seconds
            sf_compilation_time = int(metrics['COMPILATION_TIME']) / 1000 # convert to seconds
            sf_scanned_bytes = int(metrics['BYTES_SCANNED'])  # convert to seconds
            print(sf_scanned_bytes)
            row['bytes_received'] = sf_scanned_bytes
            row['sf_execuion_time'] = sf_execuion_time
            row['sf_compilation_time'] = sf_compilation_time
        except Exception as e:
            print(e)
            print(f'Metrics for query id {query_id} could not be retrieved! try again later ...')
        return row

    df = df.apply(set_time, axis=1)
    return df

def get_current_timezone():
    with open('/etc/timezone', 'r') as f:
        timezone = f.read().strip()
        timezone = "UTC"
    return timezone

def convert_timezone(datetime_str: str, dst_timezone='America/Los_Angeles'):
    ###
    # default is set to California, because the default timezone of snowflake is california
    # for more info see here: https://stackoverflow.com/questions/71378174/snowflake-timezone
    ###
    source_timezone = pytz.timezone(get_current_timezone())
    datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%d-%H:%M:%S')
    localized_datetime = source_timezone.localize(datetime_obj)
    target_timezone = pytz.timezone(dst_timezone)
    california_time = localized_datetime.astimezone(target_timezone)
    
    return california_time

def collect_snowparkpandas_metrics(df: pd.DataFrame):
    def set_time(row):
        start_time = row['start_time']
        execution_end = row['execution_end']
        start_time = convert_timezone(start_time)
        from datetime import datetime, timedelta

        execution_end = convert_timezone(execution_end)
        execution_end = execution_end + timedelta(seconds=2)
        print("Start time: ", start_time, " ;End time: ", execution_end)
        try:
            metrics = SESSION.sql(f"""
                            SELECT *
                            FROM TABLE(snowflake.information_schema.query_history(
                                            end_time_range_start => TO_TIMESTAMP_LTZ('{start_time}'),
                                            end_time_range_end => TO_TIMESTAMP_LTZ('{execution_end}')
                                        ))
                            WHERE USER_NAME = CURRENT_USER()
                            ORDER BY start_time;
                            """).collect()
            dump_all_snowflake_metrics(metrics)
            sf_execuion_time, sf_compilation_time, sf_scanned_bytes = 0, 0, 0
            for m in metrics:
                print(m)
                if m['QUERY_TYPE'] == 'CREATE_TABLE' or m['QUERY_TYPE'] == 'SELECT':
                    sf_execuion_time += int(m['EXECUTION_TIME']) / 1000 # convert to seconds
                    sf_compilation_time += int(m['COMPILATION_TIME']) / 1000 # convert to seconds
                    sf_scanned_bytes += int(m['BYTES_SCANNED']) # convert to seconds
            row['sf_execuion_time'] = sf_execuion_time
            row['sf_compilation_time'] = sf_compilation_time
            print(sf_scanned_bytes)
            row['bytes_received'] = sf_scanned_bytes
        except:
            print(f"Metrics for the snowparkpandas execution index: {row['index']} could not be retrieved!")
        return row

    df = df.apply(set_time, axis=1)
    return df

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('-file', help='main log file. Please provide the path relative to the benchmark folder', type=str, default=DEFAULT_LOG_FILE)
    args = parser.parse_args()
    logfile = args.file
    log_df = pd.read_csv(logfile)
    log_df = log_df[log_df['error'] == False]

    snowpandas_df = log_df[log_df['engine'] == 'snowpandas']
    snowpandas_df = snowpandas_df[snowpandas_df['sf_execuion_time'].isna()]

    snowparkpandas_df = log_df[log_df['engine'] == 'snowparkpandas']
    snowparkpandas_df = snowparkpandas_df[snowparkpandas_df['sf_execuion_time'].isna()]

    snowpandas_df = collect_snowpandas_metrics(snowpandas_df)
    snowparkpandas_df = collect_snowparkpandas_metrics(snowparkpandas_df)
    log_df.update(snowpandas_df)
    log_df.update(snowparkpandas_df)
    print(log_df)
    log_df.to_csv(logfile, index=False)
