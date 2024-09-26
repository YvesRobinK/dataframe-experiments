import time, os
import subprocess
import signal
from datetime import datetime
import traceback

from tasks.task import Task
from credentials import S3_BUCKET, S3_BUCKET_SSB, S3_BUCKET_SPACESHIP, S3_BUCKET_HOUSEPRICE
from ioReader import IOParser

import s3fs


class TimeoutException(Exception):
    pass

class TerminationException(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutException

signal.signal(signal.SIGALRM, timeout_handler)

def handle_terimination_signal(signum, frame):
    raise TerminationException

# signal.signal(signal.SIGTERM, handle_terimination_signal)

def get_data_paths(
        engine: str = None,
        data_format: str = "parquet",
        scaling_factor: str = "SF1",
        query_name: str = 'Fork',
):
    s3 = s3fs.S3FileSystem(anon=False)
    if query_name in {'Fork', 'Endeavor', 'Catboost'}: # spaceship data set
        s3_bucket = S3_BUCKET_SPACESHIP
    elif query_name in {'HousePricePrediction'}: # house prediction data set
        s3_bucket = S3_BUCKET_HOUSEPRICE
    elif 'ssb' in query_name: # SSB data set
        s3_bucket = S3_BUCKET_SSB
    else:
        s3_bucket = S3_BUCKET

    glob_path_train = os.path.join(s3_bucket, scaling_factor, "train_h.{}/*".format(data_format))
    glob_path_test = os.path.join(s3_bucket, scaling_factor, "test_h.{}/*".format(data_format))
    train_paths = s3.glob(glob_path_train)
    test_paths = s3.glob(glob_path_test)
    prefix = "s3://" if engine != "spark" else "s3a://"

    train_paths = [prefix + x for x in train_paths]
    test_paths = [prefix + x for x in test_paths]
    return test_paths, train_paths

def get_ssb_paths(
        engine: str = None,
        data_format: str = "parquet",
        scaling_factor: str = "SF1"
):
    s3 = s3fs.S3FileSystem(anon=False)
    glob_path = os.path.join(S3_BUCKET_SSB, scaling_factor, "*.{}".format(data_format))
    glob_path_lineorder = os.path.join(S3_BUCKET_SSB, scaling_factor, "lineorder/*.{}".format(data_format))

    paths = s3.glob(glob_path)
    lineorder_paths = s3.glob(glob_path_lineorder)

    prefix = "s3://" if engine != "spark" else "s3a://"

    paths = [prefix + x for x in paths]
    paths_dict = {}
    for path in paths:
        if "date" in path:
            paths_dict["date"] = path
        elif "customer" in path:
            paths_dict["customer"] = path
        elif "supplier" in path:
            paths_dict["supplier"] = path
        elif "part" in path:
            paths_dict["part"] = path
    lineorder_paths = [prefix + x for x in lineorder_paths]
    return paths_dict, lineorder_paths


class Executor():
    def __init__(self,
                 query: Task,
                 scaling_factor: int,
                 format: str,
                 logfile: str,
                 engine: str = "standart",
                 number_runs: int = 3,
                 ):
        self.logfile = logfile
        self.logging = True if logfile else False
        self.scaling_factor = scaling_factor
        self.engine = engine
        self.query = query
        self.query.engine = self.engine

        self.format = format
        self.query.executor = self


        self.number_runs = number_runs
        self.test_paths, self.train_paths = get_data_paths(data_format=format,
                                                            scaling_factor=scaling_factor,
                                                            engine=engine,
                                                            query_name=self.query.name)
        self.filename_dict = {"train": self.train_paths,
                              "test": self.test_paths
                              }
        self.ssb_paths , self.lineorder_paths = get_ssb_paths(data_format=format, scaling_factor=scaling_factor, engine=engine)
        self.filename_dict["ssb"] = {
            "paths" : self.ssb_paths,
            "lineorder_paths": self.lineorder_paths
        }
        self.query.filename_dict = self.filename_dict
        self.module = None
        self.query.reader_func = self.reader_func
        self.query.list_reader_func = self.list_reader_func
        self.io_parser = IOParser()
        print("Filename dict: ", self.query.filename_dict)

    def list_reader_func(self, filename_list):
        return self.module.concat([self.reader_func(filename) for filename in filename_list])

    def reader_func(self, filename):
        if self.format == "csv":
            return self.module.read_csv(filename)
        else:
            return self.module.read_parquet(filename)

    def shutdown(self):
        if self.logging:
            self.monitor_process.terminate()

    def setup(self):
        self.start_time = time.time()
        if self.logging:
            formatted_timestamp = datetime.fromtimestamp(self.start_time).strftime('%Y-%m-%d-%H:%M:%S')
            monitoring_file_name = f'results/cpu_usage_monitoring/{self.engine}/{self.query.name}_{self.scaling_factor}_{formatted_timestamp}.csv'
            self.monitor_process = subprocess.Popen(['python3', 'monitor.py', '-p', f'{os.getpid()}', '-f', f'{monitoring_file_name}'])
            self.io_parser.set_up()

    def save_result(self):
        if not self.logging:
            return
        if self.scaling_factor != "SF1" and self.scaling_factor != "SF0":
            return
        if "ssb" in self.query.name:
            self.query.result = self.query.joined
        else:
            self.query.result = self.query.train

        df = self.query.result

        if self.scaling_factor == "SF1" or self.scaling_factor == "SF0":
            file_name = f"results/compare/{self.engine}/{self.query.name}.csv"
            if self.engine == "polars":
                df.collect().to_pandas().to_csv(file_name)
            elif self.engine == "snowpandas":
                df._query_compiler._modin_frame.to_pandas().to_csv(file_name)
            elif self.engine == "spark":
               df.to_pandas().to_csv(file_name)
            elif self.engine == "vaex":
                df.to_pandas_df().to_csv(file_name)
            elif self.engine == "dask":
                df.compute().to_csv(file_name)
            else:
                df.to_csv(file_name)

    @property
    def timeout(self):
        return int(os.environ["TIMEOUT"])

    def log(self, start_time, download_end, execution_end, error):
        if not self.logging:
            return

        if not os.path.exists(self.logfile):
            with open(self.logfile, 'a') as fd:
                header = "start_time," +\
                        "execution_end," +\
                        "query_name," +\
                        "scaling_factor," +\
                        "dataformat," +\
                        "engine," +\
                        "download_time," +\
                        "execution_time," +\
                        "total_time," +\
                        "query_id," +\
                        "bytes_received," +\
                        "bytes_sent," +\
                        "sf_execuion_time," +\
                        "sf_compilation_time," +\
                        "error\n"
                fd.write(header)

        if self.engine == "snowpandas":
            query_id = self.query.train._query_compiler._modin_frame._sf_session.sql("""
                                WITH recent_queries AS (
                                SELECT
                                QUERY_ID,
                                ROW_NUMBER() OVER (ORDER BY START_TIME DESC) AS rn
                                FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
                                )
                                SELECT QUERY_ID
                                FROM recent_queries
                                WHERE rn = 3""").collect()[0][0]
            #query_id = self.query.train._query_compiler._modin_frame._sf_session.sql("SELECT LAST_QUERY_ID()").collect()[0][0]
        else:
            query_id = 'None'

        bytes_received, bytes_sent = self.io_parser.get_result()
        with open(self.logfile, 'a') as fd:
            fd.write((
                    f"{datetime.fromtimestamp(start_time).strftime('%Y-%m-%d-%H:%M:%S')},"
                    f"{datetime.fromtimestamp(execution_end).strftime('%Y-%m-%d-%H:%M:%S')},"
                    f"{self.query.name},"
                    f"{self.scaling_factor},"
                    f"{self.format},"
                    f"{self.engine},"
                    f"{str(download_end - start_time)},"
                    f"{str(execution_end - download_end)},"
                    f"{str(execution_end - start_time)},"
                    f"{query_id},"
                    f"{bytes_received},"
                    f"{bytes_sent},"
                    f"{'None'},"
                    f"{'None'},"
                    f"{error}\n"
                ))

    def log_exception(self, start_time):
        if not self.logging:
            return

        with open('results/error_log.txt', 'a') as f:
            query_info = f"start time: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d-%H:%M:%S')},\n" +\
                        f"query name: {self.query.name},\n" +\
                        f"scaling factor: {self.scaling_factor},\n" +\
                        f"engine: {self.engine},\n" +\
                        f"error:\n"
            f.write(query_info)
            traceback.print_exc(file=f)
            f.write('==================================\n\n')

    def log_timeout_exception(self, start_time):
        if not self.logging:
            return

        with open('results/error_log.txt', 'a') as f:
            query_info = f"start time: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d-%H:%M:%S')},\n" +\
                        f"query name: {self.query.name},\n" +\
                        f"scaling factor: {self.scaling_factor},\n" +\
                        f"engine: {self.engine},\n" +\
                        f"error: TIMEOUT {self.timeout} seconds exceeded\n"
            f.write(query_info)
            f.write('==================================\n\n')

    def measure(self):
        self.setup()
        print('start: ', self.start_time)
        self.query.download()
        download_end = time.time()
        print('download end: ', download_end)
        timeout = self.timeout
        print(f'Timeout is {timeout} seconds')
        signal.alarm(timeout)
        try:
            self.query.execute()
            error = False
            signal.alarm(0)
        except TimeoutException:
            print(f"Execution terminated due to exceeding time limit: {timeout} seconds")
            self.log_timeout_exception(self.start_time)
            error = True
        except Exception as e:
            print('\n****\nExecution failed!!!\n****\n')
            self.log_exception(self.start_time)
            error = True

        execution_end = time.time()
        print('execution end: ',  execution_end)

        self.log(self.start_time, download_end, execution_end, error)
        self.save_result()
        self.shutdown()
