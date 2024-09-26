import sys
import os
import json
import snowflake

import ray

import argparse
parser = argparse.ArgumentParser(description="Parsing is used to run modin configurations")
parser.add_argument('--modin_flag', help='Whether the benchmark is set to be run in modin mode')
parser.add_argument('--query', help='The query modin should run')
parser.add_argument('--sf' ,help='Scaling factor')
parser.add_argument('--format' ,help='The data format used' )

import time
import subprocess
import signal

import s3fs
from snowflake.snowpark import Session
import modin.pandas as mpd

from credentials import S3_BUCKET, SNOWFLAKE_CONNECTION_DICT, WAREHOUSE_NAME,\
    DIRECTORY_PATH_RAY, S3_ACCESS_KEY, S3_SECRET_KEY

TIMEOUT = 60 * 10


class TimeoutException(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutException

signal.signal(signal.SIGALRM, timeout_handler)

def get_data_paths(
        engine: str = None,
        data_format: str = "parquet",
        scaling_factor: str = "SF1"
):
    s3 = s3fs.S3FileSystem(anon=False)
    glob_path = os.path.join(S3_BUCKET, scaling_factor, "lineorder", "*.{}".format(data_format))
    print(glob_path)
    prefix = "s3://" if engine != "spark" else "s3a://"
    file_prefix = prefix + S3_BUCKET + "/{}/".format(scaling_factor)
    lineorder_files = s3.glob(glob_path)
    lineorder_files = [prefix + filename for filename in lineorder_files]
    return file_prefix, lineorder_files


class Executor():

    def __init__(self,
                 scaling_factor: int,
                 format: str,
                 logfile: str,
                 engine: str = "standart",
                 number_runs: int = 3
                 ):
        self.logfile = logfile
        self.scaling_factor = scaling_factor
        self.engine = engine
        self.number_runs = number_runs
        self.file_prefix, self.lineorder_files = get_data_paths(data_format=format, scaling_factor=scaling_factor,
                                                                engine=engine)
        print("Self file prefix", self.file_prefix)
        self.filename_dict = {"date": self.file_prefix + "date.{}".format(format),
                              "supplier": self.file_prefix + "supplier.{}".format(format),
                              "customer": self.file_prefix + "customer.{}".format(format),
                              "part": self.file_prefix + "part.{}".format(format),
                              "lineorder": self.lineorder_files}
        self.module = None

    def shutdown(self):
        pass

    def setup(self):
        pass

    def measure(self):
        self.setup()
        start_time = time.time()
        self.query.download()
        download_end = time.time()
        signal.alarm(TIMEOUT)
        try:
            self.query.execute()
            signal.alarm(0)
        except TimeoutException:
            print("function terminated")
        execution_end = time.time()
        with open(self.logfile, 'a') as fd:
            queryID = "None"
            if self.engine == "snowpandas":
                queryID = \
                self.query.lineorder._query_compiler._modin_frame._sf_session.sql("SELECT LAST_QUERY_ID()").collect()[0][0]
            fd.write((f"{self.query.name},"
                      f"{self.scaling_factor},"
                      f"{self.format},"
                      f"{self.engine},"
                      f"{str(download_end - start_time)},"
                      f"{str(execution_end - download_end)},"
                      f"{str(execution_end - start_time)},"
                      f"{queryID},"
                      f"{str('None')},"
                      f"{str('None')}\n"
                      ))

        self.shutdown()


class PandasExecutor(Executor):

    def __init__(self,
                 query,
                 format,
                 scaling_factor,
                 logfile):
        super().__init__(scaling_factor, format, logfile, engine="pandas")
        self.query = query
        self.format = format
        import pandas as pd
        def list_reader_func(filename_list):
            return pd.concat([self.query.reader_func(filename) for filename in filename_list])

        def reader_func(filename):
            if format == "csv":
                return pd.read_csv(filename)
            else:
                return pd.read_parquet(filename)

        self.module = pd
        self.query.module = self.module
        self.query.reader_func = reader_func
        self.query.list_reader_func = list_reader_func
        self.query.filename_dict = self.filename_dict


class ModinExecutor(Executor):
    def __init__(self,
                 query,
                 format,
                 scaling_factor,
                 logfile):
        super().__init__(scaling_factor, format, logfile, engine="modin")
        self.query = query
        self.format = format

        import modin.pandas as pd
        import modin.config as cfg
        import ray


        import importlib
        importlib.reload(pd)
        importlib.reload(cfg)
        importlib.reload(ray)
        #os.environ["RAY_memory_monitor_refresh_ms"] = "0"
        ray.init(
            num_cpus=48,
            ignore_reinit_error=True,
            _system_config={
                "object_spilling_config": json.dumps(
                    {
                        "type": "filesystem",
                        "params": {
                            "directory_path": DIRECTORY_PATH_RAY,
                            "buffer_size": 10_000_000,
                        }
                    },
                )
            },
        )
        #os.environ["MODIN_MEMORY"] = "8000000000"
        #os.environ["MODIN_CPUS"] = "8"
        self.module = mpd
        self.ray = ray
        def list_reader_func(filename_list):
            return self.module.concat([self.query.reader_func(filename) for filename in filename_list])
        def reader_func(filename):
            if format == "csv":
                return self.module.read_csv(filename)
            else:
                return self.module.read_parquet(filename)
        self.query.module = self.module
        self.query.reader_func = reader_func
        self.query.list_reader_func = list_reader_func
        self.query.filename_dict = self.filename_dict

    def shutdown(self):
        self.ray.shutdown()
        pass

class SnowpandasExecutor(Executor):
    def __init__(self,
                 query,
                 format,
                 scaling_factor,
                 logfile):
        super().__init__(scaling_factor, format, logfile, engine="snowpandas")
        self.query = query
        self.format = format
        import modin.pandas as pd
        import modin.config as cfg
        import ray
        ray.init(
            num_cpus=96,
            ignore_reinit_error=True,
            _system_config={
                "object_spilling_config": json.dumps(
                    {
                        "type": "filesystem",
                        "params": {
                            "directory_path": DIRECTORY_PATH_RAY,
                            "buffer_size": 10_000_000,
                        }
                    },
                )
            },
        )

        self.sf_db_name = "SSB"
        self.sf_warehouse_name = WAREHOUSE_NAME

        cfg.LogMode.disable()
        cfg.IsExperimental.put(True)
        cfg.SnowFlakeConnectionParameters.put(SNOWFLAKE_CONNECTION_DICT)
        cfg.SnowFlakeDatabaseName.put(self.sf_db_name)
        cfg.SnowFlakeWarehouseName.put(self.sf_warehouse_name)

        def list_reader_func(filename_list):
            return pd.from_sf_table("SSB_{}.LINEORDER".format(scaling_factor))

        def reader_func(filename):
            if "lineorder" in filename:
                return pd.from_sf_table("SSB_{}.LINEORDER".format(scaling_factor))
            elif "date" in filename:
                return pd.from_sf_table("SSB_{}.DATE".format(scaling_factor))
            elif "customer" in filename:
                return pd.from_sf_table("SSB_{}.CUSTOMER".format(scaling_factor))
            elif "part" in filename:
                return pd.from_sf_table("SSB_{}.PART".format(scaling_factor))
            elif "supplier" in filename:
                return pd.from_sf_table("SSB_{}.SUPPLIER".format(scaling_factor))

        self.module = pd
        self.query.module = self.module
        self.query.reader_func = reader_func
        self.query.list_reader_func = list_reader_func
        self.query.filename_dict = self.filename_dict
    def setup(self):
        subprocess.Popen(["bash", 'suspend.sh', str(self.sf_warehouse_name)]).wait()


class SparkExecutor(Executor):
    def __init__(self,
                 query,
                 format,
                 scaling_factor,
                 logfile):
        super().__init__(scaling_factor, format, logfile, engine="spark")
        self.name = "spark"
        self.query = query
        self.format = format


        import findspark
        findspark.init()
        import pyspark.pandas as pd
        from pyspark import SparkConf
        from pyspark.sql import SparkSession

        os.environ['PYSPARK_SUBMIT_ARGS'] = f'--driver-memory 180g --executor-memory 6g --executor-cores 48 --conf spark.local.dir={DIRECTORY_PATH_RAY} pyspark-shell'

        conf = SparkConf() \
            .setAppName("Connect AWS") \
            .setMaster("local[*]")

        conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2")
        spark = SparkSession \
            .builder \
            .config(conf=conf) \
            .getOrCreate()

        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", S3_ACCESS_KEY)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", S3_SECRET_KEY)


        def list_reader_func(filename_list):
            return pd.concat([self.query.reader_func(filename) for filename in filename_list])

        def reader_func(filename):
            if format == "csv":
                return spark.read.format("csv").option("header", True).load(filename).pandas_api()
            else:
                return spark.read.format("parquet").option("header", True).load(filename).pandas_api()

        self.module = pd
        self.query.module = self.module
        self.query.reader_func = reader_func
        self.query.list_reader_func = list_reader_func
        self.query.filename_dict = self.filename_dict


class Query():
    def __init__(self):
        self.module = None
        self.reader_func = None
        self.list_reader_func = None
        self.filename_dict = None
        self.res = "not assigned yet"


class SelectionQuery(Query):
    def __init__(self):
        super().__init__()
        self.name = "selection"

    def download(self):
        self.lineorder = self.list_reader_func(self.filename_dict["lineorder"])
        #print("Size of df: ", sys.getsizeof(self.lineorder))
    def execute(self):
        self.res = self.lineorder[["LO_COMMITDATE", "LO_DISCOUNT", "LO_ORDERPRIORITY"]]
        print(self.res)

class FilterQuery(Query):
    def __init__(self):
        super().__init__()
        self.name = "filter"
    def download(self):
        self.lineorder = self.list_reader_func(self.filename_dict["lineorder"])

    def execute(self):
        self.res = self.lineorder.loc[self.lineorder['LO_QUANTITY'] < 24]
        print(self.res)

class JoinQuery(Query):
    def __init__(self):
        super().__init__()
        self.name = "join"
    def download(self):
        self.lineorder = self.list_reader_func(self.filename_dict["lineorder"])
        self.date = self.reader_func(self.filename_dict["date"])
        self.part = self.reader_func(self.filename_dict["part"])
        self.customer = self.reader_func(self.filename_dict["customer"])
        self.supplier = self.reader_func(self.filename_dict["supplier"])
    def execute(self):
        self.res = self.lineorder.set_index('LO_ORDERDATE').join(self.date.set_index('D_DATEKEY'))
        self.res = self.res.set_index('LO_CUSTKEY').join(self.customer.set_index('C_CUSTKEY'))
        self.res = self.res.set_index('LO_PARTKEY').join(self.part.set_index('P_PARTKEY'))
        self.res = self.res.set_index('LO_SUPPKEY').join(self.supplier.set_index('S_SUPPKEY'))
        print(self.res)


class AggSumQuery(Query):
    def __init__(self):
        super().__init__()
        self.name = "agg"

    def download(self):
        self.lineorder = self.list_reader_func(self.filename_dict["lineorder"])
        #self.lineorder_cut = self.lineorder[:500000]
        self.lineorder_cut = self.lineorder[["LO_CUSTKEY", "LO_ORDERPRIORITY", "LO_REVENUE"]]

    def execute(self):
        self.res = self.lineorder_cut.sum()
        print(self.res)

class GroupByQuery(Query):
    def __init__(self):
        super().__init__()
        self.name = "groupby"

    def download(self):
        self.lineorder = self.list_reader_func(self.filename_dict["lineorder"])
        #self.lineorder_cut = self.lineorder[:500000]
        self.lineorder_cut = self.lineorder[["LO_CUSTKEY", "LO_ORDERPRIORITY", "LO_REVENUE"]]

    def execute(self):
        self.res = self.lineorder_cut.groupby(["LO_CUSTKEY", "LO_ORDERPRIORITY"], as_index=True).agg({'LO_REVENUE': 'sum'})
        print(self.res)

class SortQuery(Query):
    def __init__(self):
        super().__init__()
        self.name = "sort"

    def download(self):
        self.lineorder = self.list_reader_func(self.filename_dict["lineorder"])

    def execute(self):
        self.res = self.lineorder.sort_values(by=['LO_ORDERPRIORITY', 'LO_REVENUE'], ascending=[True, False])
        print(self.res)

class ColumOpAssignmentQuery(Query):
    def __init__(self):
        super().__init__()
        self.name = "columnopassignment"

    def download(self):
        self.lineorder = self.list_reader_func(self.filename_dict["lineorder"])

    def execute(self):
        self.res = self.lineorder
        self.res["PROFIT"] = self.res["LO_REVENUE"] - self.res["LO_DISCOUNT"]
        print(self.res)


def fetch_snowflake_metrics(logfile):
    warehouse = WAREHOUSE_NAME
    session = Session.builder.configs(SNOWFLAKE_CONNECTION_DICT).create()
    session.use_warehouse(warehouse)
    f = open(logfile)
    lines = f.readlines()
    f.close()
    out = open(logfile, "w")
    for item in lines:
        line = item.split(',')

        if line[7] != "None" and line[7] != "queryID":
            try:
                metrics = session.sql("SELECT * FROM snowflake.account_usage.query_history WHERE QUERY_ID = '{}';".format(line[7])).collect()[0].as_dict()
                line[8] = str(metrics['EXECUTION_TIME'])
                line[9] = str(metrics['COMPILATION_TIME'])
                line = ",".join(line) + "\n"
            except:
                line = ",".join(line)
        else:
            line = ",".join(line)
        out.write(line)
    out.close()

def main():
    args = parser.parse_args()
    logfile = "results/experiment.log.csv"


    if args.modin_flag =="True":
        print(args.query)
        query_from_arg = eval(args.query)
        ex = ModinExecutor(query_from_arg(), args.format, args.sf, logfile)
        print(f"Executor= {type(ex)} ; Query= {type(ex.query)}")
        ex.measure()
        return

    if not os.path.exists(logfile):
        with open(logfile, 'a') as fd:
            fd.write(
                "functionName,scalingFactor,dataformat,engine,downloadTime,executionTime,totalTime,queryID,sf_execuionTime,"
                "sf_compilationTime\n")

    number_of_runs = 1
    scaling_factors = ["SF1"]
    data_formats = ["parquet"]
    executor_list =[SnowpandasExecutor] #[ SparkExecutor] #[ ModinExecutor, SnowpandasExecutor, SparkExecutor]
    query_list = [SelectionQuery, FilterQuery, SortQuery, GroupByQuery, ColumOpAssignmentQuery, JoinQuery]
    for data_format in data_formats:
        for scaling_factor in scaling_factors:
            for executor in executor_list:
                for query in query_list:
                    for i in range(number_of_runs):
                        ex = executor(query(), data_format, scaling_factor,  logfile)
                        print(f"Executor= {type(ex)} ; Query= {type(ex.query)}")
                        ex.measure()
    time.sleep(180)
    fetch_snowflake_metrics(logfile)


if __name__ == '__main__':
    main()
