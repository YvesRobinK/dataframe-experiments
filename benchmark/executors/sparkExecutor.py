import os

from .executor import Executor
from credentials import S3_ACCESS_KEY, S3_SECRET_KEY, DIRECTORY_PATH_RAY

class SparkExecutor(Executor):
    def __init__(self,
                 query,
                 format,
                 scaling_factor,
                 logfile):
        super().__init__(query, scaling_factor, format, logfile, engine="spark")

        import findspark
        findspark.init()

        import pyspark.pandas as pd
        from pyspark import SparkConf
        from pyspark.sql import SparkSession
        os.environ['PYSPARK_SUBMIT_ARGS'] = f'--driver-memory 248g --executor-memory 6g --executor-cores 64 --conf spark.local.dir={DIRECTORY_PATH_RAY}  pyspark-shell'
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
        self.spark = spark
        pd.set_option('compute.ops_on_diff_frames', True)

        self.module = pd

    def reader_func(self, filename):
        if self.format == "csv":
            return self.spark.read.format("csv").option("header", True).load(filename).pandas_api()
        else:
            return self.spark.read.format("parquet").option("header", True).load(filename).pandas_api()
