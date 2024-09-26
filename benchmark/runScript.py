import os
import sys
import argparse

parser = argparse.ArgumentParser(description="Parsing is used to run modin configurations")
parser.add_argument('--modin_flag', help='Whether the benchmark is set to be run in modin mode')
parser.add_argument('--query', help='The query modin should run')
parser.add_argument('--sf', help='Scaling factor')
parser.add_argument('--format', help='The data format used')
parser.add_argument('--executor', help='The executor used')
parser.add_argument('--logfile', help='The logfile')
parser.add_argument('--timeout', help='time after which timeout will be called')

from executors.snowparkPandasExecutor import SnowparkPandasExecutor
from executors.vaexExecutor import VaexExecutor
from executors.daskExecutor import DaskExecutor
from executors.modinDaskExecutor import ModinDaskExecutor
from executors.modinRayExecutor import ModinRayExecutor
from executors.polarsExecutor import PolarsExecutor
from executors.pandasExecutor import PandasExecutor
from executors.modinExecutor import ModinExecutor
from executors.sparkExecutor import SparkExecutor
from executors.snowpandasExecutor import SnowpandasExecutor

EXECUTOR_DICT = {
    "Pandas": PandasExecutor,
    "Modin": ModinExecutor,
    "Spark" : SparkExecutor,
    "Snowpandas" : SnowpandasExecutor,
    "Polars": PolarsExecutor,
    "ModinRay": ModinRayExecutor,
    "ModinDask": ModinDaskExecutor,
    "Dask": DaskExecutor,
    "Vaex": VaexExecutor,
    "SnowparkPandas": SnowparkPandasExecutor,
}

from tasks.notebookFork import Fork
from tasks.notebookEndeavor import Endeavor
from tasks.notebookCatBoost import CatBoost
from tasks.notebookHousePrice import HousePrice

from tasks.ssb.ssb_q1_1 import Query_1_1
from tasks.ssb.ssb_q1_2 import Query_1_2
from tasks.ssb.ssb_q1_3 import Query_1_3

from tasks.ssb.ssb_q2_1 import Query_2_1
from tasks.ssb.ssb_q2_2 import Query_2_2
from tasks.ssb.ssb_q2_3 import Query_2_3

from tasks.ssb.ssb_q3_1 import Query_3_1
from tasks.ssb.ssb_q3_2 import Query_3_2
from tasks.ssb.ssb_q3_3 import Query_3_3
from tasks.ssb.ssb_q3_4 import Query_3_4

from tasks.ssb.ssb_q4_1 import Query_4_1
from tasks.ssb.ssb_q4_2 import Query_4_2
from tasks.ssb.ssb_q4_3 import Query_4_3

TASK_DICT = {
    "Fork": Fork,
    "Endeavor" : Endeavor,
    "HousePricePrediction": HousePrice,
    "Catboost" : CatBoost,
    "ssb_1.1": Query_1_1,
    "ssb_1.2": Query_1_2,
    "ssb_1.3": Query_1_3,
    "ssb_2.1": Query_2_1,
    "ssb_2.2": Query_2_2,
    "ssb_2.3": Query_2_3,
    "ssb_3.1": Query_3_1,
    "ssb_3.2": Query_3_2,
    "ssb_3.3": Query_3_3,
    "ssb_3.4": Query_3_4,
    "ssb_4.1": Query_4_1,
    "ssb_4.2": Query_4_2,
    "ssb_4.3": Query_4_3
}

def main():
    args = parser.parse_args()
    print(sys.executable.split('/')[-3])

    if not args.timeout is None:
        os.environ["TIMEOUT"] = args.timeout

    ex = EXECUTOR_DICT[args.executor](TASK_DICT[args.query](), args.format, args.sf, args.logfile)
    ex.measure()
    return

if __name__ == '__main__':
    main()
