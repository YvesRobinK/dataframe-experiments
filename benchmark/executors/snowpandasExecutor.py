from .executor import Executor
import json
import subprocess

from credentials import WAREHOUSE_NAME, DIRECTORY_PATH_RAY, SNOWFLAKE_CONNECTION_DICT
from .executor import Executor


class SnowpandasExecutor(Executor):
    def __init__(self,
                 query,
                 format,
                 scaling_factor,
                 logfile):
        super().__init__(query, scaling_factor, format, logfile, engine="snowpandas")
        import modin.pandas as pd
        import modin.config as cfg
        import ray
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
        if self.query.name in {'Fork', 'Endeavor', 'Catboost'}: # spaceship data set
            self.sf_db_name = "SPACE"
        elif self.query.name in {'HousePricePrediction'}: # house prediction data set
            self.sf_db_name = "HOUSE"
        elif "ssb" in self.query.name:
            self.sf_db_name = "SSB"

        self.sf_warehouse_name = WAREHOUSE_NAME

        cfg.LogMode.disable()
        cfg.IsExperimental.put(True)
        cfg.SnowFlakeConnectionParameters.put(SNOWFLAKE_CONNECTION_DICT)
        cfg.SnowFlakeDatabaseName.put(self.sf_db_name)
        cfg.SnowFlakeWarehouseName.put(self.sf_warehouse_name)

        # todo: check if we should change snowflake as well?
        self.query.scaling_factor = self.scaling_factor
        self.module = pd
        self.query.module = self.module

    def list_reader_func(self, filename_list):
        return self.module.from_sf_table(f"{self.sf_db_name}_{self.scaling_factor}.TRAIN")

    def reader_func(self, filename):
        if "test" in filename:
            return self.module.from_sf_table(f"{self.sf_db_name}_{self.scaling_factor}.test")

        self.query.module = self.module
        self.query.filename_dict = self.filename_dict
        self.query.scaling_factor = self.scaling_factor

    def setup(self):
        subprocess.Popen(["bash", 'suspend.sh', str(self.sf_warehouse_name)]).wait()
        super().setup()
