import json

from .executor import Executor
from credentials import DIRECTORY_PATH_RAY


def glist_reader_func(filename_list, module):
    return module.concat([greader_func(filename, module) for filename in filename_list])


def greader_func(filename,module):
    if format == "csv":
        return module.read_csv(filename)
    else:
        return module.read_parquet(filename)

class ModinExecutor(Executor):
    def __init__(self,
                 query,
                 format,
                 scaling_factor,
                 logfile):
        super().__init__(query, scaling_factor, format, logfile, engine="modin")
        """
        os.environ.pop('MODIN_ENGINE', None)
        os.environ.pop('MODIN_CPUS', None)
        import importlib
        import modin.pandas as pd
        import modin.config as cfg
        import ray
        importlib.reload(ray)
        importlib.reload(pd)
        """
        import modin.pandas as pd
        import ray
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
        self.module = pd
        #os.environ["MODIN_MEMORY"] = "8000000000"
        #os.environ["MODIN_CPUS"] = "8"
        #cfg.Engine.put("ray")
