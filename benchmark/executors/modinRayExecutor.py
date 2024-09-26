import json

from .executor import Executor
from credentials import DIRECTORY_PATH_RAY


class ModinRayExecutor(Executor):
    def __init__(self,
                 query,
                 format,
                 scaling_factor,
                 logfile):
        super().__init__(query, scaling_factor, format, logfile, engine="modinray")
        import modin.config as modin_cfg
        modin_cfg.Engine.put("ray")
        import ray
        #os.environ["RAY_memory_monitor_refresh_ms"] = "0"
        ray.init(
            num_cpus=64,
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

        # DONT MOVE THE FOLLOWING LINE! importing modin.padnas must be after setting the backend engine!
        import modin.pandas as pd
        self.module = pd
