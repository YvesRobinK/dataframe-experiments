import os

from credentials import DIRECTORY_PATH_RAY
from .executor import Executor

class VaexExecutor(Executor):
    def __init__(self,
                query,
                format: str,
                scaling_factor: int,
                logfile: str,
                ):
        super().__init__(query, scaling_factor, format, logfile, engine='vaex')
        os.environ['VAEX_HOME'] = DIRECTORY_PATH_RAY
        import vaex as pd
        self.module = pd

    def reader_func(self, filename):
        return self.module.open(filename)

    def shutdown(self):
        super().shutdown()
        import shutil
        vaex_home_dir = os.environ['VAEX_HOME']
        shutil.rmtree(f"{vaex_home_dir}/file-cache")
        shutil.rmtree(f"{vaex_home_dir}/lock")
