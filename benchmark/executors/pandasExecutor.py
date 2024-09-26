from .executor import Executor

class PandasExecutor(Executor):

    def __init__(self,
                 query,
                 format,
                 scaling_factor,
                 logfile):
        super().__init__(query, scaling_factor, format, logfile, engine="pandas")
        import pandas as pd
        self.module = pd
