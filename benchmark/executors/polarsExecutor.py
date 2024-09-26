from .executor import Executor

class PolarsExecutor(Executor):
    def __init__(self,
                query,
                format: str,
                scaling_factor: int,
                logfile: str,
                ):
        super().__init__(query, scaling_factor, format, logfile, engine='polars')
        import polars as pd
        self.module = pd

    def reader_func(self, filename):
        if self.format == "csv":
            return self.module.scan_csv(filename, cache=False)
        else:
            return self.module.scan_parquet(filename, cache=False)
