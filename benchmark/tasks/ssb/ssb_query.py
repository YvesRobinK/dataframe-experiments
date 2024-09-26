from ..task import Task


class Ssb(Task):
    def __init__(self):
        super().__init__()

    def set_results(self):
        self.result = self.joined

    def download_pandas(self):
        raise 'Download Pandas not implemented'

    def download_snowpandas(self):
        raise 'Download snowpandas not implemented'

    def download_snowparkpandas(self):
        raise 'Download snowpark pandas not implemented'

    def download_polars(self):
        self.download_pandas()

    def download_dask(self):
        self.download_pandas()

    def download(self):
        if self.engine == "pandas" or "modin" in self.engine or self.engine == "spark":
            self.download_pandas()
        elif self.engine == "snowpandas":
            self.download_snowpandas()
        elif self.engine == "polars":
            self.download_polars()
        elif self.engine == "dask":
            self.download_dask()
        elif self.engine == "vaex":
            self.download_pandas()
        elif self.engine == "snowparkpandas":
            self.download_snowparkpandas()
