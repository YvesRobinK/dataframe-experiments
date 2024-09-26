class Task():
    def __init__(self):
        self.engine: str = None
        self.name: str = None
        self.module = None
        self.reader_func = None
        self.list_reader_func = None
        self.filename_dict = None
        self.res = "not assigned yet"
        self.train = None
        self.test = None

    def download(self):
        self.train = self.list_reader_func(self.filename_dict["train"])

    def spark_execution(self):
        raise 'not implemented yet'

    def pandas_execution(self):
        raise 'not implemented yet'

    def modin_execution(self):
        raise 'not implemented yet'

    def modin_ray_execution(self):
        self.modin_execution()

    def modin_dask_execution(self):
        self.modin_execution()

    def snowpandas_execution(self):
        raise 'not implemented yet'

    def polars_execution(self):
        raise 'not implemented yet'

    def dask_execution(self):
        raise 'not implemented yet'

    def vaex_execution(self):
        raise 'not implemented yet'

    def snowparkpandas_execution(self):
        raise 'not implemented yet'

    def polars_post_execution(self):
        # https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.collect.html
        self.result = self.result.collect()

    def dask_post_execution(self):
        # self.result.to_parquet("/data/result_dask")
        self.result = self.result.compute()

    def vaex_post_execution(self):
        self.result = self.result.materialize()

    def post_execute(self):
        if self.engine == "snowpandas":
            if hasattr(self.result._query_compiler._modin_frame, "_frame"):
                self.result = self.result._query_compiler._modin_frame.to_pandas()
            else:
                self.result = self.result
            """
            batches = self.result._query_compiler._modin_frame._frame._frame.to_pandas_batches()
            self.result = next(batches)
            """
        elif self.engine == 'snowparkpandas':
            pass
        elif self.engine == 'vaex':
            self.vaex_post_execution()
        elif self.engine == 'polars':
            self.polars_post_execution()
        elif self.engine == 'dask':
            self.dask_post_execution()

    def set_results(self):
        self.result = self.train

    def execute(self):
        if self.engine == "spark":
            self.spark_execution()
        elif self.engine == "pandas":
            self.pandas_execution()
        elif self.engine == "modin":
            self.modin_execution()
        elif self.engine == 'modinray':
            self.modin_ray_execution()
        elif self.engine == 'modindask':
            self.modin_dask_execution()
        elif self.engine == "snowpandas":
            self.snowpandas_execution()
        elif self.engine == 'polars':
            self.polars_execution()
        elif self.engine == 'dask':
            self.dask_execution()
        elif self.engine == 'vaex':
            self.vaex_execution()
        elif self.engine == 'snowparkpandas':
            self.snowparkpandas_execution()

        self.set_results()
        self.post_execute()
        #trigger execution for all engines
        if self.engine == "snowparkpandas":
            pass
            # batches = self.result._query_compiler._modin_frame._frame._frame.to_pandas_batches()
            # self.result = next(batches)

        print(self.result)
        print('Execution finished successfully!')
