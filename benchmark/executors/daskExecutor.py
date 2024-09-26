from .executor import Executor
import os
class DaskExecutor(Executor):
    def __init__(self,
                query,
                format: str,
                scaling_factor: int,
                logfile: str,
                ):
        super().__init__(query, scaling_factor, format, logfile, engine='dask')
        """
        from dask.distributed import Client, LocalCluster

        # Create a LocalCluster with a specified spill location
        cluster = LocalCluster(
            n_workers=10,
            threads_per_worker=4,
            memory_limit="4GB",
            local_directory="/data"  # Directory for spill files
        )

        # Attach the client to the cluster
        client = Client(cluster)
        """
        import os

        # Set the DASK_TEMPORARY_DIRECTORY environment variable
        os.environ["DASK_TEMPORARY_DIRECTORY"] = "/data"
        #os.environ["DASK_WORKER_MEMORY_LIMIT"] = "8GB"

        #os.environ["DASK_WORKER_NWORKERS"] = "30"  # Set to 4 workers

        # Set the number of threads per worker
        #os.environ["DASK_WORKER_THREADS_PER_WORKER"] = "2"
        import dask.dataframe as dd
        self.module = dd

    def shutdown(self):
        super().shutdown()
        import shutil
        shutil.rmtree("/data/result_dask")

    def reader_func(self, filename):
        if self.format == "csv":
            return self.module.read_csv(filename, sample_rows=2000)
        else:
            return self.module.read_parquet(filename, sample_rows=2000)
