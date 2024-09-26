from .executor import Executor

class ModinDaskExecutor(Executor):
    def __init__(self,
                 query,
                 format,
                 scaling_factor,
                 logfile):
        super().__init__(query, scaling_factor, format, logfile, engine="modindask")
        """
        import modin.config as modin_cfg
        modin_cfg.Engine.put("dask")
        from distributed import Client
        client = Client()
        # DONT MOVE THE FOLLOWING LINE! importing modin.padnas must be after setting the backend engine!
        import modin.pandas as pd
        """
        """
        import os
        os.environ["DASK_TEMPORARY_DIRECTORY"] = "/data"

        
        os.environ["MODIN_ENGINE"] = "dask"
        
        import modin.pandas as pd

        # Set the MODIN_ENGINE environment variable to dask

        self.module = pd
        #os.environ["MODIN_MEMORY"] = "8000000000"
        #os.environ["MODIN_CPUS"] = "8"
        """
        import modin.config as cfg
        from distributed import Client
        from dask.distributed import LocalCluster

        # Step 1: Configure and start a Dask cluster on a single machine
        cluster = LocalCluster(
            n_workers=4,  # Number of worker processes (adjust according to your CPU cores)
            threads_per_worker=16,  # Number of threads per worker (adjust based on workload)
            memory_limit='64GB',  # Memory limit per worker (adjust based on your system)
            dashboard_address=':8787'  # Dask dashboard address (optional)
        )
        client = Client(cluster)  # Connect the Dask client to this cluster

        # Step 2: Set Modin to use Dask
        cfg.Engine.put("dask")

        # Step 3: Use Modin as you normally would with pandas
        import modin.pandas as pd
        self.module = pd


