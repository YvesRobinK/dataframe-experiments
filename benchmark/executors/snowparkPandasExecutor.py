from .executor import Executor
import subprocess

from credentials import WAREHOUSE_NAME, SNOWFLAKE_CONNECTION_DICT
from .executor import Executor


class SnowparkPandasExecutor(Executor):
    def __init__(self,
                 query,
                 format,
                 scaling_factor,
                 logfile):
        super().__init__(query, scaling_factor, format, logfile, engine="snowparkpandas")
        import modin.pandas as pd
        # import snowflake.snowpark.modin.pandas as pd
        import snowflake.snowpark.modin.plugin
        from snowflake.snowpark import Session
        from snowflake.snowpark.version import VERSION

        if self.query.name in {'Fork', 'Endeavor', 'Catboost'}: # spaceship data set
            self.sf_db_name = "SPACE"
        elif self.query.name in {'HousePricePrediction'}: # house prediction data set
            self.sf_db_name = "HOUSE"
        elif "ssb" in self.query.name:
            self.sf_db_name = "SSB"

        CONNECTION_PARAMETERS: dict = SNOWFLAKE_CONNECTION_DICT
        CONNECTION_PARAMETERS['database'] = self.sf_db_name
        CONNECTION_PARAMETERS['schema'] = self.schema

        session = Session.builder.configs(CONNECTION_PARAMETERS).create()
        self.session = session
        session.sql_simplifier_enabled = True
        snowflake_environment = session.sql('SELECT current_user(), current_version()').collect()
        snowpark_version = VERSION

        print('\nConnection Established with the following parameters:')
        print('User                        : {}'.format(snowflake_environment[0][0]))
        print('Role                        : {}'.format(session.get_current_role()))
        print('Database                    : {}'.format(session.get_current_database()))
        print('Schema                      : {}'.format(session.get_current_schema()))
        print('Warehouse                   : {}'.format(session.get_current_warehouse()))
        print('Snowflake version           : {}'.format(snowflake_environment[0][1]))
        print('Snowpark for Python version : {}.{}.{}'.format(snowpark_version[0],snowpark_version[1],snowpark_version[2]))

        self.sf_warehouse_name = WAREHOUSE_NAME
        self.module = pd

    @property
    def schema(self):
        return self.sf_db_name + '_' + self.scaling_factor

    def list_reader_func(self, filename_list):
        # snowpark_df = self.session.table("TRAIN")
        # return snowpark_df.to_snowpark_pandas()
        return self.module.read_snowflake('TRAIN')

    def reader_func(self, tablename):
        # snowpark_df = self.session.table("TRAIN")
        # return snowpark_df.to_snowpark_pandas()
        return self.module.read_snowflake(tablename)

    def setup(self):
        subprocess.Popen(["bash", 'suspend.sh', str(self.sf_warehouse_name)]).wait()
        super().setup()
