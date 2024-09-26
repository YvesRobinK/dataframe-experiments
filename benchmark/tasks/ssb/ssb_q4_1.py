from tasks.ssb.ssb_query import Ssb


class Query_4_1(Ssb):
    def __init__(self):
        super().__init__()
        self.name = "ssb_4.1"

    def download_pandas(self):
        print("SSB_Dict: ", self.filename_dict["ssb"])
        self.lineorder = self.list_reader_func(self.filename_dict["ssb"]["lineorder_paths"])
        self.date = self.reader_func(self.filename_dict["ssb"]["paths"]["date"])
        self.customer = self.reader_func(self.filename_dict["ssb"]["paths"]["customer"])
        self.supplier = self.reader_func(self.filename_dict["ssb"]["paths"]["supplier"])
        self.part = self.reader_func(self.filename_dict["ssb"]["paths"]["part"])

    def download_snowpandas(self):
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>Scaling factor: ", "SSB_" + str(self.scaling_factor) + ".DATE" )
        self.lineorder = self.module.from_sf_table(tablename="SSB_" + self.scaling_factor + ".LINEORDER")
        self.date = self.module.from_sf_table(tablename="SSB_" + self.scaling_factor + ".DATE")
        self.customer = self.module.from_sf_table(tablename="SSB_" + self.scaling_factor + ".CUSTOMER")
        self.supplier = self.module.from_sf_table(tablename="SSB_" + self.scaling_factor + ".SUPPLIER")
        self.part = self.module.from_sf_table(tablename="SSB_" + self.scaling_factor + ".PART")

    def download_snowparkpandas(self):
        self.lineorder = self.reader_func("LINEORDER")
        self.date = self.reader_func("DATE")
        self.customer = self.reader_func("CUSTOMER")
        self.supplier = self.reader_func("SUPPLIER")
        self.part = self.reader_func("PART")

    def pandas_execution(self):
        self.joined = self.lineorder.set_index('LO_ORDERDATE').join(self.date.set_index('D_DATEKEY'))
        self.joined = self.joined.set_index('LO_CUSTKEY').join(self.customer.set_index('C_CUSTKEY'))
        self.joined = self.joined.set_index('LO_PARTKEY').join(self.part.set_index('P_PARTKEY'))
        self.joined = self.joined.set_index('LO_SUPPKEY').join(self.supplier.set_index('S_SUPPKEY'))
        self.joined = self.joined.loc[self.joined['C_REGION'] == 'AMERICA']
        self.joined = self.joined.loc[self.joined['S_REGION'] == 'AMERICA']
        self.joined = self.joined.loc[(self.joined['P_MFGR'] == 'MFGR#1') | (self.joined['P_MFGR'] == 'MFGR#2')]
        self.joined['PROFIT1'] = self.joined['LO_REVENUE'] - self.joined['LO_SUPPLYCOST']
        self.joined = self.joined[['D_YEAR', 'C_NATION', 'PROFIT1']]
        self.joined = self.joined.groupby(["D_YEAR", "C_NATION"], as_index=False).agg({'PROFIT1': 'sum'})
        self.joined = self.joined.sort_values(by=["D_YEAR", "C_NATION"], ascending=[True, True])

    def spark_execution(self):
        self.pandas_execution()

    def snowpandas_execution(self):
        self.joined = self.lineorder.set_index('LO_ORDERDATE').join(self.date.set_index('D_DATEKEY'))
        self.joined = self.joined.set_index('LO_CUSTKEY').join(self.customer.set_index('C_CUSTKEY'))
        self.joined = self.joined.set_index('LO_PARTKEY').join(self.part.set_index('P_PARTKEY'))
        self.joined = self.joined.set_index('LO_SUPPKEY').join(self.supplier.set_index('S_SUPPKEY'))
        self.joined = self.joined.loc[self.joined['C_REGION'] == 'AMERICA']
        self.joined = self.joined.loc[self.joined['S_REGION'] == 'AMERICA']
        self.joined = self.joined.loc[(self.joined['P_MFGR'] == 'MFGR#1') | (self.joined['P_MFGR'] == 'MFGR#2')]
        self.joined['PROFIT1'] = self.joined['LO_REVENUE'] - self.joined['LO_SUPPLYCOST']
        self.joined = self.joined[['D_YEAR', 'C_NATION', 'PROFIT1']]
        self.joined = self.joined.groupby(["D_YEAR", "C_NATION"], as_index=False).agg({'PROFIT1': 'sum'})
        self.joined = self.joined.sort_values(by=["D_YEAR", "C_NATION"], ascending=[True, True])
        self.train = self.lineorder

    def polars_execution(self):
        self.joined = self.lineorder.join(self.date, left_on='LO_ORDERDATE', right_on='D_DATEKEY')
        self.joined = self.joined.join(self.customer, left_on='LO_CUSTKEY', right_on='C_CUSTKEY')
        self.joined = self.joined.join(self.part, left_on='LO_PARTKEY', right_on='P_PARTKEY')
        self.joined = self.joined.join(self.supplier, left_on='LO_SUPPKEY', right_on='S_SUPPKEY')
        # Filter the DataFrame
        self.joined = self.joined.filter(self.executor.module.col('C_REGION') == "AMERICA")
        self.joined = self.joined.filter(self.executor.module.col('S_REGION') == "AMERICA")
        self.joined = self.joined.filter((self.executor.module.col('P_MFGR') == "MFGR#1") | (self.executor.module.col('P_MFGR') == "MFGR#2"))
        self.joined = (
            self.joined.select(self.executor.module.all(),(self.executor.module.col('LO_REVENUE') - self.executor.module.col('LO_SUPPLYCOST')).alias("PROFIT1")))
        self.joined = self.joined.select(['D_YEAR', 'C_NATION', 'PROFIT1'])
        self.joined = (
            self.joined.group_by(["D_YEAR", "C_NATION"])
            .agg(self.executor.module.col("PROFIT1").sum().alias("PROFIT1"))
        )
        self.joined = self.joined.sort(by=["D_YEAR", "C_NATION"], descending=[False, False])

    def dask_execution(self):
        self.joined = self.lineorder.merge(self.date, left_on="LO_ORDERDATE", right_on="D_DATEKEY")
        self.joined = self.joined.merge(self.customer, left_on="LO_CUSTKEY", right_on="C_CUSTKEY")
        self.joined = self.joined.merge(self.part, left_on="LO_PARTKEY", right_on="P_PARTKEY")
        self.joined = self.joined.merge(self.supplier, left_on="LO_SUPPKEY", right_on="S_SUPPKEY")

        self.joined = self.joined[self.joined['C_REGION'] == "AMERICA"]
        self.joined = self.joined[self.joined['S_REGION'] == "AMERICA"]
        self.joined = self.joined[(self.joined['P_MFGR'] == "MFGR#1") | (self.joined['P_MFGR'] == "MFGR#2")]
        self.joined["PROFIT1"] = self.joined["LO_REVENUE"] - self.joined["LO_SUPPLYCOST"]
        self.joined = self.joined[['D_YEAR', 'C_NATION', 'PROFIT1']]
        self.joined = self.joined.groupby(["D_YEAR", "C_NATION"]).agg({'PROFIT1': 'sum'}).reset_index()
        self.joined = self.joined.sort_values(by=["D_YEAR", "C_NATION"], descending=[False, False])

    def vaex_execution(self):
        self.joined = self.lineorder.join(self.date, left_on='LO_ORDERDATE', right_on='D_DATEKEY', how='inner')
        self.joined = self.joined.join(self.customer, left_on='LO_CUSTKEY', right_on='C_CUSTKEY', how='inner')
        self.joined = self.joined.join(self.part, left_on='LO_PARTKEY', right_on='P_PARTKEY', how='inner')
        self.joined = self.joined.join(self.supplier, left_on='LO_SUPPKEY', right_on='S_SUPPKEY', how='inner')
        self.joined = self.joined[self.joined['C_REGION'] == "AMERICA"]
        self.joined = self.joined[self.joined['S_REGION'] == "AMERICA"]
        self.joined = self.joined[(self.joined['P_MFGR'] == "MFGR#1") | (self.joined['P_MFGR'] == "MFGR#2")]
        self.joined["PROFIT1"] = self.joined["LO_REVENUE"] - self.joined["LO_SUPPLYCOST"]
        self.joined = self.joined[['D_YEAR', 'C_NATION', 'PROFIT1']]
        self.joined = self.joined.groupby(by=["D_YEAR", "C_NATION"], agg={'PROFIT1': self.executor.module.agg.sum('PROFIT1')})
        self.joined = self.joined.sort(["D_YEAR", "C_NATION"], ascending=[True, True])

    def modin_execution(self):
        self.pandas_execution()

    def snowparkpandas_execution(self):
        self.pandas_execution()
