from tasks.ssb.ssb_query import Ssb


class Query_2_2(Ssb):
    def __init__(self):
        super().__init__()
        self.name = "ssb_2.2"

    def download_pandas(self):
        print("SSB_Dict: ", self.filename_dict["ssb"])
        self.lineorder = self.list_reader_func(self.filename_dict["ssb"]["lineorder_paths"])
        self.date = self.reader_func(self.filename_dict["ssb"]["paths"]["date"])
        self.part = self.reader_func(self.filename_dict["ssb"]["paths"]["part"])
        self.supplier = self.reader_func(self.filename_dict["ssb"]["paths"]["supplier"])

    def download_snowpandas(self):
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>Scaling factor: ", "SSB_" + str(self.scaling_factor) + ".DATE" )
        self.lineorder = self.module.from_sf_table(tablename="SSB_" + self.scaling_factor + ".LINEORDER")
        self.date = self.module.from_sf_table(tablename="SSB_" + self.scaling_factor + ".DATE")
        self.part = self.module.from_sf_table(tablename="SSB_" + self.scaling_factor + ".PART")
        self.supplier = self.module.from_sf_table(tablename="SSB_" + self.scaling_factor + ".SUPPLIER")

    def download_snowparkpandas(self):
        self.lineorder = self.reader_func("LINEORDER")
        self.date = self.reader_func("DATE")
        self.part = self.reader_func("PART")
        self.supplier = self.reader_func("SUPPLIER")

    def pandas_execution(self):
        self.joined = self.lineorder.set_index('LO_ORDERDATE').join(self.date.set_index('D_DATEKEY'))
        self.joined = self.joined.set_index('LO_PARTKEY').join(self.part.set_index('P_PARTKEY'))
        self.joined = self.joined.set_index('LO_SUPPKEY').join(self.supplier.set_index('S_SUPPKEY'))
        self.joined = self.joined.loc[(self.joined['P_BRAND1'] >= 'MFGR#2221') & (self.joined['P_BRAND1'] <= 'MFGR#2228')]
        self.joined = self.joined.loc[self.joined['S_REGION'] == 'ASIA']
        self.joined = self.joined[["LO_REVENUE", "D_YEAR", "P_BRAND1"]]
        self.joined = self.joined.groupby(["D_YEAR", "P_BRAND1"], as_index=False).agg({'LO_REVENUE': 'sum'})
        self.joined = self.joined.sort_values(by=["D_YEAR", "P_BRAND1"], ascending=[True, True])
        self.joined = self.joined.rename(columns={"LO_REVENUE": 'SUM(LO_REVENUE)'})

    def spark_execution(self):
        self.pandas_execution()

    def snowpandas_execution(self):
        self.joined = self.lineorder.set_index('LO_ORDERDATE').join(self.date.set_index('D_DATEKEY'))
        self.joined = self.joined.set_index('LO_PARTKEY').join(self.part.set_index('P_PARTKEY'))
        self.joined = self.joined.set_index('LO_SUPPKEY').join(self.supplier.set_index('S_SUPPKEY'))
        self.joined = self.joined.loc[(self.joined['P_BRAND1'] >= 'MFGR#2221') & (self.joined['P_BRAND1'] <= 'MFGR#2228')]
        self.joined = self.joined.loc[self.joined['S_REGION'] == 'ASIA']
        self.joined = self.joined[["LO_REVENUE", "D_YEAR", "P_BRAND1"]]
        self.joined = self.joined.groupby(["D_YEAR", "P_BRAND1"], as_index=False).agg({'LO_REVENUE': 'sum'})
        self.joined = self.joined.sort_values(by=["D_YEAR", "P_BRAND1"], ascending=[True, True])
        self.joined = self.joined.rename(columns={"LO_REVENUE": 'SUM(LO_REVENUE)'})
        self.train = self.lineorder

    def polars_execution(self):
        self.joined = self.lineorder.join(self.date, left_on='LO_ORDERDATE', right_on='D_DATEKEY')
        self.joined = self.joined.join(self.part, left_on='LO_PARTKEY', right_on='P_PARTKEY')
        self.joined = self.joined.join(self.supplier, left_on='LO_SUPPKEY', right_on='S_SUPPKEY')
        # Filter the DataFrame
        self.joined = self.joined.filter((self.executor.module.col('P_BRAND1') >= "MFGR#2221") & (self.executor.module.col('P_BRAND1') <= "MFGR#2228"))
        self.joined = self.joined.filter(self.executor.module.col('S_REGION') == "ASIA")
        self.joined = self.joined.select(["LO_REVENUE", "D_YEAR", "P_BRAND1"])
        self.joined = (
            self.joined.group_by(["D_YEAR", "P_BRAND1"])
            .agg(self.executor.module.col("LO_REVENUE").sum().alias("SUM(LO_REVENUE)"))
        )
        self.joined = self.joined.sort(by=["D_YEAR", "P_BRAND1"], descending=[False, False])

    def dask_execution(self):
        self.joined = self.lineorder.merge(self.date, left_on="LO_ORDERDATE", right_on="D_DATEKEY")
        self.joined = self.joined.merge(self.part, left_on="LO_PARTKEY", right_on="P_PARTKEY")
        self.joined = self.joined.merge(self.supplier, left_on="LO_SUPPKEY", right_on="S_SUPPKEY")
        self.joined = self.joined[(self.joined['P_BRAND1'] >= 'MFGR#2221') & (self.joined['P_BRAND1'] <= 'MFGR#2228')]
        self.joined = self.joined[self.joined['S_REGION'] == "ASIA"]
        self.joined = self.joined[["LO_REVENUE", "D_YEAR", "P_BRAND1"]]
        self.joined = self.joined.groupby(["D_YEAR", "P_BRAND1"]).agg({'LO_REVENUE': 'sum'}).reset_index()
        self.joined = self.joined.sort_values(by=["D_YEAR", "P_BRAND1"], ascending=[True, True])
        self.joined = self.joined.rename(columns={"LO_REVENUE": "SUM(LO_REVENUE)"})

    def vaex_execution(self):
        self.joined = self.lineorder.join(self.date, left_on='LO_ORDERDATE', right_on='D_DATEKEY', how='inner')
        self.joined = self.joined.join(self.part, left_on='LO_PARTKEY', right_on='P_PARTKEY', how='inner')
        self.joined = self.joined.join(self.supplier, left_on='LO_SUPPKEY', right_on='S_SUPPKEY', how='inner')
        #self.joined = self.joined[(self.joined['P_BRAND1'] >= 'MFGR#2221') & (self.joined['P_BRAND1'] <= 'MFGR#2228')]
        self.joined = self.joined[self.joined['S_REGION'] == "ASIA"]
        self.joined = self.joined[["LO_REVENUE", "D_YEAR", "P_BRAND1"]]
        self.joined = self.joined.groupby(by=['D_YEAR', 'P_BRAND1'], agg={'LO_REVENUE': self.executor.module.agg.sum('LO_REVENUE')})
        self.joined = self.joined.sort(['D_YEAR', 'P_BRAND1'])
        self.joined.rename('LO_REVENUE', 'SUM(LO_REVENUE)')

    def modin_execution(self):
        self.pandas_execution()

    def snowparkpandas_execution(self):
        self.pandas_execution()
