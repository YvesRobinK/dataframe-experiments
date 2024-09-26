from tasks.ssb.ssb_query import Ssb


class Query_3_3(Ssb):
    def __init__(self):
        super().__init__()
        self.name = "ssb_3.3"

    def download_pandas(self):
        print("SSB_Dict: ", self.filename_dict["ssb"])
        self.lineorder = self.list_reader_func(self.filename_dict["ssb"]["lineorder_paths"])
        self.date = self.reader_func(self.filename_dict["ssb"]["paths"]["date"])
        self.customer = self.reader_func(self.filename_dict["ssb"]["paths"]["customer"])
        self.supplier = self.reader_func(self.filename_dict["ssb"]["paths"]["supplier"])

    def download_snowpandas(self):
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>Scaling factor: ", "SSB_" + str(self.scaling_factor) + ".DATE" )
        self.lineorder = self.module.from_sf_table(tablename="SSB_" + self.scaling_factor + ".LINEORDER")
        self.date = self.module.from_sf_table(tablename="SSB_" + self.scaling_factor + ".DATE")
        self.customer = self.module.from_sf_table(tablename="SSB_" + self.scaling_factor + ".CUSTOMER")
        self.supplier = self.module.from_sf_table(tablename="SSB_" + self.scaling_factor + ".SUPPLIER")

    def download_snowparkpandas(self):
        self.lineorder = self.reader_func("LINEORDER")
        self.date = self.reader_func("DATE")
        self.customer = self.reader_func("CUSTOMER")
        self.supplier = self.reader_func("SUPPLIER")

    def pandas_execution(self):
        self.joined = self.lineorder.set_index('LO_ORDERDATE').join(self.date.set_index('D_DATEKEY'))
        self.joined = self.joined.set_index('LO_CUSTKEY').join(self.customer.set_index('C_CUSTKEY'))
        self.joined = self.joined.set_index('LO_SUPPKEY').join(self.supplier.set_index('S_SUPPKEY'))
        self.joined = self.joined.loc[self.joined['C_NATION'] == 'UNITED KINGDOM']
        self.joined = self.joined.loc[(self.joined['C_CITY'] == 'UNITED KI1') | (
                    self.joined['C_CITY'] == 'UNITED KI5')]  # (c_city='UNITED KI1' or c_city='UNITED KI5')
        self.joined = self.joined.loc[(self.joined['S_CITY'] == 'UNITED KI1') | (self.joined['S_CITY'] == 'UNITED KI5')]
        self.joined = self.joined.loc[self.joined['S_NATION'] == 'UNITED KINGDOM']
        self.joined = self.joined.loc[(self.joined['D_YEAR'] >= 1992) & (self.joined['D_YEAR'] <= 1997)]
        self.joined = self.joined[['C_CITY', 'S_CITY', 'D_YEAR', 'LO_REVENUE']]
        self.joined = self.joined.groupby(["C_CITY", "S_CITY", "D_YEAR"], as_index=False).agg({'LO_REVENUE': 'sum'})
        self.joined = self.joined.sort_values(by=['D_YEAR', 'LO_REVENUE'], ascending=[True, False])
        self.joined = self.joined.rename(columns={"LO_REVENUE": 'REVENUE'})

    def spark_execution(self):
        self.pandas_execution()

    def snowpandas_execution(self):
        self.joined = self.lineorder.set_index('LO_ORDERDATE').join(self.date.set_index('D_DATEKEY'))
        self.joined = self.joined.set_index('LO_CUSTKEY').join(self.customer.set_index('C_CUSTKEY'))
        self.joined = self.joined.set_index('LO_SUPPKEY').join(self.supplier.set_index('S_SUPPKEY'))
        self.joined = self.joined.loc[self.joined['C_NATION'] == 'UNITED KINGDOM']
        self.joined = self.joined.loc[(self.joined['C_CITY'] == 'UNITED KI1') | (
                    self.joined['C_CITY'] == 'UNITED KI5')]  # (c_city='UNITED KI1' or c_city='UNITED KI5')
        self.joined = self.joined.loc[(self.joined['S_CITY'] == 'UNITED KI1') | (self.joined['S_CITY'] == 'UNITED KI5')]
        self.joined = self.joined.loc[self.joined['S_NATION'] == 'UNITED KINGDOM']
        self.joined = self.joined.loc[(self.joined['D_YEAR'] >= 1992) & (self.joined['D_YEAR'] <= 1997)]
        self.joined = self.joined[['C_CITY', 'S_CITY', 'D_YEAR', 'LO_REVENUE']]
        self.joined = self.joined.groupby(["C_CITY", "S_CITY", "D_YEAR"], as_index=False).agg({'LO_REVENUE': 'sum'})
        self.joined = self.joined.sort_values(by=['D_YEAR', 'LO_REVENUE'], ascending=[True, False])
        self.joined = self.joined.rename(columns={"LO_REVENUE": 'REVENUE'})
        self.train = self.lineorder

    def polars_execution(self):
        self.joined = self.lineorder.join(self.date, left_on='LO_ORDERDATE', right_on='D_DATEKEY')
        self.joined = self.joined.join(self.customer, left_on='LO_CUSTKEY', right_on='C_CUSTKEY')
        self.joined = self.joined.join(self.supplier, left_on='LO_SUPPKEY', right_on='S_SUPPKEY')
        # Filter the DataFrame
        self.joined = self.joined.filter(self.executor.module.col('C_NATION') == "UNITED KINGDOM")
        self.joined = self.joined.filter(
            (self.executor.module.col('C_CITY') == 'UNITED KI1') | (self.executor.module.col('C_CITY') == 'UNITED KI5'))
        self.joined = self.joined.filter(
            (self.executor.module.col('S_CITY') == 'UNITED KI1') | (self.executor.module.col('S_CITY') == 'UNITED KI5'))
        self.joined = self.joined.filter(self.executor.module.col('S_NATION') == "UNITED KINGDOM")
        self.joined = self.joined.filter((self.executor.module.col('D_YEAR') >= 1992) & (self.executor.module.col('D_YEAR') <= 1997))

        self.joined = self.joined.select(['C_CITY', 'S_CITY', 'D_YEAR', 'LO_REVENUE'])
        self.joined = (
            self.joined.group_by(["C_CITY", "S_CITY", "D_YEAR"])
            .agg(self.executor.module.col("LO_REVENUE").sum().alias("REVENUE"))
        )

        self.joined = self.joined.sort(by=["D_YEAR", "REVENUE"], descending=[False, True])

    def dask_execution(self):
        self.joined = self.lineorder.merge(self.date, left_on="LO_ORDERDATE", right_on="D_DATEKEY")
        self.joined = self.joined.merge(self.customer, left_on="LO_CUSTKEY", right_on="C_CUSTKEY")
        self.joined = self.joined.merge(self.supplier, left_on="LO_SUPPKEY", right_on="S_SUPPKEY")

        self.joined = self.joined[self.joined['C_NATION'] == "UNITED KINGDOM"]
        self.joined = self.joined[(self.joined['C_CITY'] == 'UNITED KI1') | (self.joined['C_CITY'] == 'UNITED KI5')]
        self.joined = self.joined[(self.joined['S_CITY'] == 'UNITED KI1') | (self.joined['S_CITY'] == 'UNITED KI5')]
        self.joined = self.joined[self.joined['S_NATION'] == "UNITED KINGDOM"]
        self.joined = self.joined[(self.joined['D_YEAR'] >= 1992) & (self.joined['D_YEAR'] <= 1997)]

        self.joined = self.joined[['C_CITY', 'S_CITY', 'D_YEAR', 'LO_REVENUE']]
        self.joined = self.joined.groupby(["C_CITY", "S_CITY", "D_YEAR"]).agg({'LO_REVENUE': 'sum'}).reset_index()
        self.joined = self.joined.rename(columns={"LO_REVENUE": "REVENUE"})
        self.joined = self.joined.sort_values(by=["D_YEAR", "REVENUE"], ascending=[True, False])

    def vaex_execution(self):
        self.joined = self.lineorder.join(self.date, left_on='LO_ORDERDATE', right_on='D_DATEKEY', how='inner')
        self.joined = self.joined.join(self.customer, left_on='LO_CUSTKEY', right_on='C_CUSTKEY', how='inner')
        self.joined = self.joined.join(self.supplier, left_on='LO_SUPPKEY', right_on='S_SUPPKEY', how='inner')
        self.joined = self.joined[self.joined['C_NATION'] == "UNITED KINGDOM"]
        self.joined = self.joined[(self.joined['C_CITY'] == 'UNITED KI1') | (self.joined['C_CITY'] == 'UNITED KI5')]
        self.joined = self.joined[(self.joined['S_CITY'] == 'UNITED KI1') | (self.joined['S_CITY'] == 'UNITED KI5')]
        self.joined = self.joined[self.joined['S_NATION'] == "UNITED KINGDOM"]
        self.joined = self.joined[(self.joined['D_YEAR'] >= 1992) & (self.joined['D_YEAR'] <= 1997)]
        self.joined = self.joined[['C_CITY', 'S_CITY', 'D_YEAR', 'LO_REVENUE']]
        self.joined = self.joined.groupby(by=["C_CITY", "S_CITY", "D_YEAR"], agg={'LO_REVENUE': self.executor.module.agg.sum('LO_REVENUE')})
        self.joined.rename('LO_REVENUE', 'REVENUE')
        self.joined = self.joined.sort(["D_YEAR", "REVENUE"], ascending=[True, False])

    def modin_execution(self):
        self.pandas_execution()

    def snowparkpandas_execution(self):
        self.pandas_execution()
