from tasks.ssb.ssb_query import Ssb


class Query_1_1(Ssb):
    def __init__(self):
        super().__init__()
        self.name = "ssb_1.1"

    def download_pandas(self):
        print("SSB_Dict: ", self.filename_dict["ssb"])
        self.lineorder = self.list_reader_func(self.filename_dict["ssb"]["lineorder_paths"])
        self.date = self.reader_func(self.filename_dict["ssb"]["paths"]["date"])

    def download_snowpandas(self):
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>Scaling factor: ", "SSB_" + str(self.executor.scaling_factor) + ".DATE" )
        self.lineorder = self.executor.module.from_sf_table(tablename="SSB_" + self.executor.scaling_factor + ".LINEORDER")
        self.date = self.executor.module.from_sf_table(tablename="SSB_" + self.executor.scaling_factor + ".DATE")

    def download_snowparkpandas(self):
        self.lineorder = self.reader_func("LINEORDER")
        self.date = self.reader_func("DATE")

    def pandas_execution(self):
        self.joined = self.lineorder.set_index('LO_ORDERDATE').join(self.date.set_index('D_DATEKEY'))
        self.joined = self.joined.loc[(self.joined['LO_DISCOUNT'] <= 3) & (self.joined['LO_DISCOUNT'] >= 1)]
        self.joined = self.joined.loc[self.joined['D_YEAR'] == 1993]
        self.joined = self.joined.loc[self.joined['LO_QUANTITY'] < 25]
        self.joined = self.joined['LO_EXTENDEDPRICE'] * self.joined['LO_DISCOUNT']
        self.joined = self.joined.sum()
        self.joined = self.executor.module.DataFrame(data={'REVENUE': [self.joined]})

    def spark_execution(self):
        self.joined = self.lineorder.set_index('LO_ORDERDATE').join(self.date.set_index('D_DATEKEY'))
        self.joined = self.joined.loc[(self.joined['LO_DISCOUNT'] <= 3) & (self.joined['LO_DISCOUNT'] >= 1)]
        self.joined = self.joined.loc[self.joined['D_YEAR'] == 1993]
        self.joined = self.joined.loc[self.joined['LO_QUANTITY'] < 25]
        self.joined = self.joined['LO_EXTENDEDPRICE'] * self.joined['LO_DISCOUNT']
        self.joined = self.joined.sum()
        import pandas
        self.joined = pandas.DataFrame([self.joined], columns=["REVENUE"])
        self.joined = self.executor.module.from_pandas(self.joined)

    def snowpandas_execution(self):
        self.joined = self.lineorder.set_index('LO_ORDERDATE').join(self.date.set_index('D_DATEKEY'))
        self.joined = self.joined.loc[(self.joined['LO_DISCOUNT'] <= 3) & (self.joined['LO_DISCOUNT'] >= 1)]
        self.joined = self.joined.loc[self.joined['D_YEAR'] == 1993]
        self.joined = self.joined.loc[self.joined['LO_QUANTITY'] < 25]
        self.joined = self.joined['LO_EXTENDEDPRICE'] * self.joined['LO_DISCOUNT']
        self.joined = self.joined.sum()
        self.joined = self.executor.module.DataFrame(data={'REVENUE': [self.joined]})
        self.train = self.lineorder

    def polars_execution(self):
        self.joined = self.lineorder.join(self.date, left_on='LO_ORDERDATE', right_on='D_DATEKEY')
        self.joined = self.joined.filter((self.executor.module.col('LO_DISCOUNT') <= 3) & (self.executor.module.col('LO_DISCOUNT') >= 1))
        self.joined = self.joined.filter(self.executor.module.col('D_YEAR') == 1993)
        self.joined = self.joined.filter(self.executor.module.col('LO_QUANTITY') < 25)
        self.joined = (self.joined.select(self.executor.module.col('LO_EXTENDEDPRICE') * self.executor.module.col('LO_DISCOUNT')))
        self.joined = self.joined.sum()
        self.joined = self.joined.rename({'LO_EXTENDEDPRICE': 'REVENUE'})

    def dask_execution(self):
        self.joined = self.lineorder.merge(self.date, left_on="LO_ORDERDATE", right_on="D_DATEKEY")
        self.joined = self.joined[(self.joined['LO_DISCOUNT'] <= 3) & (self.joined['LO_DISCOUNT'] >= 1)]
        self.joined = self.joined[self.joined['D_YEAR'] == 1993]
        self.joined = self.joined[self.joined['LO_QUANTITY'] < 25]
        self.joined['total'] = self.joined['LO_EXTENDEDPRICE'] * self.joined['LO_DISCOUNT']
        self.joined = self.joined['total'].sum()
        self.joined = self.executor.module.DataFrame.from_dict({'REVENUE': [self.joined.compute()]})

    def vaex_execution(self):
        self.joined = self.lineorder.join(self.date, left_on='LO_ORDERDATE', right_on='D_DATEKEY', how='inner')
        self.joined = self.joined[(self.joined['LO_DISCOUNT'] <= 3) & (self.joined['LO_DISCOUNT'] >= 1)]
        self.joined = self.joined[self.joined['D_YEAR'] == 1993]
        self.joined = self.joined[self.joined['LO_QUANTITY'] < 25]
        self.joined['REVENUE'] = self.joined['LO_EXTENDEDPRICE'] * self.joined['LO_DISCOUNT']
        self.joined = self.joined['REVENUE'].sum().item()
        self.joined = self.executor.module.from_dict({'REVENUE': [self.joined]})

    def modin_execution(self):
        self.pandas_execution()

    def snowparkpandas_execution(self):
        self.pandas_execution()
