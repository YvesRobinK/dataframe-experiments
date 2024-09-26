from .task import Task



class Fork(Task):
    # link to the note book on kaggle:
    # https://www.kaggle.com/code/kovcspter/fork-of-starship
    def __init__(self):
        super().__init__()
        self.name = "Fork"

    def spark_execution(self):
        self.train = self.train.drop('Name', axis=1)
        self.train['Transported'].apply(lambda x: 0 if x == False else 1 )
        self.train['Transported'] = self.train['Transported'].astype('int')
        from pyspark.sql.functions import split
        self.train = self.train.to_spark().withColumn('Deck', split(self.train.to_spark()['Cabin'], '/')[0]).pandas_api()
        self.train = self.train.to_spark().withColumn('Num', split(self.train.to_spark()['Cabin'], '/')[1]).pandas_api()
        self.train = self.train.to_spark().withColumn('Side', split(self.train.to_spark()['Cabin'], '/')[2]).pandas_api()
        col_to_sum = ['RoomService', 'FoodCourt', 'ShoppingMall', 'Spa', 'VRDeck']
        for c in col_to_sum:
            self.train[c] = self.train[c].fillna(0).astype('int32')

        self.train["SumSpends"] = self.train[col_to_sum].sum(axis=1)

    def pandas_execution(self):
        self.train.drop('Name', axis=1, inplace=True)
        self.train['Transported'].replace(False, 0, inplace=True)
        self.train['Transported'].replace(True, 1, inplace=True)
        self.train[["Deck", "Num", "Side"]] = self.train['Cabin'].str.split('/', expand=True)
        col_to_sum = ['RoomService', 'FoodCourt', 'ShoppingMall', 'Spa', 'VRDeck']
        self.train["SumSpends"] = self.train[col_to_sum].sum(axis=1)

    def modin_execution(self):
        self.pandas_execution()

    def snowpandas_execution(self):
        self.train.drop('Name', axis=1, inplace=True)
        self.train = self.train['Transported'].replace("False", 0)
        self.train = self.train['Transported'].replace("True", 1)
        self.train[["Deck", "Num", "Side"]] = self.train['Cabin'].str.split('/', expand=True)
        col_to_sum = ['RoomService', 'FoodCourt', 'ShoppingMall', 'Spa', 'VRDeck']
        self.train["SumSpends"] = self.train[col_to_sum].sum(axis=1)

    def polars_execution(self):
        import polars as pl
        self.train: pl.LazyFrame
        self.train = self.train.drop('Name')
        self.train = self.train.with_columns(
            pl.col('Transported').replace(False, 0)
        )
        self.train = self.train.with_columns(pl.col('Cabin').str.split_exact('/', 2))
        self.train = self.train.with_columns([
            pl.col('Cabin').struct.field('field_0').alias('Deck'),
            pl.col('Cabin').struct.field('field_1').alias('Num'),
            pl.col('Cabin').struct.field('field_2').alias('Side'),
        ])
        col_to_sum = ['RoomService', 'FoodCourt', 'ShoppingMall', 'Spa', 'VRDeck']
        self.train = self.train.with_columns([
            sum(pl.col(col) for col in col_to_sum).alias('SumSpends')
        ])

    def dask_execution(self):
        self.train = self.train.drop('Name', axis=1)
        self.train['Transported'] = self.train['Transported'].replace({False: 0, True: 1})
        self.train[['Deck', 'Num', 'Side']] = self.train['Cabin'].str.split('/', expand=True, n=2)
        col_to_sum = ['RoomService', 'FoodCourt', 'ShoppingMall', 'Spa', 'VRDeck']
        self.train['SumSpends'] = self.train[col_to_sum].sum(axis=1)

    def vaex_execution(self):
        self.train = self.train.drop(['Name'])
        self.train['Transported'] = self.train['Transported'].astype('int')
        #! NOTE: I could not find any better equivalent for splitting in VAEX
        #self.train['Deck'] = self.train['Cabin'].str.split('/').apply(lambda x: x[0])
        #self.train['Num'] = self.train['Cabin'].str.split('/').apply(lambda x: x[1])
        #self.train['Side'] = self.train['Cabin'].str.split('/').apply(lambda x: x[2])
        # Safely extract 'Deck'
        self.train['Deck'] = self.train['Cabin'].str.split('/').apply(lambda x: x[0] if x is not None and isinstance(x, list) and len(x) > 0 else None)
        # Safely extract 'Num'
        self.train['Num'] = self.train['Cabin'].str.split('/').apply(lambda x: x[1] if x is not None and isinstance(x, list) and len(x) > 1 else None)
        # Safely extract 'Side'
        self.train['Side'] = self.train['Cabin'].str.split('/').apply(lambda x: x[2] if x is not None and isinstance(x, list) and len(x) > 2 else None)

        


        col_to_sum = ['RoomService', 'FoodCourt', 'ShoppingMall', 'Spa', 'VRDeck']
        self.train['SumSpends'] = self.train[col_to_sum[0]] + self.train[col_to_sum[1]] + self.train[col_to_sum[2]] + self.train[col_to_sum[3]] + self.train[col_to_sum[4]]

    def snowparkpandas_execution(self):
        column_mapping = {
            'PASSENGERID': 'PassengerId',
            'HOMEPLANET': 'HomePlanet',
            'CRYOSLEEP': 'CryoSleep',
            'CABIN': 'Cabin',
            'DESTINATION': 'Destination',
            'AGE': 'Age',
            'VIP': 'VIP',
            'ROOMSERVICE': 'RoomService',
            'FOODCOURT': 'FoodCourt',
            'SHOPPINGMALL': 'ShoppingMall',
            'SPA': 'Spa',
            'VRDECK': 'VRDeck',
            'NAME': 'Name',
            'TRANSPORTED': 'Transported'
        }
        self.train = self.train.rename(columns=column_mapping)
        self.train.drop('Name', axis=1, inplace=True)
        self.train['Transported'] = self.train['Transported'].replace({"False": 0, "True": 1})
        #! note: expand=true is not supported, the following 3 split queries are executed eagerly!
        # self.train[["Deck", "Num", "Side"]] = self.train['Cabin'].str.split('/', expand=True)
        self.train["Deck"] = self.train['Cabin'].str.split('/')[0]
        self.train["Num"] = self.train['Cabin'].str.split('/')[1]
        self.train["Side"] = self.train['Cabin'].str.split('/')[2]
        col_to_sum = ['RoomService', 'FoodCourt', 'ShoppingMall', 'Spa', 'VRDeck']
        self.train["SumSpends"] = self.train[col_to_sum].sum(axis=1)
