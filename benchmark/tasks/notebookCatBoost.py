from .task import Task


class CatBoost(Task):
    def __init__(self):
        super().__init__()
        # link to kaggle notebook:
        # https://www.kaggle.com/code/pasuvulasaikiran/titanic-spaceship-feature-selection-catboost/notebook
        self.name = "Catboost"

    def spark_execution(self):
        from pyspark.sql.functions import split
        self.train = self.train.to_spark().withColumn('Cabin1_', split(self.train.to_spark()['Cabin'], '/')[0]).pandas_api()
        self.train = self.train.to_spark().withColumn('Cabin2_', split(self.train.to_spark()['Cabin'], '/')[1]).pandas_api()
        self.train = self.train.to_spark().withColumn('Cabin3_', split(self.train.to_spark()['Cabin'], '/')[2]).pandas_api()


        self.train = self.train.to_spark().withColumn('Pid1_', split(self.train.to_spark()['PassengerId'], '_')[0]).pandas_api()
        self.train = self.train.to_spark().withColumn('Pid2_', split(self.train.to_spark()['PassengerId'], '_')[1]).pandas_api()
        self.train[["Pid1_", 'Pid2_']] = self.train[["Pid1_", 'Pid2_']].astype('int')

        self.train = self.train.to_spark().withColumn('Fname_',split(self.train.to_spark()['Name'], ' ')[0]).pandas_api()
        self.train = self.train.to_spark().withColumn('Lname_',split(self.train.to_spark()['Name'], ' ')[1]).pandas_api()

        self.train = self.train.to_spark().withColumn('Fname_',split(self.train.to_spark()['Name'], ' ')[0]).pandas_api()
        self.train = self.train.to_spark().withColumn('Lname_',split(self.train.to_spark()['Name'], ' ')[1]).pandas_api()

        self.train['sum_exp_'] = self.train['RoomService'] + self.train['FoodCourt']
        self.train['sum_exp_'] = self.train['sum_exp_'] + self.train['ShoppingMall']
        self.train['sum_exp_'] = self.train['sum_exp_'] + self.train['Spa']
        self.train['sum_exp_'] = self.train['sum_exp_'] + self.train['VRDeck']

        self.train['sum_exp_'] = self.train['sum_exp_'] / self.train['Pid2_']

        #Original call
        #tt['Age_cat_'] =pd.cut(tt.Age, bins=[0, 5, 12, 18, 50, 150], labels=['Toddler/Baby', 'Child', 'Teen', 'Adult', 'Elderly'])
        self.train.loc[self.train['Age'] <= 5, ['Age_cat_']] = 'Toddler/Baby'
        self.train.loc[((self.train['Age'] > 5) & (self.train['Age'] <= 12)), ['Age_cat_']] = 'Child'
        self.train.loc[((self.train['Age'] > 12) & (self.train['Age'] <= 18)), ['Age_cat_']] = 'Teen'
        self.train.loc[((self.train['Age'] > 18) & (self.train['Age'] <= 50)), ['Age_cat_']] = 'Adult'
        self.train.loc[((self.train['Age'] > 50) & (self.train['Age'] <= 150)), ['Age_cat_']] = 'Elderly'

    def pandas_execution(self):
        self.train[["Cabin1_", 'Cabin2_', 'Cabin3_']] = self.train['Cabin'].str.split('/', expand=True)
        self.train[["Pid1_", 'Pid2_']] = self.train['PassengerId'].str.split('_', expand=True)
        self.train[["Pid1_", 'Pid2_']] = self.train[["Pid1_", 'Pid2_']].astype('int')

        self.train[["Fname_", 'Lname_']] = self.train['Name'].str.split(' ', expand=True)

        self.train['sum_exp_'] = self.train['RoomService'] + self.train['FoodCourt']
        self.train['sum_exp_'] = self.train['sum_exp_'] + self.train['ShoppingMall']
        self.train['sum_exp_'] = self.train['sum_exp_'] + self.train['Spa']
        self.train['sum_exp_'] = self.train['sum_exp_'] + self.train['VRDeck']

        self.train['sum_exp_'] = self.train['sum_exp_'] / self.train['Pid2_']

        #Original call
        #tt['Age_cat_'] =pd.cut(tt.Age, bins=[0, 5, 12, 18, 50, 150], labels=['Toddler/Baby', 'Child', 'Teen', 'Adult', 'Elderly'])
        self.train.loc[self.train['Age'] <= 5, ['Age_cat_']] = 'Toddler/Baby'
        self.train.loc[((self.train['Age'] > 5) & (self.train['Age'] <= 12)), ['Age_cat_']] = 'Child'
        self.train.loc[((self.train['Age'] > 12) & (self.train['Age'] <= 18)), ['Age_cat_']] = 'Teen'
        self.train.loc[((self.train['Age'] > 18) & (self.train['Age'] <= 50)), ['Age_cat_']] = 'Adult'
        self.train.loc[((self.train['Age'] > 50) & (self.train['Age'] <= 150)), ['Age_cat_']] = 'Elderly'

    def modin_execution(self):
        self.pandas_execution()

    def snowpandas_execution(self):
        self.pandas_execution()

    def polars_execution(self):
        import polars as pl
        self.train = self.train.with_columns(pl.col('Cabin').str.split_exact('/', 2))
        self.train = self.train.with_columns([
                pl.col('Cabin').struct.field('field_0').alias('Deck'),
                pl.col('Cabin').struct.field('field_1').alias('Num'),
                pl.col('Cabin').struct.field('field_2').alias('Side'),
            ])
        self.train = self.train.with_columns(pl.col('PassengerId').str.split_exact('_', 1))
        self.train = self.train.with_columns([
            pl.col('PassengerId').struct.field('field_0').alias('Pid1_'),
            pl.col('PassengerId').struct.field('field_1').alias('Pid2_'),
        ])
        self.train = self.train.with_columns([
            pl.col('Pid1_').cast(pl.Int32).alias('Pid1_'),
            pl.col('Pid2_').cast(pl.Int32).alias('Pid2_')
        ])
        self.train = self.train.with_columns(pl.col('Name').str.split_exact(' ', 1))
        self.train = self.train.with_columns([
            pl.col('Name').struct.field('field_0').alias('Fname_'),
            pl.col('Name').struct.field('field_1').alias('Lname_'),
        ])
        self.train = self.train.with_columns([
            (pl.col('RoomService') + pl.col('FoodCourt') + pl.col('ShoppingMall') + pl.col('Spa') + pl.col('VRDeck')).alias('sum_exp_')
        ])
        self.train = self.train.with_columns([
            (pl.col('sum_exp_') / pl.col('Pid2_')).alias('sum_exp_')
        ])
        self.train = self.train.with_columns([
            pl.lit('AA').alias('Age_cat_'),  # Initialize 'AgeGroup' to 'AA'
        ])
        self.train = self.train.with_columns([
            pl.when(pl.col('Age') <= 5).then(pl.lit('Toddler/Baby')).otherwise(pl.col('Age_cat_')).alias('Age_cat_')
        ])
        self.train = self.train.with_columns([
            pl.when((pl.col('Age') > 5) & (pl.col('Age') <= 12)).then(pl.lit('Child')).otherwise(pl.col('Age_cat_')).alias('Age_cat_')
        ])
        self.train = self.train.with_columns([
            pl.when((pl.col('Age') > 12) & (pl.col('Age') <= 18)).then(pl.lit('Teen')).otherwise(pl.col('Age_cat_')).alias('Age_cat_')
        ])
        self.train = self.train.with_columns([
            pl.when((pl.col('Age') > 18) & (pl.col('Age') <= 50)).then(pl.lit('Adult')).otherwise(pl.col('Age_cat_')).alias('Age_cat_')
        ])
        self.train = self.train.with_columns([
            pl.when((pl.col('Age') > 50) & (pl.col('Age') <= 150)).then(pl.lit('Elderly')).otherwise(pl.col('Age_cat_')).alias('Age_cat_')
        ])

    def dask_execution(self):
        self.train[['Cabin1_', 'Cabin2_', 'Cabin3_']] = self.train['Cabin'].str.split('/', expand=True, n=2)
        self.train[['Pid1_', 'Pid2_']] = self.train['PassengerId'].str.split('_', expand=True, n=1)
        self.train[['Pid1_', 'Pid2_']] = self.train[['Pid1_', 'Pid2_']].astype('int')
        self.train[['Fname_', 'Lname_']] = self.train['Name'].str.split(' ', expand=True, n=1)
        self.train['sum_exp_'] = (self.train['RoomService'] +
                                self.train['FoodCourt'] +
                                self.train['ShoppingMall'] +
                                self.train['Spa'] +
                                self.train['VRDeck'])
        self.train['sum_exp_'] = self.train['sum_exp_'] / self.train['Pid2_']
        self.train['Age_cat_'] = 'Adult'  # Default category
        self.train = self.train.assign(
            Age_cat_=self.train['Age'].mask(self.train['Age'] <= 5, 'Toddler/Baby'),
        )
        self.train = self.train.assign(
            Age_cat_=self.train['Age'].mask((self.train['Age'] > 5) & (self.train['Age'] <= 12), 'Child'),
        )
        self.train = self.train.assign(
            Age_cat_=self.train['Age'].mask((self.train['Age'] > 12) & (self.train['Age'] <= 18), 'Teen'),
        )
        self.train = self.train.assign(
            Age_cat_=self.train['Age'].mask((self.train['Age'] > 18) & (self.train['Age'] <= 50), 'Adult'),
        )
        self.train = self.train.assign(
            Age_cat_=self.train['Age'].mask((self.train['Age'] > 50) & (self.train['Age'] <= 150), 'Elderly'),
        )

    def vaex_execution(self):
        """
        # Splitting strings into multiple columns
        self.train['Cabin1_'] = self.train['Cabin'].str.split('/').apply(lambda x: x[0])
        self.train['Cabin2_'] = self.train['Cabin'].str.split('/').apply(lambda x: x[1])
        self.train['Cabin3_'] = self.train['Cabin'].str.split('/').apply(lambda x: x[2])

        self.train['Pid1_'] = self.train['PassengerId'].str.split('_').apply(lambda x: x[0])
        self.train['Pid2_'] = self.train['PassengerId'].str.split('_').apply(lambda x: x[1])
        self.train['Pid1_'] = self.train['Pid1_'].astype('int')
        self.train['Pid2_'] = self.train['Pid2_'].astype('int')

        self.train['Fname_'] = self.train['Name'].str.split(' ').apply(lambda x: x[0])
        self.train['Lname_'] = self.train['Name'].str.split(' ').apply(lambda x: x[1])
        """
        # Safely split 'Cabin' into 'Cabin1_', 'Cabin2_', 'Cabin3_'
        self.train['Cabin1_'] = self.train['Cabin'].str.split('/').apply(lambda x: x[0] if x is not None and isinstance(x, list) and len(x) > 0 else None)
        self.train['Cabin2_'] = self.train['Cabin'].str.split('/').apply(lambda x: x[1] if x is not None and isinstance(x, list) and len(x) > 1 else None)
        self.train['Cabin3_'] = self.train['Cabin'].str.split('/').apply(lambda x: x[2] if x is not None and isinstance(x, list) and len(x) > 2 else None)

        # Safely split 'PassengerId' into 'Pid1_' and 'Pid2_', then convert to int
        self.train['Pid1_'] = self.train['PassengerId'].str.split('_').apply(lambda x: int(x[0]) if x is not None and isinstance(x, list) and len(x) > 0 else None)
        self.train['Pid2_'] = self.train['PassengerId'].str.split('_').apply(lambda x: int(x[1]) if x is not None and isinstance(x, list) and len(x) > 1 else None)

        # Safely split 'Name' into 'Fname_' and 'Lname_'
        self.train['Fname_'] = self.train['Name'].str.split(' ').apply(lambda x: x[0] if x is not None and isinstance(x, list) and len(x) > 0 else None)
        self.train['Lname_'] = self.train['Name'].str.split(' ').apply(lambda x: x[1] if x is not None and isinstance(x, list) and len(x) > 1 else None)

        # Summing multiple columns
        self.train['sum_exp_'] = self.train['RoomService'] + self.train['FoodCourt'] + self.train['ShoppingMall'] + self.train['Spa'] + self.train['VRDeck']

        # Dividing sum_exp_ by Pid2_
        self.train['sum_exp_'] = self.train['sum_exp_'] / self.train['Pid2_']
        # Assigning categories based on Age
        self.train['Age_cat_'] = self.train.func.where(self.train['Age'] <= 5, 'Toddler/Baby',
                            self.train.func.where((self.train['Age'] > 5) & (self.train['Age'] <= 12), 'Child',
                            self.train.func.where((self.train['Age'] > 12) & (self.train['Age'] <= 18), 'Teen',
                            self.train.func.where((self.train['Age'] > 18) & (self.train['Age'] <= 50), 'Adult',
                            self.train.func.where((self.train['Age'] > 50) & (self.train['Age'] <= 150), 'Elderly', None)))))

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
        """
        self.train = self.train.rename(columns=column_mapping)
        #! Note: split functions are executed eagerly
        self.train['Cabin1_'] = self.train['Cabin'].str.split('/').apply(lambda x: x[0])
        self.train['Cabin2_'] = self.train['Cabin'].str.split('/').apply(lambda x: x[1])
        self.train['Cabin3_'] = self.train['Cabin'].str.split('/').apply(lambda x: x[2])
        #! Note: split functions are executed eagerly
        self.train['Pid1_'] = self.train['PassengerId'].str.split('_').apply(lambda x: x[0])
        self.train['Pid2_'] = self.train['PassengerId'].str.split('_').apply(lambda x: x[1])

        self.train[["Pid1_", 'Pid2_']] = self.train[["Pid1_", 'Pid2_']].astype('int')
        #! Note: split functions are executed eagerly
        self.train['Fname_'] = self.train['Name'].str.split(' ').apply(lambda x: x[0])
        self.train['Lname_'] = self.train['Name'].str.split(' ').apply(lambda x: x[1])

        self.train['sum_exp_'] = self.train['RoomService'] + self.train['FoodCourt']
        self.train['sum_exp_'] = self.train['sum_exp_'] + self.train['ShoppingMall']
        self.train['sum_exp_'] = self.train['sum_exp_'] + self.train['Spa']
        self.train['sum_exp_'] = self.train['sum_exp_'] + self.train['VRDeck']

        self.train['sum_exp_'] = self.train['sum_exp_'] / self.train['Pid2_']

        #Original call
        #tt['Age_cat_'] =pd.cut(tt.Age, bins=[0, 5, 12, 18, 50, 150], labels=['Toddler/Baby', 'Child', 'Teen', 'Adult', 'Elderly'])
        self.train.loc[self.train['Age'] <= 5, ['Age_cat_']] = 'Toddler/Baby'
        self.train.loc[((self.train['Age'] > 5) & (self.train['Age'] <= 12)), ['Age_cat_']] = 'Child'
        self.train.loc[((self.train['Age'] > 12) & (self.train['Age'] <= 18)), ['Age_cat_']] = 'Teen'
        self.train.loc[((self.train['Age'] > 18) & (self.train['Age'] <= 50)), ['Age_cat_']] = 'Adult'
        self.train.loc[((self.train['Age'] > 50) & (self.train['Age'] <= 150)), ['Age_cat_']] = 'Elderly'
        """
        # Rename columns
        self.train = self.train.rename(columns=column_mapping)

        # Safely split 'Cabin' into 'Cabin1_', 'Cabin2_', 'Cabin3_'
        self.train['Cabin1_'] = self.train['Cabin'].str.split('/').apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None)
        self.train['Cabin2_'] = self.train['Cabin'].str.split('/').apply(lambda x: x[1] if isinstance(x, list) and len(x) > 1 else None)
        self.train['Cabin3_'] = self.train['Cabin'].str.split('/').apply(lambda x: x[2] if isinstance(x, list) and len(x) > 2 else None)

        # Safely split 'PassengerId' into 'Pid1_' and 'Pid2_', then ensure they're integers
        self.train['Pid1_'] = self.train['PassengerId'].str.split('_').apply(lambda x: int(x[0]) if isinstance(x, list) and len(x) > 0 else None)
        self.train['Pid2_'] = self.train['PassengerId'].str.split('_').apply(lambda x: int(x[1]) if isinstance(x, list) and len(x) > 1 else None)

        # Ensure there are no None values before converting to int
        self.train['Pid1_'] = self.train['Pid1_'].fillna(0).astype('int')
        self.train['Pid2_'] = self.train['Pid2_'].fillna(0).astype('int')

        # Safely split 'Name' into 'Fname_' and 'Lname_'
        self.train['Fname_'] = self.train['Name'].str.split(' ').apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None)
        self.train['Lname_'] = self.train['Name'].str.split(' ').apply(lambda x: x[1] if isinstance(x, list) and len(x) > 1 else None)

        # Calculate sum of expenses and divide by 'Pid2_'
        self.train['sum_exp_'] = (self.train['RoomService'] + self.train['FoodCourt'] + self.train['ShoppingMall'] +
                          self.train['Spa'] + self.train['VRDeck'])

        # Avoid division by zero or None in 'Pid2_'
        self.train['sum_exp_'] = self.train.apply(lambda row: row['sum_exp_'] / row['Pid2_'] if row['Pid2_'] != 0 else 0, axis=1)

        # Create 'Age_cat_' categories based on 'Age'
        self.train.loc[self.train['Age'] <= 5, 'Age_cat_'] = 'Toddler/Baby'
        self.train.loc[(self.train['Age'] > 5) & (self.train['Age'] <= 12), 'Age_cat_'] = 'Child'
        self.train.loc[(self.train['Age'] > 12) & (self.train['Age'] <= 18), 'Age_cat_'] = 'Teen'
        self.train.loc[(self.train['Age'] > 18) & (self.train['Age'] <= 50), 'Age_cat_'] = 'Adult'
        self.train.loc[(self.train['Age'] > 50) & (self.train['Age'] <= 150), 'Age_cat_'] = 'Elderly'
