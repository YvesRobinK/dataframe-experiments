from .task import Task


class Endeavor(Task):
    # link to the note book on kaggle:
    # https://www.kaggle.com/code/cv13j0/spaceship-titanic-higher-score-endeavor/notebook
    def __init__(self):
        super().__init__()
        self.name = "Endeavor"

    def spark_execution(self):
        self.train.loc[self.train['Age'] < 13, ['RoomService', 'FoodCourt', 'ShoppingMall', 'Spa', 'VRDeck']] = 0
        self.train.loc[self.train['CryoSleep'] == True, ['RoomService', 'FoodCourt', 'ShoppingMall', 'Spa', 'VRDeck']] = 0
        self.train['AgeGroup'] = 1.0
        self.train.loc[self.train['Age'] < 13, ['AgeGroup']] = 0.0

        # Fill NaNs values or with mean or most commond value...
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        numeric_tmp = self.train.select_dtypes(include = numerics)
        categ_tmp = self.train.select_dtypes(exclude = numerics)

        for col in numeric_tmp.columns:
            m = self.train[col].mean()
            self.train[col] = self.train[col].fillna(value=m)

        for col in categ_tmp.columns:
            mode = self.train[col].mode()[0]
            if self.engine == "spark" and (mode == False or mode == True):
                self.train[col] = self.train[col].astype(bool)
                mode = bool(mode)
                self.train[col] = self.train[col].fillna(value=mode)
            else:
                self.train[col] = self.train[col].fillna(value=mode)

        self.train['Total_Billed'] = self.train['RoomService'] + self.train['FoodCourt']
        self.train['Total_Billed'] = self.train['Total_Billed'] + self.train['ShoppingMall']
        self.train['Total_Billed'] = self.train['Total_Billed'] + self.train['Spa']
        self.train['Total_Billed'] = self.train['Total_Billed'] + self.train['VRDeck']

        from pyspark.sql.functions import split
        self.train = self.train.to_spark().withColumn('CabinDeck',split(self.train.to_spark()['Cabin'], '/')[0]).pandas_api()
        self.train = self.train.to_spark().withColumn('CabinNum',split(self.train.to_spark()['Cabin'], '/')[1]).pandas_api()
        self.train = self.train.to_spark().withColumn('CabinSide',split(self.train.to_spark()['Cabin'], '/')[2]).pandas_api()
        self.train = self.train.drop('Cabin', axis=1)
        self.train = self.train.to_spark().withColumn('FirstName',split(self.train.to_spark()['Name'], ' ')[0]).pandas_api()
        self.train = self.train.to_spark().withColumn('FamilyName',split(self.train.to_spark()['Name'], ' ')[1]).pandas_api()
        self.train = self.train.drop('Name', axis=1)
        self.train = self.train.to_spark().withColumn('TravelGroup',split(self.train.to_spark()['PassengerId'], '_')[0]).pandas_api()
        self.train = self.train.to_spark().withColumn('ID',split(self.train.to_spark()['PassengerId'], '_')[1]).pandas_api()
        self.train = self.train.drop('PassengerId', axis=1)

    def pandas_execution(self):
        self.train.loc[self.train['Age'] < 13, ['RoomService', 'FoodCourt', 'ShoppingMall', 'Spa', 'VRDeck']] = 0
        self.train.loc[self.train['CryoSleep'] == True, ['RoomService', 'FoodCourt', 'ShoppingMall', 'Spa', 'VRDeck']] = 0
        self.train['AgeGroup'] = 1.0
        self.train.loc[self.train['Age'] < 13, ['AgeGroup']] = 0.0
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        numeric_tmp = self.train.select_dtypes(include = numerics)
        categ_tmp = self.train.select_dtypes(exclude = numerics)

        for col in numeric_tmp.columns:
            m = self.train[col].mean()
            self.train[col] = self.train[col].fillna(value=m)

        for col in categ_tmp.columns:
            mode = self.train[col].mode()[0]
            self.train[col] = self.train[col].fillna(value=mode)

        self.train['Total_Billed'] = self.train['RoomService'] + self.train['FoodCourt']
        self.train['Total_Billed'] = self.train['Total_Billed'] + self.train['ShoppingMall']
        self.train['Total_Billed'] = self.train['Total_Billed'] + self.train['Spa']
        self.train['Total_Billed'] = self.train['Total_Billed'] + self.train['VRDeck']
        self.train[["CabinDeck", "CabinNum", "CabinSide"]] = self.train['Cabin'].str.split('/', expand=True)
        self.train.drop(columns = ['Cabin'], inplace = True)
        self.train[["FirstName", "FamilyName"]] = self.train['Name'].str.split(' ', expand=True)
        self.train.drop(columns = ['Name'], inplace = True)
        self.train[['TravelGroup', 'ID']] =  self.train['PassengerId'].str.split('_', expand = True)
        self.train.drop(columns=['PassengerId'], inplace= True)

    def modin_execution(self):
        self.pandas_execution()

    def snowpandas_execution(self):
        self.train.loc[self.train['Age'] < 13, ['RoomService', 'FoodCourt', 'ShoppingMall', 'Spa', 'VRDeck']] = 0
        self.train.loc[self.train['CryoSleep'] == True, ['RoomService', 'FoodCourt', 'ShoppingMall', 'Spa', 'VRDeck']] = 0
        self.train['AgeGroup'] = 1.0
        self.train.loc[self.train['Age'] < 13, ['AgeGroup']] = 0.0
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        numeric_tmp = self.train.select_dtypes(include = numerics)
        categ_tmp = self.train.select_dtypes(exclude = numerics)

        for col in numeric_tmp.columns:
            self.train[col] = self.train[col].fillna(method="snow_mean")

        for col in categ_tmp.columns:
            self.train[col] = self.train[col].fillna(method="snow_mode")

        self.train['Total_Billed'] = self.train['RoomService'] + self.train['FoodCourt']
        self.train['Total_Billed'] = self.train['Total_Billed'] + self.train['ShoppingMall']
        self.train['Total_Billed'] = self.train['Total_Billed'] + self.train['Spa']
        self.train['Total_Billed'] = self.train['Total_Billed'] + self.train['VRDeck']
        self.train[["CabinDeck", "CabinNum", "CabinSide"]] = self.train['Cabin'].str.split('/', expand=True)
        self.train.drop(columns = ['Cabin'], inplace = True)
        self.train[["FirstName", "FamilyName"]] = self.train['Name'].str.split(' ', expand=True)
        self.train.drop(columns = ['Name'], inplace = True)
        self.train[['TravelGroup', 'ID']] =  self.train['PassengerId'].str.split('_', expand = True)
        self.train.drop(columns=['PassengerId'], inplace= True)

    def polars_execution(self):
        import polars as pl
        columns_to_set_zero = ['RoomService', 'FoodCourt', 'ShoppingMall', 'Spa', 'VRDeck']
        self.train = self.train.with_columns([
            pl.when(pl.col('Age') < 13).then(0).otherwise(pl.col(col)).alias(col) for col in columns_to_set_zero
        ])
        columns_to_set_zero = ['RoomService', 'FoodCourt', 'ShoppingMall', 'Spa', 'VRDeck']
        self.train = self.train.with_columns([
            pl.when(pl.col('CryoSleep') == True).then(0).otherwise(pl.col(col)).alias(col) for col in columns_to_set_zero
        ])
        self.train = self.train.with_columns([
            pl.lit(1.0).alias('AgeGroup'),  # Initialize 'AgeGroup' to 1.0
        ])
        self.train = self.train.with_columns([
            pl.when(pl.col('Age') < 13).then(0.0).otherwise(pl.col('AgeGroup')).alias('AgeGroup')
        ])
        numerics = [pl.Int16, pl.Int32, pl.Int64, pl.Float32, pl.Float64]
        numeric_tmp = self.train.select([pl.col(c) for c in self.train.collect_schema().names() if self.train.collect_schema()[c] in numerics])
        categ_tmp = self.train.select([pl.col(c) for c in self.train.collect_schema().names() if self.train.collect_schema()[c] not in numerics])
        categ_tmp = ['HomePlanet', 'CryoSleep', 'Cabin', 'Destination', 'VIP', 'Name', 'Transported']
        self.train = self.train.with_columns([
            pl.when(pl.col(col).is_null()).then(pl.col(col).mean()).otherwise(pl.col(col)).alias(col) for col in numeric_tmp.collect_schema().names()
        ])
        self.train = self.train.with_columns([
            pl.when(pl.col(col).is_null()).then(pl.col(col).mode()).otherwise(pl.col(col)).alias(col) for col in categ_tmp
        ])
        self.train = self.train.with_columns([
            (pl.col("RoomService") + pl.col("FoodCourt") + pl.col("ShoppingMall") + pl.col("Spa") + pl.col("VRDeck")).alias("Total_Billed")
        ])
        self.train = self.train.with_columns(pl.col('Cabin').str.split_exact('/', 2))
        self.train = self.train.with_columns([
            pl.col('Cabin').struct.field('field_0').alias('Deck'),
            pl.col('Cabin').struct.field('field_1').alias('Num'),
            pl.col('Cabin').struct.field('field_2').alias('Side'),
        ])
        self.train = self.train.drop('Cabin')
        self.train = self.train.with_columns(pl.col('Name').str.split_exact(' ', 1))
        self.train = self.train.with_columns([
            pl.col('Name').struct.field('field_0').alias('FirstName'),
            pl.col('Name').struct.field('field_1').alias('FamilyName'),
        ])
        self.train = self.train.drop('Name')
        self.train = self.train.with_columns(pl.col('PassengerId').str.split_exact('_', 1))
        self.train = self.train.with_columns([
            pl.col('PassengerId').struct.field('field_0').alias('TravelGroup'),
            pl.col('PassengerId').struct.field('field_1').alias('ID'),
        ])
        self.train = self.train.drop('PassengerId')

    def dask_execution(self):
        # self.train.loc[self.train['Age'] < 13, ['RoomService', 'FoodCourt', 'ShoppingMall', 'Spa', 'VRDeck']] = 0
        self.train = self.train.assign(
            RoomService=self.train['RoomService'].where(~(self.train['Age'] < 13), 0),
            FoodCourt=self.train['FoodCourt'].where(~(self.train['Age'] < 13), 0),
            ShoppingMall=self.train['ShoppingMall'].where(~(self.train['Age'] < 13), 0),
            Spa=self.train['Spa'].where(~(self.train['Age'] < 13), 0),
            VRDeck=self.train['VRDeck'].where(~(self.train['Age'] < 13), 0),
        )
        # self.train.loc[self.train['CryoSleep'] == True, ['RoomService', 'FoodCourt', 'ShoppingMall', 'Spa', 'VRDeck']] = 0
        self.train = self.train.assign(
            RoomService=self.train['RoomService'].where(~(self.train['CryoSleep'] == True), 0),
            FoodCourt=self.train['FoodCourt'].where(~(self.train['CryoSleep'] == True), 0),
            ShoppingMall=self.train['ShoppingMall'].where(~(self.train['CryoSleep'] == True), 0),
            Spa=self.train['Spa'].where(~(self.train['CryoSleep'] == True), 0),
            VRDeck=self.train['VRDeck'].where(~(self.train['CryoSleep'] == True), 0),
        )
        self.train['AgeGroup'] = 1.0
        # self.train.loc[self.train['Age'] < 13, ['AgeGroup']] = 0.0
        self.train['AgeGroup'] = self.train['AgeGroup'].where(~(self.train['Age'] < 13), 0.0)

        # Selecting numeric and categorical columns
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        numeric_tmp = self.train.select_dtypes(include=numerics)
        categ_tmp = self.train.select_dtypes(exclude=numerics)

        def compute_mean_and_fillna(df, col):
            m = df[col].mean()
            df[col] = df[col].fillna(m)
            return df

        def compute_mode_and_fillna(df, col):
            m = df[col].mode()[0]
            df[col] = df[col].fillna(m)
            return df

        # Fill missing numeric columns with mean
        for col in numeric_tmp.columns:
            # you should use the below syntax to maintain the lazy execution
            self.train = self.train.map_partitions(compute_mean_and_fillna, col=col)

        # Fill missing categorical columns with mode
        for col in categ_tmp.columns:
            # you should use the below syntax to maintain the lazy execution
            self.train = self.train.map_partitions(compute_mode_and_fillna, col=col)

        self.train['Total_Billed'] = self.train['RoomService'] + self.train['FoodCourt'] + self.train['ShoppingMall'] + self.train['Spa'] + self.train['VRDeck']

        self.train[['CabinDeck', 'CabinNum', 'CabinSide']] = self.train['Cabin'].str.split('/', expand=True, n=2)
        self.train = self.train.drop(columns=['Cabin'])

        self.train[['FirstName', 'FamilyName']] = self.train['Name'].str.split(' ', expand=True, n=1)
        self.train = self.train.drop(columns=['Name'])

        self.train[['TravelGroup', 'ID']] = self.train['PassengerId'].str.split('_', expand=True, n=1)
        self.train = self.train.drop(columns=['PassengerId'])

    def vaex_execution(self):
        # raise 'The implementation for VAEX of Endeavor note book is not completed due to function mode, see todo below'
        self.train['RoomService'] = self.train.func.where(self.train['Age'] < 13, 0, self.train['RoomService'])
        self.train['FoodCourt'] = self.train.func.where(self.train['Age'] < 13, 0, self.train['FoodCourt'])
        self.train['ShoppingMall'] = self.train.func.where(self.train['Age'] < 13, 0, self.train['ShoppingMall'])
        self.train['Spa'] = self.train.func.where(self.train['Age'] < 13, 0, self.train['Spa'])
        self.train['VRDeck'] = self.train.func.where(self.train['Age'] < 13, 0, self.train['VRDeck'])

        # Set values to 0 for CryoSleep
        self.train['RoomService'] = self.train.func.where(self.train['CryoSleep'] == True, 0, self.train['RoomService'])
        self.train['FoodCourt'] = self.train.func.where(self.train['CryoSleep'] == True, 0, self.train['FoodCourt'])
        self.train['ShoppingMall'] = self.train.func.where(self.train['CryoSleep'] == True, 0, self.train['ShoppingMall'])
        self.train['Spa'] = self.train.func.where(self.train['CryoSleep'] == True, 0, self.train['Spa'])
        self.train['VRDeck'] = self.train.func.where(self.train['CryoSleep'] == True, 0, self.train['VRDeck'])

        # Create 'AgeGroup' column and set values
        self.train['AgeGroup'] = self.train.func.where(self.train['Age'] >= 13, 1.0, 0)
        numeric_cols = self.train.get_column_names(dtype=['int16', 'int32', 'int64', 'float16', 'float32', 'float64'])
        # Fill numeric columns with mean
        for col in numeric_cols:
            mean_value = self.train[col].mean()
            self.train[col] = self.train[col].fillna(mean_value)

        # Fill categorical columns with mode
        categorical_cols = self.train.get_column_names(dtype=['string', 'bool'])
        for col in categorical_cols:
            #! todo: correct implementation of mode may not be possible, the below implementattion does
            #! not result into the desired output
            import vaex
            grouped = self.train.groupby(by=col, agg={'count': vaex.agg.count()})
            mode_value = grouped[grouped['count'] == grouped['count'].max()][col].values[0]
            #! default mode api from VAEX does not work, it seems that it only works for floats/ints
            # mode_value = self.train.mode(self.train[col])
            self.train[col] = self.train[col].fillna(str(mode_value))
        """
        # Split 'Cabin' into 'CabinDeck', 'CabinNum', 'CabinSide'
        self.train['CabinDeck'] = self.train['Cabin'].str.split('/').apply(lambda x: x[0])
        self.train['CabinNum'] = self.train['Cabin'].str.split('/').apply(lambda x: x[1])
        self.train['CabinSide'] = self.train['Cabin'].str.split('/').apply(lambda x: x[2])
        self.train = self.train.drop(['Cabin'])

        # Split 'Name' into 'FirstName' and 'FamilyName'
        self.train['FirstName'] = self.train['Name'].str.split(' ').apply(lambda x: x[0])
        self.train['FamilyName'] = self.train['Name'].str.split(' ').apply(lambda x: x[1])
        self.train = self.train.drop(['Name'])

        # Split 'PassengerId' into 'TravelGroup' and 'ID'
        self.train['TravelGroup'] = self.train['PassengerId'].str.split('_').apply(lambda x: x[0])
        self.train['ID'] = self.train['PassengerId'].str.split('_').apply(lambda x: x[1])
        self.train = self.train.drop(['PassengerId'])

        # Calculate 'Total_Billed' as the sum of specific columns
        self.train['Total_Billed'] = (self.train['RoomService'] + self.train['FoodCourt'] +
                                    self.train['ShoppingMall'] + self.train['Spa'] +
                                    self.train['VRDeck'])
        """
        # Split 'Cabin' into 'CabinDeck', 'CabinNum', 'CabinSide'
        self.train['CabinDeck'] = self.train['Cabin'].str.split('/').apply(lambda x: x[0] if x is not None and isinstance(x, list) and len(x) > 0 else None)
        self.train['CabinNum'] = self.train['Cabin'].str.split('/').apply(lambda x: x[1] if x is not None and isinstance(x, list) and len(x) > 1 else None)
        self.train['CabinSide'] = self.train['Cabin'].str.split('/').apply(lambda x: x[2] if x is not None and isinstance(x, list) and len(x) > 2 else None)
        self.train = self.train.drop(['Cabin'])

        # Split 'Name' into 'FirstName' and 'FamilyName'
        self.train['FirstName'] = self.train['Name'].str.split(' ').apply(lambda x: x[0] if x is not None and isinstance(x, list) and len(x) > 0 else None)
        self.train['FamilyName'] = self.train['Name'].str.split(' ').apply(lambda x: x[1] if x is not None and isinstance(x, list) and len(x) > 1 else None)
        self.train = self.train.drop(['Name'])

        # Split 'PassengerId' into 'TravelGroup' and 'ID'
        self.train['TravelGroup'] = self.train['PassengerId'].str.split('_').apply(lambda x: x[0] if x is not None and isinstance(x, list) and len(x) > 0 else None)
        self.train['ID'] = self.train['PassengerId'].str.split('_').apply(lambda x: x[1] if x is not None and isinstance(x, list) and len(x) > 1 else None)
        self.train = self.train.drop(['PassengerId'])

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
        self.train.loc[self.train['Age'] < 13, ['RoomService', 'FoodCourt', 'ShoppingMall', 'Spa', 'VRDeck']] = 0
        self.train.loc[self.train['CryoSleep'] == True, ['RoomService', 'FoodCourt', 'ShoppingMall', 'Spa', 'VRDeck']] = 0
        self.train['AgeGroup'] = 1.0
        self.train.loc[self.train['Age'] < 13, ['AgeGroup']] = 0.0
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        numeric_tmp = self.train.select_dtypes(include = numerics)
        categ_tmp = self.train.select_dtypes(exclude = numerics)

        for col in numeric_tmp.columns:
            m = self.train[col].mean()
            self.train[col] = self.train[col].fillna(value=m)

        for col in categ_tmp.columns:
            #! note: NotImplementedError: Snowpark pandas does not yet support the method Series.mode
            #raise 'NotImplementedError: Snowpark pandas does not yet support the method Series.mode'
            grouped = self.train.groupby(col).size().reset_index(name='count')
            mode = grouped[col][0]
            self.train[col] = self.train[col].fillna(value=mode)

        self.train['Total_Billed'] = self.train['RoomService'] + self.train['FoodCourt']
        self.train['Total_Billed'] = self.train['Total_Billed'] + self.train['ShoppingMall']
        self.train['Total_Billed'] = self.train['Total_Billed'] + self.train['Spa']
        self.train['Total_Billed'] = self.train['Total_Billed'] + self.train['VRDeck']
        #! note: split funtion is executed eagerly, it's very slow
        self.train["CabinDeck"] = self.train['Cabin'].str.split('/')[0]
        self.train["CabinNum"] = self.train['Cabin'].str.split('/')[1]
        self.train["CabinSide"] = self.train['Cabin'].str.split('/')[2]
        self.train.drop(columns = ['Cabin'], inplace = True)
        #! note: split funtion is executed eagerly, it's very slow
        self.train["FirstName"] = self.train['Name'].str.split(' ')[0]
        self.train["FamilyName"] = self.train['Name'].str.split(' ')[1]
        self.train.drop(columns = ['Name'], inplace = True)
        #! note: split funtion is executed eagerly, it's very slow
        self.train['TravelGroup'] =  self.train['PassengerId'].str.split('_')[0]
        self.train['ID'] =  self.train['PassengerId'].str.split('_')[1]
        self.train.drop(columns=['PassengerId'], inplace= True)
