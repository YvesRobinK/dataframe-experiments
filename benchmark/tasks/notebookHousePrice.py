from .task import Task


class HousePrice(Task):
    # link to the note book on kaggle:
    # https://www.kaggle.com/code/darkside92/detailed-examination-for-house-price-top-10
    def __init__(self):
        super().__init__()
        self.name = "HousePricePrediction"

    def spark_execution(self):
        self.pandas_execution()

    def pandas_execution(self):
        self.train.loc[self.train['Fireplaces']==0,'FireplaceQu'] = 'Nothing'

        # Original query is line below. As transform is not implemented in our dataframe, we would replace it with a random float 65
        # self.train['LotFrontage'] = self.train['LotFrontage'].fillna(self.train.groupby('1stFlrSF')['LotFrontage'].transform('mean'))
        self.train['LotFrontage'] = self.train['LotFrontage'].fillna(65)

        # self.train['LotFrontage'] = self.train['LotFrontage'].interpolate(method='linear',inplace=True)
        self.train['LotFrontage'] = self.train['LotFrontage'].fillna(4)
        self.train['LotFrontage'] = self.train['LotFrontage'].astype(int)

        # original query is below. as transform is not implemented in our dataframe we would replace it with a random float 66
        # self.train['MasVnrArea'] = self.train['MasVnrArea'].fillna(self.train.groupby('MasVnrType')['MasVnrArea'].transform('mean'))
        # self.train['MasVnrArea'].interpolate(method='linear',inplace=True)
        self.train['MasVnrArea'] = self.train['MasVnrArea'].fillna(55)
        self.train['MasVnrArea'] = self.train['MasVnrArea'].astype(int)
        self.train["Fence"] = self.train["Fence"].fillna("None")
        self.train["FireplaceQu"] = self.train["FireplaceQu"].fillna("None")
        self.train["Alley"] = self.train["Alley"].fillna("None")
        self.train["PoolQC"] = self.train["PoolQC"].fillna("None")

        self.train["MiscFeature"] = self.train["MiscFeature"].fillna("None")
        self.train.loc[self.train['BsmtFinSF1'] == 0,'BsmtFinType1'] = 'Unf'
        self.train.loc[self.train['BsmtFinSF2'] == 0,'BsmtQual'] = 'TA'
        self.train['YrBltRmd'] = self.train['YearBuilt'] + self.train['YearRemodAdd']

        self.train['Total_Square_Feet'] = self.train['1stFlrSF'] + self.train['TotalBsmtSF']
        self.train['Total_Square_Feet'] = self.train['Total_Square_Feet'] + self.train['2ndFlrSF']
        self.train['Total_Square_Feet'] = self.train['Total_Square_Feet'] + self.train['BsmtFinSF1']
        self.train['Total_Square_Feet'] = self.train['Total_Square_Feet'] + self.train['BsmtFinSF2']

        self.train['Total_Bath'] = (self.train['FullBath'] + (0.5 * self.train['HalfBath']) + self.train['BsmtFullBath'] + (0.5 * self.train['BsmtHalfBath']))
        """
        self.train['Total_Bath'] = self.train['FullBath'] + self.train['HalfBath']
        self.train['Total_Bath'] = self.train['Total_Bath'] + self.train['BsmtFullBath']
        self.train['Total_Bath'] = self.train['Total_Bath'] + self.train['BsmtHalfBath']
        """
        self.train['Total_Porch_Area'] = self.train['OpenPorchSF'] + self.train['3SsnPorch']
        self.train['Total_Porch_Area'] = self.train['Total_Porch_Area'] + self.train['EnclosedPorch']
        self.train['Total_Porch_Area'] = self.train['Total_Porch_Area'] + self.train['ScreenPorch']
        self.train['Total_Porch_Area'] = self.train['Total_Porch_Area'] + self.train['WoodDeckSF']

        # self.train['exists_pool'] = self.train['PoolArea'].apply(lambda x: 1 if x > 0 else 0) REPLACED BY THE FOLLOWING LINE
        self.train['exists_pool'] = 0
        self.train.loc[self.train['PoolArea'] > 0 ,'exists_pool'] = 1
        # self.train['exists_garage'] = self.train['GarageArea'].apply(lambda x: 1 if x > 0 else 0) REPLACED BY THE FOLLOWING LINE
        self.train['exists_garage'] = 0
        self.train.loc[self.train['GarageArea'] > 0, 'exists_garage'] = 1
        # self.train['exists_fireplace'] = self.train['Fireplaces'].apply(lambda x: 1 if x > 0 else 0) REPLACED BY THE FOLLOWING LINE
        self.train['exists_fireplace'] = 0
        self.train.loc[self.train['Fireplaces'] > 0, 'exists_fireplace'] = 1
        # self.train['exists_bsmt'] = self.train['TotalBsmtSF'].apply(lambda x: 1 if x > 0 else 0) REPLACED BY THE FOLLOWING LINE
        self.train['exists_bsmt'] = 0
        self.train.loc[self.train['TotalBsmtSF'] > 0, 'exists_bsmt'] = 1
        # self.train['old_house'] = self.train['YearBuilt'].apply(lambda x: 1 if x <1990 else 0) REPLACED BY THE FOLLOWING LINE
        self.train['old_house'] = 0
        self.train.loc[self.train['YearBuilt'] < 1990, 'old_house'] = 1

    def modin_execution(self):
        self.pandas_execution()

    def snowpandas_execution(self):
        ##
        # Note that 3 columns in snowpandas version is different from the original dataset:
        # 1. FirstFlrSF: 1stFlrSF   2. SecondFlrSF: 2ndFlrSF    3. SsnPorch: 3SsnPorch
        # Because the name of these columns starts with a digit, when they are saved in the database,
        # their name have ''. to get rid of the '' around the name, I changed the name of these 3 columns.
        ##
        self.train.loc[self.train['Fireplaces'] == 0,['FireplaceQu']] = 'Nothing'

        # Original query is line below. As transform is not implemented in our dataframe, we would replace it with a random float 65
        # self.train['LotFrontage'] = self.train['LotFrontage'].fillna(self.train.groupby('1stFlrSF')['LotFrontage'].transform('mean'))
        self.train['LotFrontage'] = self.train['LotFrontage'].fillna(65)

        # self.train['LotFrontage'] = self.train['LotFrontage'].interpolate(method='linear',inplace=True)
        self.train['LotFrontage'] = self.train['LotFrontage'].fillna(4)
        self.train['LotFrontage'] = self.train['LotFrontage'].astype(int)

        # original query is below. as transform is not implemented in our dataframe we would replace it with a random float 66
        # self.train['MasVnrArea'] = self.train['MasVnrArea'].fillna(self.train.groupby('MasVnrType')['MasVnrArea'].transform('mean'))
        # self.train['MasVnrArea'].interpolate(method='linear',inplace=True)
        self.train['MasVnrArea'] = self.train['MasVnrArea'].fillna(55)
        self.train['MasVnrArea'] = self.train['MasVnrArea'].astype(int)
        self.train["Fence"] = self.train["Fence"].fillna("None")
        self.train["FireplaceQu"] = self.train["FireplaceQu"].fillna("None")
        self.train["Alley"] = self.train["Alley"].fillna("None")
        self.train["PoolQC"] = self.train["PoolQC"].fillna("None")

        self.train["MiscFeature"] = self.train["MiscFeature"].fillna("None")
        #! error: when self.train['BsmtFinSF1'] == 0 is executed, type of 'BsmtFinSF1' column is changed to boolean which may cause further errors.
        self.train.loc[self.train['BsmtFinSF1'] == 0,['BsmtFinType1']] = 'Unf'
        self.train.loc[self.train['BsmtFinSF2'] == 0,['BsmtQual']] = 'TA'
        
        
        self.train['YrBltRmd'] = self.train['YearBuilt'] + self.train['YearRemodAdd']
        
        self.train['Total_Square_Feet'] = self.train['FirstFlrSF'] + self.train['TotalBsmtSF']
        self.train['Total_Square_Feet'] = self.train['Total_Square_Feet'] + self.train['SecondFlrSF']
        self.train['Total_Square_Feet'] = self.train['Total_Square_Feet'] + self.train['BsmtFinSF1']
        self.train['Total_Square_Feet'] = self.train['Total_Square_Feet'] + self.train['BsmtFinSF2']

        #! error: 0.5 * col is not defined!
        #### self.train['Total_Bath'] = (self.train['FullBath'] + (0.5 * self.train['HalfBath']) + self.train['BsmtFullBath'] + (0.5 * self.train['BsmtHalfBath']))

        self.train['Total_Bath'] = self.train['HalfBath'] + self.train['BsmtHalfBath']
        self.train['Total_Bath'] = 0.5 * self.train['Total_Bath']
        self.train['Total_Bath'] = self.train['Total_Bath'] + self.train['FullBath']
        self.train['Total_Bath'] = self.train['Total_Bath'] + self.train['BsmtFullBath']

        self.train['Total_Bath'] = self.train['FullBath'] + self.train['HalfBath']
        self.train['Total_Bath'] = self.train['Total_Bath'] + self.train['BsmtFullBath']
        self.train['Total_Bath'] = self.train['Total_Bath'] + self.train['BsmtHalfBath']

        self.train['Total_Porch_Area'] = self.train['OpenPorchSF'] + self.train['SsnPorch']
        self.train['Total_Porch_Area'] = self.train['Total_Porch_Area'] + self.train['EnclosedPorch']
        self.train['Total_Porch_Area'] = self.train['Total_Porch_Area'] + self.train['ScreenPorch']
        self.train['Total_Porch_Area'] = self.train['Total_Porch_Area'] + self.train['WoodDeckSF']
        

        # self.train['exists_pool'] = self.train['PoolArea'].apply(lambda x: 1 if x > 0 else 0) REPLACED BY THE FOLLOWING LINE
        self.train['exists_pool'] = 0
        self.train.loc[self.train['PoolArea'] > 0 ,['exists_pool']] = 1
        # self.train['exists_garage'] = self.train['GarageArea'].apply(lambda x: 1 if x > 0 else 0) REPLACED BY THE FOLLOWING LINE
        self.train['exists_garage'] = 0
        self.train.loc[self.train['GarageArea'] > 0, ['exists_garage']] = 1
        # self.train['exists_fireplace'] = self.train['Fireplaces'].apply(lambda x: 1 if x > 0 else 0) REPLACED BY THE FOLLOWING LINE
        self.train['exists_fireplace'] = 0
        #self.train.loc[self.train['Fireplaces'] > 0, 'exists_fireplace'] = 1
        # self.train['exists_bsmt'] = self.train['TotalBsmtSF'].apply(lambda x: 1 if x > 0 else 0) REPLACED BY THE FOLLOWING LINE
        self.train['exists_bsmt'] = 0
        self.train.loc[self.train['TotalBsmtSF'] > 0, ['exists_bsmt']] = 1
        # self.train['old_house'] = self.train['YearBuilt'].apply(lambda x: 1 if x <1990 else 0) REPLACED BY THE FOLLOWING LINE
        self.train['old_house'] = 0
        self.train.loc[self.train['YearBuilt'] < 1990, ['old_house']] = 1
        #! error after finishing the execution: batches = self.train._query_compiler._modin_frame._frame._frame.to_pandas_batches()


    def polars_execution(self):
        import polars as pl
        self.train = self.train.with_columns([
            pl.when(pl.col('Fireplaces') == 0).then(pl.lit('Nothing')).otherwise(pl.col('FireplaceQu')).alias('FireplaceQu')
        ])
        self.train = self.train.with_columns([
            pl.when(pl.col('LotFrontage').is_null()).then(6665).otherwise(pl.col('LotFrontage')).alias('LotFrontage')
        ])
        self.train = self.train.with_columns([
            pl.when(pl.col('LotFrontage').is_null()).then(4).otherwise(pl.col('LotFrontage')).alias('LotFrontage')
        ])
        self.train = self.train.with_columns([
            pl.col('LotFrontage').cast(pl.Int32).alias('LotFrontage'),
        ])
        self.train = self.train.with_columns([
            pl.when(pl.col('MasVnrArea').is_null()).then(55).otherwise(pl.col('MasVnrArea')).alias('MasVnrArea')
        ])
        self.train = self.train.with_columns([
            pl.col('MasVnrArea').cast(pl.Int32).alias('MasVnrArea'),
        ])
        self.train = self.train.with_columns([
            pl.col('Fence').str.replace('NA', 'None').alias('Fence')
        ])
        self.train = self.train.with_columns([
            pl.col('FireplaceQu').str.replace('NA', 'None').alias('FireplaceQu')
        ])
        self.train = self.train.with_columns([
            pl.col('Alley').str.replace('NA', 'None').alias('Alley')
        ])
        self.train = self.train.with_columns([
            pl.col('PoolQC').str.replace('NA', 'None').alias('PoolQC')
        ])
        self.train = self.train.with_columns([
            pl.col('MiscFeature').str.replace('NA', 'None').alias('MiscFeature')
        ])
        self.train = self.train.with_columns([
            pl.when(pl.col('BsmtFinSF1') == 0).then(pl.lit('Unf')).otherwise(pl.col('BsmtFinType1')).alias('BsmtFinType1')
        ])
        self.train = self.train.with_columns([
            pl.when(pl.col('BsmtFinSF2') == 0).then(pl.lit('TA')).otherwise(pl.col('BsmtQual')).alias('BsmtQual')
        ])
        self.train = self.train.with_columns([
            (pl.col('YearBuilt') + pl.col('YearRemodAdd')).alias('YrBltRmd')
        ])

    def dask_execution(self):
        self.train['FireplaceQu'] = self.train['FireplaceQu'].mask(self.train['Fireplaces'] == 0, 'Nothing')
        self.train['LotFrontage'] = self.train['LotFrontage'].fillna(65)
        self.train['LotFrontage'] = self.train['LotFrontage'].fillna(4)
        self.train['LotFrontage'] = self.train['LotFrontage'].astype(int)

        self.train['MasVnrArea'] = self.train['MasVnrArea'].fillna(55)
        self.train['MasVnrArea'] = self.train['MasVnrArea'].astype(int)

        self.train["Fence"] = self.train["Fence"].fillna("None")
        self.train["FireplaceQu"] = self.train["FireplaceQu"].fillna("None")
        self.train["Alley"] = self.train["Alley"].fillna("None")
        self.train["PoolQC"] = self.train["PoolQC"].fillna("None")
        self.train["MiscFeature"] = self.train["MiscFeature"].fillna("None")

        self.train['BsmtFinType1'] = self.train['BsmtFinType1'].mask(self.train['BsmtFinSF1'] == 0, 'Unf')
        self.train['BsmtQual'] = self.train['BsmtQual'].mask(self.train['BsmtFinSF2'] == 0, 'TA')

        self.train['YrBltRmd'] = self.train['YearBuilt'] + self.train['YearRemodAdd']
        self.train['Total_Square_Feet'] = (self.train['1stFlrSF'] + self.train['TotalBsmtSF'] +
                                        self.train['2ndFlrSF'] + self.train['BsmtFinSF1'] + self.train['BsmtFinSF2'])

        self.train['Total_Bath'] = (self.train['FullBath'] + (0.5* self.train['HalfBath']) +
                                    self.train['BsmtFullBath'] + (0.5 *self.train['BsmtHalfBath']))

        self.train['Total_Porch_Area'] = (self.train['OpenPorchSF'] + self.train['3SsnPorch'] +
                                        self.train['EnclosedPorch'] + self.train['ScreenPorch'] + self.train['WoodDeckSF'])

        self.train['exists_pool'] = (self.train['PoolArea'] > 0).astype(int)
        self.train['exists_garage'] = (self.train['GarageArea'] > 0).astype(int)
        self.train['exists_fireplace'] = (self.train['Fireplaces'] > 0).astype(int)
        self.train['exists_bsmt'] = (self.train['TotalBsmtSF'] > 0).astype(int)
        self.train['old_house'] = (self.train['YearBuilt'] < 1990).astype(int)

    def vaex_execution(self):
        # Replacing values based on conditions
        self.train['FireplaceQu'] = self.train.func.where(self.train['Fireplaces'] == 0, 'Nothing', self.train['FireplaceQu'])
        # Filling NaN values
        self.train['LotFrontage'] = self.train['LotFrontage'].fillna(65)
        self.train['LotFrontage'] = self.train['LotFrontage'].fillna(4)
        self.train['LotFrontage'] = self.train['LotFrontage'].astype('int')

        self.train['MasVnrArea'] = self.train['MasVnrArea'].fillna(55)
        self.train['MasVnrArea'] = self.train['MasVnrArea'].astype('int')

        self.train['Fence'] = self.train['Fence'].fillna("None")
        self.train['FireplaceQu'] = self.train['FireplaceQu'].fillna("None")
        self.train['Alley'] = self.train['Alley'].fillna("None")
        self.train['PoolQC'] = self.train['PoolQC'].fillna("None")
        self.train['MiscFeature'] = self.train['MiscFeature'].fillna("None")

        # Replacing values based on conditions
        self.train['BsmtFinType1'] = self.train.func.where(self.train['BsmtFinSF1'] == 0, 'Unf', self.train['BsmtFinType1'])
        self.train['BsmtQual'] = self.train.func.where(self.train['BsmtFinSF2'] == 0, 'TA', self.train['BsmtQual'])

        # Creating new columns based on existing ones
        self.train['YrBltRmd'] = self.train['YearBuilt'] + self.train['YearRemodAdd']

        self.train['Total_Square_Feet'] = self.train['1stFlrSF'] + self.train['TotalBsmtSF'] + self.train['2ndFlrSF'] + self.train['BsmtFinSF1'] + self.train['BsmtFinSF2']

        self.train['Total_Bath'] = self.train['FullBath'] + (0.5 * self.train['HalfBath']) + self.train['BsmtFullBath'] + (0.5 * self.train['BsmtHalfBath'])

        self.train['Total_Porch_Area'] = self.train['OpenPorchSF'] + self.train['3SsnPorch'] + self.train['EnclosedPorch'] + self.train['ScreenPorch'] + self.train['WoodDeckSF']


        self.train['exists_pool'] = self.train.func.where(self.train['PoolArea'] > 0, 1, 0)
        self.train['exists_garage'] = self.train.func.where(self.train['GarageArea'] > 0, 1, 0)
        self.train['exists_fireplace'] = self.train.func.where(self.train['Fireplaces'] > 0, 1, 0)
        self.train['exists_bsmt'] = self.train.func.where(self.train['TotalBsmtSF'] > 0, 1, 0)
        self.train['old_house'] = self.train.func.where(self.train['YearBuilt'] < 1990, 1, 0)

    def snowparkpandas_execution(self):
        column_mapping = {
            'ID': 'Id',
            'MSSUBCLASS': 'MSSubClass',
            'MSZONING': 'MSZoning',
            'LOTFRONTAGE': 'LotFrontage',
            'LOTAREA': 'LotArea',
            'STREET': 'Street',
            'ALLEY': 'Alley',
            'LOTSHAPE': 'LotShape',
            'LANDCONTOUR': 'LandContour',
            'UTILITIES': 'Utilities',
            'LOTCONFIG': 'LotConfig',
            'LANDSLOPE': 'LandSlope',
            'NEIGHBORHOOD': 'Neighborhood',
            'CONDITION1': 'Condition1',
            'CONDITION2': 'Condition2',
            'BLDGTYPE': 'BldgType',
            'HOUSESTYLE': 'HouseStyle',
            'OVERALLQUAL': 'OverallQual',
            'OVERALLCOND': 'OverallCond',
            'YEARBUILT': 'YearBuilt',
            'YEARREMODADD': 'YearRemodAdd',
            'ROOFSTYLE': 'RoofStyle',
            'ROOFMATL': 'RoofMatl',
            'EXTERIOR1ST': 'Exterior1st',
            'EXTERIOR2ND': 'Exterior2nd',
            'MASVNRTYPE': 'MasVnrType',
            'MASVNRAREA': 'MasVnrArea',
            'EXTERQUAL': 'ExterQual',
            'EXTERCOND': 'ExterCond',
            'FOUNDATION': 'Foundation',
            'BSMTQUAL': 'BsmtQual',
            'BSMTCOND': 'BsmtCond',
            'BSMTEXPOSURE': 'BsmtExposure',
            'BSMTFINTYPE1': 'BsmtFinType1',
            'BSMTFINSF1': 'BsmtFinSF1',
            'BSMTFINTYPE2': 'BsmtFinType2',
            'BSMTFINSF2': 'BsmtFinSF2',
            'BSMTUNFSF': 'BsmtUnfSF',
            'TOTALBSMTSF': 'TotalBsmtSF',
            'HEATING': 'Heating',
            'HEATINGQC': 'HeatingQC',
            'CENTRALAIR': 'CentralAir',
            'ELECTRICAL': 'Electrical',
            'FIRSTFLRSF': '1stFlrSF',
            'SECONDFLRSF': '2ndFlrSF',
            'LOWQUALFINSF': 'LowQualFinSF',
            'GRLIVAREA': 'GrLivArea',
            'BSMTFULLBATH': 'BsmtFullBath',
            'BSMTHALFBATH': 'BsmtHalfBath',
            'FULLBATH': 'FullBath',
            'HALFBATH': 'HalfBath',
            'BEDROOMABVGR': 'BedroomAbvGr',
            'KITCHENABVGR': 'KitchenAbvGr',
            'KITCHENQUAL': 'KitchenQual',
            'TOTRMSABVGRD': 'TotRmsAbvGrd',
            'FUNCTIONAL': 'Functional',
            'FIREPLACES': 'Fireplaces',
            'FIREPLACEQU': 'FireplaceQu',
            'GARAGETYPE': 'GarageType',
            'GARAGEYRBLT': 'GarageYrBlt',
            'GARAGEFINISH': 'GarageFinish',
            'GARAGECARS': 'GarageCars',
            'GARAGEAREA': 'GarageArea',
            'GARAGEQUAL': 'GarageQual',
            'GARAGECOND': 'GarageCond',
            'PAVEDDRIVE': 'PavedDrive',
            'WOODDECKSF': 'WoodDeckSF',
            'OPENPORCHSF': 'OpenPorchSF',
            'ENCLOSEDPORCH': 'EnclosedPorch',
            'SSNPORCH': '3SsnPorch',
            'SCREENPORCH': 'ScreenPorch',
            'POOLAREA': 'PoolArea',
            'POOLQC': 'PoolQC',
            'FENCE': 'Fence',
            'MISCFEATURE': 'MiscFeature',
            'MISCVAL': 'MiscVal',
            'MOSOLD': 'MoSold',
            'YRSOLD': 'YrSold',
            'SALETYPE': 'SaleType',
            'SALECONDITION': 'SaleCondition',
            'SALEPRICE': 'SalePrice'
        }
        self.train = self.train.rename(columns=column_mapping)
        self.pandas_execution()
