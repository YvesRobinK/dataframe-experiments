import sys

import pandas as pd
from credentials import PWD, S3_BUCKET_HOUSEPRICE

def upload(outformat, scaling_factor, is_snowflake_data):
    count = 0
    private_storage = f"s3://{S3_BUCKET_HOUSEPRICE}/SF{scaling_factor}/"
    dtype = {
        'Id': 'int64',
        'MSSubClass': 'int64',
        'MSZoning': 'object',
        'LotFrontage': 'float64',
        'LotArea': 'int64',
        'Street': 'object',
        'Alley': 'object',
        'LotShape': 'object',
        'LandContour': 'object',
        'Utilities': 'object',
        'LotConfig': 'object',
        'LandSlope': 'object',
        'Neighborhood': 'object',
        'Condition1': 'object',
        'Condition2': 'object',
        'BldgType': 'object',
        'HouseStyle': 'object',
        'OverallQual': 'int64',
        'OverallCond': 'int64',
        'YearBuilt': 'int64',
        'YearRemodAdd': 'int64',
        'RoofStyle': 'object',
        'RoofMatl': 'object',
        'Exterior1st': 'object',
        'Exterior2nd': 'object',
        'MasVnrType': 'object',
        'MasVnrArea': 'float64',
        'ExterQual': 'object',
        'ExterCond': 'object',
        'Foundation': 'object',
        'BsmtQual': 'object',
        'BsmtCond': 'object',
        'BsmtExposure': 'object',
        'BsmtFinType1': 'object',
        'BsmtFinSF1': 'int64',
        'BsmtFinType2': 'object',
        'BsmtFinSF2': 'int64',
        'BsmtUnfSF': 'int64',
        'TotalBsmtSF': 'int64',
        'Heating': 'object',
        'HeatingQC': 'object',
        'CentralAir': 'object',
        'Electrical': 'object',
        '1stFlrSF': 'int64',
        '2ndFlrSF': 'int64',
        'LowQualFinSF': 'int64',
        'GrLivArea': 'int64',
        'BsmtFullBath': 'int64',
        'BsmtHalfBath': 'int64',
        'FullBath': 'int64',
        'HalfBath': 'int64',
        'BedroomAbvGr': 'int64',
        'KitchenAbvGr': 'int64',
        'KitchenQual': 'object',
        'TotRmsAbvGrd': 'int64',
        'Functional': 'object',
        'Fireplaces': 'int64',
        'FireplaceQu': 'object',
        'GarageType': 'object',
        'GarageYrBlt': 'float64',
        'GarageFinish': 'object',
        'GarageCars': 'int64',
        'GarageArea': 'int64',
        'GarageQual': 'object',
        'GarageCond': 'object',
        'PavedDrive': 'object',
        'WoodDeckSF': 'int64',
        'OpenPorchSF': 'int64',
        'EnclosedPorch': 'int64',
        '3SsnPorch': 'int64',
        'ScreenPorch': 'int64',
        'PoolArea': 'int64',
        'PoolQC': 'object',
        'Fence': 'object',
        'MiscFeature': 'object',
        'MiscVal': 'int64',
        'MoSold': 'int64',
        'YrSold': 'int64',
        'SaleType': 'object',
        'SaleCondition': 'object',
        'SalePrice': 'int64'
    }

    train = pd.read_csv(f"{PWD}/benchmark/data/house/data/train.csv")
    train = train.astype(dtype)
    del dtype["SalePrice"]
    test = pd.read_csv(f"{PWD}/benchmark/data/house/data/test.csv")
    test.dropna(subset=['BsmtFinSF1', 'BsmtFullBath', 'GarageCars'], inplace=True)
    test = test.astype(dtype)
    train = pd.concat([train]*pow(2,scaling_factor))

    if int(scaling_factor) < 1000:
        print(sys.getsizeof(train))
        if outformat == "csv":
            if is_snowflake_data == "True":
                train.to_csv(private_storage + "train.csv/train.{}".format(outformat), header=False, index=False)
            else:
                train.to_csv(private_storage + "train_h.csv/train_h.{}".format(outformat), header=True, index=False)
        else:
            train.to_parquet(private_storage + "train_h.{}".format(outformat), index=False)
            train.to_parquet(private_storage + "train_h.parquet/train_h.{}".format(outformat), index=False)
    else:
        for i in range(int(scaling_factor/1000)):
            if outformat == "csv":
                if is_snowflake_data =="True":
                    train.to_csv(private_storage + "train.csv/train_{}.{}".format(count,outformat), header=False, index=False)
                else:
                    train.to_csv(private_storage + "train.csv/train_h_count.{}".format(count,outformat), header=True, index=False)
            else:
                train.to_parquet(private_storage + "train_h.parquet/train_h_{}.{}".format(count,outformat), index=False)
            count += 1

if __name__ == '__main__':
    upload()
