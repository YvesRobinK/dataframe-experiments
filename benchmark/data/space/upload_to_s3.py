import sys

import pandas as pd
from credentials import PWD, S3_BUCKET_SPACESHIP


def upload(outformat, scaling_factor, is_snowflake_data):
    count = 0
    private_storage = f"s3://{S3_BUCKET_SPACESHIP}/SF{scaling_factor}/"
    dtype = {
        'PassengerId': 'object',
        'HomePlanet': 'object',
        'CryoSleep': 'object',
        'Cabin': 'object',
        'Destination': 'object',
        'Age': 'float64',
        'VIP': 'object',
        'RoomService': 'float64',
        'FoodCourt': 'float64',
        'ShoppingMall': 'float64',
        'Spa': 'float64',
        'VRDeck': 'float64',
        'Name': 'object',
        'Transported': 'bool'
    }

    train = pd.read_csv(f"{PWD}/benchmark/data/space/data/train.csv")
    train = train.astype(dtype)
    del dtype["Transported"]
    test = pd.read_csv(f"{PWD}/benchmark/data/space/data/test.csv")
    test = test.astype(dtype)

    train = pd.concat([train]*pow(2, scaling_factor))
    diff = scaling_factor - 11

    if int(scaling_factor) < 12:
        print(sys.getsizeof(train))
        if outformat == "csv":
            if is_snowflake_data == "True":
                train.to_csv(private_storage + "train.csv/train.{}".format(outformat), header=False, index=False)
            else:
                train.to_csv(private_storage + "train_h.csv/train_h.{}".format(outformat), header=True, index=False)
        else:
            train.to_parquet(private_storage + "train_h.parquet/train_h.{}".format(outformat), index=False)
    else:
        for i in range(pow(2, diff)):
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
