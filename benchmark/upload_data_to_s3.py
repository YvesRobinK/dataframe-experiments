import argparse

from data.space.upload_to_s3 import upload as upload_spcae
from data.house.upload_to_s3 import upload as upload_house
from data.ssb.upload_to_s3 import upload as upload_ssb

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some parameters.')
    parser.add_argument('--dataset', '--d', type=str, help='name of the dataset', choices=['space', 'house', 'ssb'])
    parser.add_argument('--outformat', '--o', type=str, help='Output format', choices=['csv', 'parquet'])
    parser.add_argument('--scaling_factor', '--sf', type=int, help='Scaling factor')
    parser.add_argument('--is_snowflake_data', type=bool, help='is the data for snowflake?', default=False)
    args = parser.parse_args()

    dataset = args.dataset
    outformat = args.outformat
    scaling_factor = args.scaling_factor
    is_snowflake_data = args.is_snowflake_data
    if dataset == 'space':
        upload_spcae(outformat, scaling_factor, is_snowflake_data)
    elif dataset == 'house':
        upload_house(outformat, scaling_factor, is_snowflake_data)
    elif dataset == 'ssb':
        upload_ssb(outformat, scaling_factor)
