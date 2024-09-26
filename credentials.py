import json
from pathlib import Path

file_path = Path(__file__).with_name("credentials.json")

data = None
with open(file_path) as file:
    data = json.load(file)

# Accessing individual values
S3_BUCKET = data["S3_BUCKET"]
S3_BUCKET_SSB = data["S3_BUCKET_SSB"]
S3_BUCKET_SPACESHIP = data["S3_BUCKET_SPACESHIP"]
S3_BUCKET_HOUSEPRICE = data["S3_BUCKET_HOUSEPRICE"]
SNOWFLAKE_CONNECTION_DICT = data["SNOWFLAKE_CONNECTION_DICT"]
WAREHOUSE_NAME = data["WAREHOUSE_NAME"]
DIRECTORY_PATH_RAY = data["DIRECTORY_PATH_RAY"]
PWD = data["PWD"]
PYTHON_PATH_ENV = data["PYTHON_PATH_ENV"]
S3_ACCESS_KEY = data["S3_ACCESS_KEY"]
S3_SECRET_KEY = data["S3_SECRET_KEY"]
SSB_SOURCE_DATA_LOCATION = data['SSB_SOURCE_DATA_LOCATION']
