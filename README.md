# Snowpandas experiments

## Getting Started

The ressources in this repository are used to produce the experiment results.

## Setup
Before running experiments, you have to make sure that proper configurations are set in `credentials.json` file. This configurations include:
1. Setting up aws account
2. Setting up snowflake account
3. uploading data to S3 and Snowflake

### Set up on AWS
You need to setup a aws account and set this values in `credentials.json`.
```json
    "S3_ACCESS_KEY":"",
    "S3_SECRET_KEY":""
```
For creating a account, user, and access key follow [this link](https://www.webiny.com/docs/infrastructure/aws/configure-aws-credentials). Also you can install AWS CLI by following [this link](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).

### Uplaoding data to S3
You can either upload your data to S3 and then put the bucket name in `credentials.json`, or you can use `./benchmark/upload_to_s3.py` script.
For the later one, use `-h` command to see the available options. Note that before running this script you have to create a S3 bucket. For creating a S3' bucket you can use [this link](https://khandelwal-shekhar.medium.com/read-and-write-to-from-s3-using-python-boto3-and-pandas-s3fs-144341e23aa1). If you got prevent access error, make sure to check `AmazonS3FullAccess` when creating the user.

Put the bucket name in `credentials.json` as follow:
```
    "S3_BUCKET": "bucket_name_1",
    "S3_BUCKET_SPACESHIP": "bucket_name_2",
    "S3_BUCKET_SSB": "bucket_name_3",
    "S3_BUCKET_HOUSEPRICE": "bucket_name_4"
    "PWD" : "/path/to/dataframe-experiments/"
```
Note that `S3_BUCKET` is the default bucket and when the query name is could not match either of the workloads, it uses this bucket. For the current version of experiments you can ignore it.
If you want to upload all of the scaling factors at once for spaceship and house price data, use `./benchmark/data/<data_set>/upload_all_sf_s3.sh` script. If the scripts could not find `credentials.json` file, run the following script in the terminal: `export PYTHONPATH=$PYTHONPATH:/path/to/dataframe-experiments`.

NOTE: for uploading SSB data to S3, make sure to set `SSB_SOURCE_DATA_LOCATION` in `credentials.json`. We use this parameter as we already had the SSB data ready in our public bucket. This might change in the future.

### Creating Snowflake account
Please create a snowflake account and then put the following entries in `credentials.py`:
```json
"SNOWFLAKE_CONNECTION_DICT": {
        "account": "xxxxxxx-xxxxxxx",
        "user": "username",
        "password": "password",
        "warehouse": "xsmall"
        },
"WAREHOUSE_NAME": "xsmall",
```
For setting `account`, login to your snowflake account, on the below left, click on you name, then click on account. Now you should be able to see all of your accounts. Click on the account you want and copy the account identifier. There is a copy button that does this for you. Then set the value of your account identifier for `account`.


### Installing `snowsql`

1. `Wget https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/1.3/linux_x86_64/snowsql-1.3.0-linux_x86_64.bash`
2. `bash snowsql-linux_x86_64.bash`
3. Add `snowsql` to the bashrc path
If the snowsql command have some problems, use the following command: `snowsql --upgrade`

### Initial Snowflake setup
You will need to use the Snowflake Web UI and input the commands:
```SQL
-- Create warehouses
CREATE WAREHOUSE XSMALL WITH WAREHOUSE_SIZE='XSMALL';
CREATE WAREHOUSE SMALL WITH WAREHOUSE_SIZE='SMALL';
CREATE WAREHOUSE MEDIUM WITH WAREHOUSE_SIZE='MEDIUM';
CREATE WAREHOUSE LARGE WITH WAREHOUSE_SIZE='LARGE';
CREATE WAREHOUSE XLARGE WITH WAREHOUSE_SIZE='XLARGE';
CREATE WAREHOUSE XXLARGE WITH WAREHOUSE_SIZE='XXLARGE';
CREATE WAREHOUSE XXXLARGE WITH WAREHOUSE_SIZE='XXXLARGE';
CREATE WAREHOUSE XXXXLARGE WITH WAREHOUSE_SIZE='4X-LARGE';

-- Suspend them immediately to avoid running costs
ALTER WAREHOUSE XXXXLARGE SUSPEND;
ALTER WAREHOUSE XXXLARGE SUSPEND;
ALTER WAREHOUSE XXLARGE SUSPEND;
ALTER WAREHOUSE XLARGE SUSPEND;
ALTER WAREHOUSE LARGE SUSPEND;
ALTER WAREHOUSE MEDIUM SUSPEND;
ALTER WAREHOUSE SMALL SUSPEND;

-- A default warehouse will get created which we do not need
DROP WAREHOUSE IF EXISTS COMPUTE_WH;

-- Create a database where the different schemas ('SF1' and '1000 event') are stored
CREATE DATABASE ADL;
```
Then make sure to create connections in your `~/.snowsql/config` of the type:

```
[connections.xsmall]

accountname = <your-account-id>.<aws-region>
username = <your-username>
password = <your-password>

dbname = ADL
schemaname = ADL_1000
warehousename = XSMALL

[connections.large]

accountname = <your-account-id>.<aws-region>
username = <your-username>
password = <your-password>

dbname = ADL
schemaname = ADL
warehousename = LARGE
```
Note that you can make a separate connection for each warehouse (e.g. in above we have connections for `XSMALL` and `XLARGE`).

### Creating integration between S3 and Snowflake
Follow [this guide](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration) to set up AWS and Snowflake integration.
In step 3 of the guide, you can use the following integration:
```sql
CREATE OR REPLACE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::168929489144:role/snowflake-role' -- or your equivalent AWS ROLE
  STORAGE_ALLOWED_LOCATIONS = ('s3://rumbledb-snowflake-adl/SF1/', 's3://hep-adl-ethz/hep-parquet/native/', 's3://rumbledb-snowflake-adl/ssb/sf1', 's3://rumbledb-snowflake-adl/ssb/sf10', 's3://rumbledb-snowflake-adl/ssb/sf100', 's3://rumbledb-snowflake-adl/ssb/sf1000');
```
Add your buckets to `STORAGE_ALLOWED_LOCATIONS`. Finally make sure to add the bucket path to the policy, otherwise you get access denied when staging data to snowflake. You can also ignore step 6 of the guide.

### Uploadig data to Snowflake
For uploading data to snowflake go to the `./benchmark/data/`. Then select the dataset folder you want (e.g. `house`, `space`, `ssb`). Then use `upload_to_snowflake.sh` script. Make sure to either change the configs in the script or use proper arguments.

## Deploy a machine
Create an EC2 on a m5d.12xlarge machine. Use [this repository](https://github.com/YvesRobinK/deployment-scripts) to deploy an instance with all of installed requirements.


## Running experiments
Once your EC2 instance is deployed, ssh into the instance.
`cd dataframe-experiments/benchmark/`
`nano run.sh`
In `run.sh` you can set the configuration of your experiments.These are the following arguments that should be set:
- `tasks`: indicates the query you want to run. You have the following options: ("ssb_x.x" "Fork" "Endeavor" "Catboost" "HousePricePrediction")
- `executors`: indiactes the execution engine. You have the follwing options: ("Pandas" "Spark" "Snowpandas" "Polars" "ModinRay" "ModinDask" "Dask" "Vaex" "SnowparkPandas")
- `sf`: indicates the scaling factor. Your can set it like "SFX" where X is the scaling factor.
- `warmup`: indicates the number of warmup runs. Note that we don't log warmup runs.
- `measure`: indicates the number of real runs.
- `format`: indicates the format of your input dataset. It should be either "parquet" or "csv".
- `logfile`: the file you want to save your logs.
- `timeout`: indicates the timeout after which a single experiment is terminated.
