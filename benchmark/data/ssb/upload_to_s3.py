import concurrent.futures

from credentials import S3_BUCKET_SSB, SSB_SOURCE_DATA_LOCATION
import pandas as pd
import s3fs


LO_HEADER = ['LO_ORDERKEY', 'LO_LINENUMBER', 'LO_CUSTKEY', 'LO_PARTKEY', 'LO_SUPPKEY', 'LO_ORDERDATE', 'LO_ORDERPRIORITY', 'LO_SHIPPRIORITY', \
        'LO_QUANTITY', 'LO_EXTENDEDPRICE', 'LO_ORDTOTALPRICE', 'LO_DISCOUNT', 'LO_REVENUE', 'LO_SUPPLYCOST', 'LO_TAX', 'LO_COMMITDATE', 'LO_SHIPMODE']

D_HEADER = ['D_DATEKEY', 'D_DATE', 'D_DAYOFWEEK', 'D_MONTH', 'D_YEAR', 'D_YEARMONTHNUM', 'D_YEARMONTH', 'D_DAYNUMINWEEK', 'D_DAYNUMINMONTH', 'D_DAYNUMINYEAR', \
        'D_MONTHNUMINYEAR', 'D_WEEKNUMINYEAR', 'D_SELLINGSEASON', 'D_LASTDAYINWEEKFL', 'D_LASTDAYINMONTHFL', 'D_HOLIDAYFL', 'D_WEEKDAYFL']
C_HEADER = ['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_CITY', 'C_NATION', 'C_REGION', 'C_PHONE', 'C_MKTSEGMENT']
S_HEADER = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_CITY', 'S_NATION', 'S_REGION', 'S_PHONE']
P_HEADER = ['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_CATEGORY', 'P_BRAND1', 'P_COLOR', 'P_TYPE', 'P_SIZE', 'P_CONTAINER']
SMALL_TABLES = [('date', D_HEADER), ('customer', C_HEADER), ('supplier', S_HEADER), ('part', P_HEADER)]

def process_file(lineorder_file, thread_marker, chunksize, private_storage, outformat):
    count = 0
    print("Processing file: ", lineorder_file)
    s3_path = "s3://" + lineorder_file

    try:
        with pd.read_table(s3_path, chunksize=chunksize, names=LO_HEADER, sep='|') as reader:
            for chunk in reader:
                count += 1
                if outformat == "csv":
                    output_path = f"{private_storage}lineorder/lineorder_10_{thread_marker}{count}.csv"
                    chunk.to_csv(output_path, index=False)
                else:
                    output_path = f"{private_storage}lineorder/lineorder_10_{thread_marker}{count}.parquet"
                    chunk.to_parquet(output_path, index=False)
                print(f"Saved chunk {count} to {output_path}")
    except Exception as e:
        print(f"Error processing file {lineorder_file}: {e}")
    return 1

def upload(outformat, scaling_factor):
    chunksize = 10 ** 7
    if outformat == "parquet":
        chunksize = chunksize * 2
    count = 0
    legacy_storage = f"s3://{SSB_SOURCE_DATA_LOCATION}/ssb/sf{scaling_factor}/"
    private_storage = f"s3://{S3_BUCKET_SSB}/SF{scaling_factor}/"

    s3 = s3fs.S3FileSystem(anon=False)
    print("Lineorder path: ", legacy_storage + "lineorder.tbl/")
    lineorder_files = s3.glob(legacy_storage + "lineorder.tbl/*")
    print(legacy_storage + "lineorder.tbl/*.tbl.*")
    print("Len lineorder files: ", len(lineorder_files))
    print("Lineorder files: ", lineorder_files)
    print("Scaling Factor: ", scaling_factor)
    for item in SMALL_TABLES:
        df = pd.read_table(legacy_storage + item[0] + ".tbl", encoding='ISO-8859-1', names=item[1], sep='|')
        if item[0] == "date":
            print(df)
        if outformat == "csv":
            df.to_csv(private_storage + "{}.csv".format(item[0]), index=False)
        else:
            df.to_parquet(private_storage + "{}.parquet".format(item[0]), index=False)

    ASCI_NUMBER_SMALL_A = 97
    count = ASCI_NUMBER_SMALL_A
    print("Number Files: ", lineorder_files)
    print("Scaling Factor: ", scaling_factor)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for lineorder_file in lineorder_files:
            print("Lineorder_file: ", lineorder_file)
            print("Started: ", chr(count))
            futures.append(executor.submit(process_file, lineorder_file, chr(count), chunksize, private_storage, outformat))
            count += 1
        for future in concurrent.futures.as_completed(futures):
            future.result()

if __name__ == '__main__':
    upload()