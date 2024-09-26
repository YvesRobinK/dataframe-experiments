
import pandas as pd
import os
import glob

import time

ENGINES = ["spark", "dask", "modindask", "modinray", "polars", "snowpandas", "spark", "vaex", "snowparkpandas"]

def clean_csv(
        filename: str
):
    f = open(filename)
    statement = '"Statement executed successfully."\n'
    lines = f.readlines()
    f.close()
    second = False
    index = 0
    for i in range(len(lines)):
        if "Statement executed successfully." in lines[i]:
            if second == True:
               index = i
            second = True
    if second == True:
        f = open(filename, "w")
        f.writelines(lines[index + 1:])
        f.close()

def compare_csv(
    filename_control: str,
    filename_other: str,
    query_name: str,
    engine: str
) -> bool:
    control_df = pd.read_csv(filename_control)
    #clean_csv(filename_control)
    other_df = pd.read_csv(filename_other)

    other_df = other_df.drop(other_df.columns[0], axis=1)
    control_df = control_df.drop(control_df.columns[0], axis=1)
    
    try:
        pd.testing.assert_frame_equal(control_df, other_df)
        with open("results/compare/res.csv", "a") as csv_file:
            csv_file.write("{},{},{}".format(query_name, engine, " Passed"))
            csv_file.write("\n")

    except Exception as e:
        with open("results/compare/res.csv", "a") as csv_file:
            csv_file.write("{},{},{},{}".format(query_name, engine, "Failed", str(e).replace(",", ";").replace("\n", "_")))
            csv_file.write("\n")
        pass
        



if __name__ == '__main__': 
    prefix_control = "results/compare/pandas/*.csv"
    for engine in ENGINES:
        control_files = glob.glob(prefix_control)
        other_files = glob.glob("results/compare/{}/*.csv".format(engine))

        for control_file in control_files:
            query_name = os.path.split(control_file)[len(os.path.split(control_file))-1]
            for other in other_files:
                if query_name in other:
                    #clean_csv(control_file)
                    print(query_name)
                    compare_csv(control_file, other, query_name, engine)
	    

	
