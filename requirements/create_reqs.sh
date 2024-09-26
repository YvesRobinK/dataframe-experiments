#!/usr/bin/env bash
source /home/ec2-user/anaconda3/etc/profile.d/conda.sh

conda activate Pandas_env
conda env export > pandas_req.yml
conda deactivate

conda activate Modin_env
conda env export > modin_req.yml
conda deactivate

conda activate Spark_env
conda env export > spark_req.yml
conda deactivate

conda activate Polars_env
conda env export > polars_req.yml
conda deactivate

conda activate snowpark_pandas_env
conda env export > snowpark_pandas_req.yml
conda deactivate
