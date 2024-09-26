#!/usr/bin/env bash

source /home/ec2-user/anaconda3/etc/profile.d/conda.sh
conda activate omnisci-dev
conda install s3fs
pip install snowflake-connector-python
pip install snowflake
pip install ray
pip install grpcio
conda deactivate 
