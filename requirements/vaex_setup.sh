#!/usr/bin/env bash

source /home/ec2-user/anaconda3/etc/profile.d/conda.sh
conda create -y -n Vaex_env
conda activate Vaex_env
conda install -y -c conda-forge vaex
conda install -y pydantic==1.10.9
conda install -y s3fs
conda deactivate 
