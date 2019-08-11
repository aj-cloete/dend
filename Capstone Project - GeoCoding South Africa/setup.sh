#!/bin/bash
# This bash script prepares the environment by installing the requirements and downloading the data
set -x

_MY_SCRIPT="${BASH_SOURCE[0]}"
BASEDIR=$(cd "$(dirname "$_MY_SCRIPT")" && pwd)

# Install the required packages
pip install -r requirements.txt --upgrade

# Prepare the data
mkdir data

# Get the South Africa Municipal Demarcation Board data
# The gdb file for 2016
wget -nc -O $BASEDIR/data/mdb2016.zip "https://www.arcgis.com/sharing/rest/content/items/cfddb54aab5f4d62b2144d80d49b3fdb/data"
[ ! -d "$BASEDIR/data/MDBWard2016.gdb" ] && unzip -o -d $BASEDIR/data $BASEDIR/data/mdb2016.zip
# The feature layer for 2016
wget -nc -O $BASEDIR/data/features2016.csv "https://opendata.arcgis.com/datasets/a19b92da789f4c949f17a88d14690568_0.csv"
