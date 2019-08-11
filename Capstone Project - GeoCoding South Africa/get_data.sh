#!/bin/bash
set -x

_MY_SCRIPT="${BASH_SOURCE[0]}"
BASEDIR=$(cd "$(dirname "$_MY_SCRIPT")" && pwd)
_UNAME_OUT=$(uname -s)

# Prepare the data
mkdir data

# Get the South Africa Municipal Demarcation Board data
# The gdb file for 2016
wget -nc -O $BASEDIR/data/mdb2016.zip "https://www.arcgis.com/sharing/rest/content/items/cfddb54aab5f4d62b2144d80d49b3fdb/data"
[ ! -d "$BASEDIR/data/MDBWard2016.gdb" ] && unzip -o -d $BASEDIR/data $BASEDIR/data/mdb2016.zip

# The gdb file for 2011
wget -nc -O $BASEDIR/data/mdb2011.zip "https://www.arcgis.com/sharing/rest/content/items/c34ba1c16a9d4797a32ca4e7755f5ded/data"
[ ! -d "$BASEDIR/data/MDBWard2011.gdb" ] \
  && unzip -o -d $BASEDIR/data $BASEDIR/data/mdb2011.zip \
  && mv $BASEDIR/data/MDBDistrictMunicipalBoundary2011.gdb $BASEDIR/data/MDBWard2011.gdb

# The gdb file for 2009
wget -nc -O $BASEDIR/data/mdb2009.zip "https://www.arcgis.com/sharing/rest/content/items/de335c384e694439be60ae14e97d169e/data"
[ ! -d "$BASEDIR/data/MDBWard2009.gdb" ] \
  && unzip -o -d $BASEDIR/data $BASEDIR/data/mdb2009.zip \
  && mv $BASEDIR/data/MDBDistrictMunicipalBoundary2009.gdb $BASEDIR/data/MDBWard2009.gdb

set -e
source $BASEDIR/capstone/bin/activate
