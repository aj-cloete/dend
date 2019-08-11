# Capstone Project: GeoCoding South Africa
### Udacity DataEngineering NanoDegree

## Motivation
At the time of writing, I could not find a good source with accurate information on ward, suburb, city, closest metropole and province in South Africa
The only feasible way of getting to that information from a set of latitude and longitude values is via an API - something that is not ideal when working with databases.
This aim of this project is to provide a lookup dataset onto which one can join with a (truncated) set of latitude/longitude values to obtain the information mentioned above.
Access to such a dataset means that it can be housed in a relational database and the information will be a standard SQL join away.

## Requirements
This project is built to run within the Udacity workspace environment using the helper script `setup.sh` which can be executed by either simply `./setup.sh` or `/bin/bash setup.sh`. **Do not run this script if you don't need to**.  
Once the setup script completes, ensure that you run `source capstone/bin/activate` before running the other python files to ensure that the right environment is active.

For most other applications, activating a new environment and installing the requirements using `pip install -r requirements.txt` should suffice.  If you're using __*anaconda*__, though, you're on your own.

## Raw data
To get the raw data, simply run `./get_data.sh` or `bash get_data.sh`.  The script will create a folder **data** containing the raw data used in this project.  
**to-do:** list sources websites.
