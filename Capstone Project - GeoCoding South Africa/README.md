# Capstone Project: GeoCoding South Africa
### Udacity DataEngineering NanoDegree

## Motivation
At the time of writing, I could not find a good source with accurate information on ward, suburb, city, closest metropole and province in South Africa
The only feasible way of getting to that information from a set of latitude and longitude values is via an API - something that is not ideal when working with databases.
This aim of this project is to provide a lookup dataset onto which one can join with a (truncated) set of latitude/longitude values to obtain the information mentioned above.
Access to such a dataset means that it can be housed in a relational database and the information will be a standard SQL join away.

## Requirements
This project is built to run within the Udacity workspace environment using the helper script `setup.sh` which can be executed by either simply `./setup.sh` or `/bin/bash setup.sh`. **Do not run this script if you don't need to**
Also ensure that you run `source capstone/bin/activate` before running the other python files to ensure you're in the right environment.

Activating an environment and installing the requirements from using `pip install -r requirements.txt` should suffice in most situations.  If you're using __*anaconda*__, though, you're on your own.

