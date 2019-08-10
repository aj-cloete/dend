# Capstone Project: GeoCoding South Africa
### Udacity DataEngineering NanoDegree

## Motivation
At the time of writing, I could not find a good source with accurate information on ward, suburb, city, closest metropole and province in South Africa
The only feasible way of getting to that information from a set of latitude and longitude values is via an API - something that is not ideal when working with databases.
This aim of this project is to provide a lookup dataset onto which one can join with a (truncated) set of latitude/longitude values to obtain the information mentioned above.
Access to such a dataset means that it can be housed in a relational database and the information will be a standard SQL join away.
