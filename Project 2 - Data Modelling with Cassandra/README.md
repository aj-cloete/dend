# Cassandra database for sparkify

This database has been sanctioned by sparkify and will be used to collect information about the use of their app.  Three tables have been defined, based on the queries provided.

## Project contents
- [cassandra_etl.py](cassandra_etl.py): contains the full script that creates the tables and inserts the data
- [sql_queries.py](sql_queries.py): an importable python file containing all the queries used in the etl process
- [Project_1B_my_approach.ipynb](Project_1B_my_approach.ipynb): an interactive notebook that runs through the steps in [cassandra_etl.py](cassandra_etl.py)
- [event_datafile_new.csv](event_datafile_new.csv): a csv containing the preprocessed event data from the raw files
- [Project_1B_Project_Template.ipynb](Project_1B_&#32;Project_Template.ipynb): the project template provided (not used/implemented)
- event_data: a folder containing the raw data for the project
- images: a folder containing the images for the notebook
    
## Queries:
Using a NoSQL database like Cassandra entails modelling the data after the queries that will be run.  
The queries have been identified as below and is used to inform how the data is structured and inserted into the relevant tables
### Query1:
```
SELECT artist, song as songTitle, length as songLength
FROM song_play_session_items
WHERE sessionId=338 AND itemInSession=4
```
### Query2:
```
SELECT artist, song as songTitle, firstName, lastName
FROM artist_song_session_plays
WHERE userId=10 AND sessionId=182
ORDER BY sessionId, itemInSession 
```
### Query3:
```
SELECT firstName, lastName
FROM user_song_plays
WHERE song='All Hands Against His Own'
```

## Running the ETL:
The ETL can be run by typing `python cassandra_etl.py` in the command line
Alternatively, use [Project_1B_my_approach.ipynb](Project_1B_my_approach.ipynb) to run through the code interactively

The [notebook](Project_1B_my_approach.ipynb) can be used to test whether the data has been correctly inserted