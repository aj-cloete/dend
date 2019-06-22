# Sparkify Data Warehouse - Redshift

## Purpose
The purpose of this database is to allow Sparkify to perform analytical queries on the usage of their platform.  
Specifically, the song plays will be analysed using this database.  
Things like which song is the most played, which user is the most active, etc. will be answered using this database.

## Schema Layout
   ![Schema Layout](./images/Screenshot%202019-04-22%20at%2018.58.36.png)

#### Relations and joins
1. The *songplays* table is at the centre of this star schema.  It contains all the plays from the log files that had a match with the song data.  It relates to all the other tables via foreign keys.
2. The *_time_* table can be linked to the songplays table on *start_time*.  It contains useful information for use in a *group by* statement, such as hour, day, month, etc.
3. The *songs* table contains the collection of songs from the songs data.  Information such as the title, duration, year, etc. Join on song_id
4. The *artists* table contains information about the artists from the songs data.  Information such as their name, location, etc. can be found here. Join on artist_id
5. The *users* table contains information about the users on the Sparkify platform.  Join to this table using user_id

#### Detailed view
   ![Schema Layout](./images/Screenshot%202019-04-22%20at%2019.02.05.png)

The schema was designed to have useful information collected in the dimensions.  
It was normalised according to logical entities, namely artist, song, user and time.  
Depending on the analysis, the analyst can join to the relevant tables to get the information necessary for the analysis.

## Example query:
#### Get all the information available in the database
```sql
select 
  songplays.songplay_id as play_songplay_id,
  songplays.start_time as play_start_time,
  songplays.level as play_level,
  songplays.session_id as play_session_id,
  songplays.location as play_location,
  songplays.user_agent as play_user_agent,
  "time".hour as play_hour,
  "time".day as play_day,
  "time".week as play_week,
  "time".month as play_month,
  "time".year as play_year,
  "time".weekday as play_weekday,
  users.user_id as user_id,
  users.first_name as user_first_name,
  users.last_name as user_last_name, 
  users.gender as user_gender, 
  users.level as user_level,
  artists.artist_id as artist_id,
  artists.name as artist_name, 
  artists.location as artist_location,
  artists.latitude as artist_latitude,
  artists.longitude as artist_longitude,
  songs.song_id as song_id,
  songs.title as song_title,
  songs.year as song_year,
  songs.duration as song_duration
from "songplays" 
inner join "time" using(start_time)
inner join "users" using(user_id)
inner join "artists" using(artist_id)
inner join "songs" using(song_id)
```
Use the query above as the basis for whatever analysis is at hand.  
The unneccessary columns/joins can be removed and the relevant group by and aggregates can be applied as necessary.

## Technical details
#### What's in this repository?
1. [dwh.cfg](./dwh.cfg) contains the configuration settings for the database. More on config later.
2. [infrastructure_as_code.py](./infrastructure_as_code.py) contains all the steps to set up the necessary infrastructure (Redshift cluster, AWS Role with attached policies, etc.)
3. [sql_queries.py](./sql_queries.py) contains all the SQL queries used in the ETL pipeline including create table statements, copy statements for the staging tables and insert statements
4. [create_tables.py](./create_tables.py) contains the steps necessary to create the database and underlying tables.  Run this command like this: `python create_tables.py`
5. [etl.py](./etl.py) is the main ETL code that processes the raw data and structures it into the database tables

## Configuration
```
[CLUSTER]
host = redshift-cluster.cqipuvtyifkz.us-west-2.redshift.amazonaws.com
db_name = dwh
db_user = dwhuser
db_password = dwhPassw0rd123
db_port = 5439
db_cluster_identifier = redshift-cluster
db_cluster_type = multi-node
db_num_nodes = 4
db_node_type = dc2.large
db_security_group = 'redshift-sg'
db_role_arn = arn:aws:iam::XXXXXXXXXXXX:role/myRedshiftRole

[IAM_ROLE]
iam_role_name = myRedshiftRole

[S3]
log_data = 's3://udacity-dend/log_data'
log_jsonpath = 's3://udacity-dend/log_json_path.json'
song_data = 's3://udacity-dend/song_data'

[AWS]
key =
secret =
region = 'us-west-2'
```
#### You have one of two options to run the project:
- provide AWS access keys to run infrastructure as code section by either:
  - Setting the environment varaibles AWS_KEY and AWS_SECRET
  - Populating the [AWS] section in `dwh.cfg`
- provide the cluster details (all fields in [CLUSTER] section completed)

## Running
If you provided the AWS credentials you can optionally run the following two commands:
>`python infrastructure_as_code.py create` to create the infrastructure.

>`python infrastructure_as_code.py clean` to remove the infrastructure created.

To run the project, run the following two commands (in sequence):
>`python create_tables.py`

>'python etl.py`

_*If you provided the AWS credentials, you'll notice a self-destruct function once the etl.py function as completed.  You may opt to wait 30 seconds to let it delete the infrastructure automatically or you can cancel the process to keep the infrastructure up and running.*_

### Credits
The images in this README has been generated from the wonderful package [dbvis](https://www.dbvis.com)
