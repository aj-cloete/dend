# Sparkify Data Lake - Spark

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
1. [dl.cfg](./dl.cfg) contains the configuration settings for the database. More on config later.
2. [infrastructure_as_code.py](./infrastructure_as_code.py) is a helper function to create s3 bucket and keys.  Be sure to edit the file with your own bucket credentials if you wish to implement it.                        
3. [etl.py](./etl.py) is the main ETL code that processes the raw data and structures it into the database tables
4. [ajcloete-sparkify.ipynb](./ajcloete-sparkify.ipynb) is the notebook run on the AWS EMR cluster to ensure that all steps run without errors.
5. [data](./data/) a folder containing sample data for local testing.

#### infrastructure_as_code.py example
```import infrastructure_as_code as iac
iac.create_s3_structure(iac.boto3.resource('s3', region_name='us-west-2'), 
                        bucket_root='your-bucket', keys = ['songs','songplays','time','artists','users'])```

## Configuration (dl.cfg)
```
[AWS]
aws_access_key_id = 
aws_secret_access_key = 
```
Ensure that you either complete the details in dl.cfg or ensure that your environment variables contain configurations for:
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
```

## Running the code
Depending on where you're running the code, it will need some tweaking to work correctly.  
If you're running it locally, you have to remove the config step from the create_spark_session function
You will also need to change the input_data and output_data to point to local destinations.


### Credits
The images in this README has been generated from the wonderful package [dbvis](https://www.dbvis.com)
