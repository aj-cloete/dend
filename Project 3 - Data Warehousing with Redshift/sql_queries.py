import configparser

# DROP TABLES

staging_events_table_drop = """
DROP TABLE IF EXISTS staging_events CASCADE;"""
staging_songs_table_drop = """
DROP TABLE IF EXISTS staging_songs CASCADE;"""
songplay_table_drop = """
DROP TABLE IF EXISTS songplays CASCADE;"""
user_table_drop = """
DROP TABLE IF EXISTS users CASCADE;"""
song_table_drop = """
DROP TABLE IF EXISTS songs CASCADE;"""
artist_table_drop = """
DROP TABLE IF EXISTS artists CASCADE;"""
time_table_drop = """
DROP TABLE IF EXISTS time CASCADE;"""

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
  "artist"           varchar(MAX),
  "auth"             varchar(MAX),
  "first_name"       varchar(MAX),
  "gender"           varchar(MAX),
  "item_in_session"  int4,
  "last_name"        varchar(MAX),
  "length"           numeric,
  "level"            varchar(MAX),
  "location"         varchar(MAX),
  "method"           varchar(MAX),
  "page"             varchar(MAX),
  "registration"     varchar(MAX),
  "session_id"       int4,
  "song"             varchar(MAX),
  "status"           varchar(MAX),
  "ts"               int8,
  "user_agent"       varchar(MAX),
  "user_id"          varchar(MAX)
);""")

staging_songs_table_create = ("""
CREATE TABLE "staging_songs" (
  "artist_id"         varchar(MAX),
  "artist_latitude"   numeric,
  "artist_location"   varchar(MAX),
  "artist_longitude"  numeric,
  "artist_name"       varchar(MAX),
  "duration"          numeric,
  "num_songs"         int,
  "song_id"           varchar(MAX),
  "title"             varchar(MAX),
  "year"              int
);
""")

songplay_table_create = ("""
CREATE TABLE "songplays" (
  "songplay_id"   int8 IDENTITY PRIMARY KEY,
  "start_time"    timestamp references "time" ("start_time") NOT NULL,
  "user_id"       varchar(18) REFERENCES "users" ("user_id") NOT NULL,
  "level"         varchar(16),
  "song_id"       varchar(18) REFERENCES "songs" ("song_id")  NOT NULL,
  "artist_id"     varchar(18) REFERENCES "artists" ("artist_id")  NOT NULL,
  "session_id"    int4,
  "location"      varchar(1024),
  "user_agent"    varchar(1024),
  CONSTRAINT uniqueness UNIQUE ("start_time","user_id")
)
DISTKEY (songplay_id)
SORTKEY (user_id, song_id, artist_id)
;
""")

user_table_create = ("""
CREATE TABLE "users" (
  "user_id"     varchar(18) PRIMARY KEY,
  "first_name"  varchar(128),
  "last_name"   varchar(128),
  "gender"      char,
  "level"       varchar(4)
)
DISTKEY (user_id)
SORTKEY (user_id)
;
""")

song_table_create = ("""
CREATE TABLE "songs" (
  "song_id"    varchar(18) PRIMARY KEY,
  "title"      varchar(1024),
  "artist_id"  varchar(1024),
  "year"       varchar(4),
  "duration"   float8
)
DISTKEY (song_id)
SORTKEY (artist_id)
;
""")

artist_table_create = ("""
CREATE TABLE "artists" (
  "artist_id"   varchar(18) PRIMARY KEY,
  "name"        varchar(1024),
  "location"    varchar(1024),
  "latitude"    float8,
  "longitude"   float8
)
DISTKEY (artist_id)
SORTKEY (artist_id)
;
""")

time_table_create = ("""
CREATE TABLE "time" (
  "start_time"  timestamp PRIMARY KEY,
  "hour"        int4,
  "day"         int4,
  "week"        int4,
  "month"       int4,
  "year"        int4,
  "weekday"     int4
) 
DISTKEY (start_time)
SORTKEY (start_time, year, month, week, day, weekday, hour)
;
""")


# STAGING TABLES
config = configparser.ConfigParser()
config.read('dwh.cfg')

generic_copy_statement = """
COPY {table}
FROM {s3bucket}
IAM_ROLE '{iamrole}'
JSON {json}
"""

staging_events_copy = generic_copy_statement.format(
    table='staging_events', 
    s3bucket=config.get('S3','log_data'), 
    iamrole=config.get('CLUSTER','db_role_arn'), 
    json=config.get('S3','log_jsonpath'))

staging_songs_copy = generic_copy_statement.format(
    table='staging_songs', 
    s3bucket=config.get('S3','song_data'), 
    iamrole=config.get('CLUSTER','db_role_arn'),
    json="'auto'")
# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO "songplays" (
  "start_time",
  "user_id",
  "level",
  "song_id",
  "artist_id",
  "session_id",
  "location",
  "user_agent"
)
WITH song_artist as (
  SELECT 
    songs.artist_id,
    artists.name as artist_name,
    songs.title as song_name,
    songs.song_id as song_id
  FROM "songs"
  INNER JOIN "artists" using (artist_id)
)
, selection AS (
  SELECT
    date_add('ms', ts, '1970-01-01') as start_time,
    user_id,
    level,
    sa.song_id,
    sa.artist_id,
    session_id,
    location,
    user_agent
  FROM staging_events se
  INNER JOIN song_artist as sa on sa.artist_name=se.artist and sa.song_name=se.song
  WHERE page = 'NextSong')
SELECT selection.*
FROM selection
LEFT JOIN songplays as sp using (start_time, user_id)
WHERE sp.start_time ISNULL
""")

user_table_insert = ("""
INSERT INTO "users" (
  "user_id",
  "first_name",
  "last_name",
  "gender",
  "level"
)

SELECT 
  "user_id",
  "first_name",
  "last_name",
  "gender",
  "level"
FROM staging_events
NATURAL INNER JOIN (SELECT user_id, max(ts) as ts FROM staging_events GROUP BY 1) as last_update
WHERE user_id not in (SELECT user_id from users)
    AND page='NextSong'
""")

song_table_insert = ("""
INSERT INTO "songs" (
  "song_id",
  "title",
  "artist_id",
  "year",
  "duration"
)

SELECT 
  "song_id",
  "title",
  "artist_id",
  "year",
  "duration"
FROM staging_songs
WHERE song_id not in (SELECT song_id from songs)
""")

artist_table_insert = ("""
INSERT INTO "artists" (
  "artist_id",
  "name",
  "location",
  "latitude",
  "longitude"
)

SELECT 
  "artist_id",
  "artist_name",
  "artist_location",
  max(cast("artist_latitude" as float8)) AS "artist_latitude",
  max(cast("artist_longitude" as float8)) AS "artist_longitude"
FROM staging_songs
WHERE artist_id not in (SELECT artist_id from artists)
GROUP BY 1,2,3
""")

# we select this info from the staging_events table since the time table has to be populated before the songplays table due to the foreign key constraint on that table.  
# You will notice the order in the create_table_queries and insert_table_queries reflect this point.
time_table_insert = ("""
INSERT INTO "time" (
  "start_time",
  "hour",
  "day",
  "week",
  "month",
  "year",
  "weekday")
WITH times AS (
  SELECT date_add('ms', "ts", '1970-01-01') as "start_time"
  FROM "staging_events"
  GROUP BY 1)
SELECT
  "start_time",
  extract('hour' from start_time) as "hour",
  extract('day' from start_time) as "day",
  extract('week' from start_time) as "week",
  extract('month' from start_time) as "month",
  extract('year' from start_time) as "year",
  extract('weekday' from start_time) as "weekday"
FROM times
WHERE start_time not in (SELECT start_time from "time")
  AND page='NextSong'
""")

# QUERY LISTS

# note the change in order to put songplay last in lists
create_table_queries = [staging_events_table_create, 
                        staging_songs_table_create,
                        user_table_create,
                        artist_table_create,
                        song_table_create,
                        time_table_create,
                        songplay_table_create]
drop_table_queries = [staging_events_table_drop, 
                      staging_songs_table_drop,
                      user_table_drop,
                      artist_table_drop,
                      song_table_drop,
                      time_table_drop,
                      songplay_table_drop]
copy_table_queries = [staging_events_copy,
                      staging_songs_copy]
insert_table_queries = [user_table_insert,
                        artist_table_insert,
                        song_table_insert,
                        time_table_insert,
                        songplay_table_insert]

