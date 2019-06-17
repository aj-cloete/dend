# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE "songplays" (
  "songplay_id" serial PRIMARY KEY,
  "start_time" timestamp references "time" ("start_time") NOT NULL,
  "user_id" varchar(18) REFERENCES "users" ("user_id") NOT NULL,
  "level" varchar(16),
  "song_id" varchar(18) REFERENCES "songs" ("song_id")  NOT NULL,
  "artist_id" varchar(18) REFERENCES "artists" ("artist_id")  NOT NULL,
  "session_id" int4,
  "location" varchar(1024),
  "user_agent" varchar(1024),
  CONSTRAINT uniqueness UNIQUE ("start_time","user_id")
);""")

user_table_create = ("""
CREATE TABLE "users" (
  "user_id" varchar(18) PRIMARY KEY,
  "first_name" varchar(128),
  "last_name" varchar(128),
  "gender" char,
  "level" varchar(4)
);""")

song_table_create = ("""
CREATE TABLE "songs" (
  "song_id" varchar(18) PRIMARY KEY,
  "title" varchar(128),
  "artist_id" varchar(18), --REFERENCES "artists" ("artist_id") NOT NULL,
  "year" varchar(4),
  "duration" float8
);""")

artist_table_create = ( """
CREATE TABLE "artists" (
  "artist_id" varchar(18) PRIMARY KEY,
  "name" varchar(128),
  "location" varchar(128),
  "latitude" float8,
  "longitude" float8
);""")

time_table_create = ("""
CREATE TABLE "time" (
  "start_time" timestamp PRIMARY KEY,
  "hour" int4,
  "day" int4,
  "week" int4,
  "month" varchar(16),
  "year" varchar(4),
  "weekday" varchar(16)
);""")


# INSERT RECORDS

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
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT ON CONSTRAINT "uniqueness"
DO NOTHING
""")

user_table_insert = ("""
INSERT INTO "users" (
  "user_id",
  "first_name",
  "last_name",
  "gender",
  "level"
)
VALUES (%s,%s,%s,%s,%s)
ON CONFLICT ON CONSTRAINT "users_pkey" 
DO UPDATE SET 
    "first_name" = EXCLUDED."first_name",
    "last_name" = EXCLUDED."last_name",
    "gender" = EXCLUDED."gender",
    "level" = EXCLUDED."level"
""")

song_table_insert = ("""
INSERT INTO "songs" (
"song_id",
"title",
"artist_id",
"year",
"duration"
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT ON CONSTRAINT "songs_pkey"
DO NOTHING
""")

artist_table_insert = ("""
INSERT INTO "artists" (
  "artist_id",
  "name",
  "location",
  "latitude",
  "longitude"
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT ON CONSTRAINT "artists_pkey" 
DO UPDATE SET 
    "name" = EXCLUDED."name",
    "location" = EXCLUDED."location",
    "latitude" = EXCLUDED."latitude",
    "longitude" = EXCLUDED."longitude"
""")


time_table_insert = ("""
INSERT INTO "time" (
"start_time",
"hour",
"day",
"week",
"month",
"year",
"weekday")
VALUES (%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT ON CONSTRAINT "time_pkey" 
DO NOTHING
""")


# FIND SONGS

song_select = ("""
SELECT song_id, artist_id
FROM songs
inner join artists using(artist_id)
where title=%s and name=%s and duration=cast(%s as float);
""")

# QUERY LISTS

# start with the dimensions and work back to inter-linked tables and fact tables
create_table_queries = [
    time_table_create,
    user_table_create,
    artist_table_create,
    song_table_create,
    songplay_table_create]
drop_table_queries = [f"""DROP TABLE IF EXISTS "{table}" CASCADE;""" \
                      for table in ['songplays','users','songs','artists','time']]

