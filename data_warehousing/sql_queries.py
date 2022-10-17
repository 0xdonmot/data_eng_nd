import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender VARCHAR,
itemInSession INT,
lastName VARCHAR,
length DOUBLE PRECISION,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration VARCHAR,
sessionId INT,
song VARCHAR,
status INT,
ts BIGINT,
userAgent VARCHAR,
userId INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
num_songs INT,
artist_id VARCHAR,
latitude DOUBLE PRECISION,
longitude DOUBLE PRECISION,
location VARCHAR,
name VARCHAR,
song_id VARCHAR,
title VARCHAR,
duration DOUBLE PRECISION,
year INT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
songplay_id INT IDENTITY (0, 1),
start_time TIMESTAMP,
user_id INT,
level VARCHAR,
song_id VARCHAR,
artist_id VARCHAR,
session_id INT,
location VARCHAR,
user_agent VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
user_id INT,
first_name VARCHAR,
last_name VARCHAR,
gender VARCHAR,
level VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
song_id VARCHAR,
title VARCHAR,
artist_id VARCHAR,
year INT,
duration DOUBLE PRECISION
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
artist_id VARCHAR,
name VARCHAR,
location VARCHAR,
latitude DOUBLE PRECISION,
longitude DOUBLE PRECISION
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
start_time TIMESTAMP,
hour INT,
day INT,
week INT,
month INT,
year INT,
weekday INT
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY {} FROM 's3://udacity-dend/{}'
credentials 'aws_iam_role={}'
format as json 'auto' 
region 'us-west-2';
""").format('staging_events', 'log_json_path.json', 'arn:aws:iam::852966002628:role/dwhRole')

staging_songs_copy = ("""
COPY {} FROM 's3://udacity-dend/{}'
credentials 'aws_iam_role={}'
format as json 'auto' 
region 'us-west-2';
""").format('staging_songs', 'song_data', ',arn:aws:iam::852966002628:role/dwhRole')

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays

(
start_time,
user_id,
level,
song_id,
artist_id,
session_id,
location,
user_agent
)

SELECT
timestamp 'epoch' + ts * interval '1 second',
userid,
level,
song_id,
artist_id,
sessionid,
staging_events.location,
userAgent
FROM staging_events
JOIN staging_songs 
ON  staging_events.artist = staging_songs.name
AND staging_events.length = staging_songs.duration
AND staging_events.song = staging_songs.title
WHERE staging_events.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users

(
user_id,
first_name,
last_name,
gender,
level
)

(
SELECT
userId,
firstName,
lastName,
gender,
level
FROM staging_events
);
""")

song_table_insert = ("""
INSERT INTO songs

(
song_id,
title,
artist_id,
year,
duration
)

(
SELECT
song_id,
title,
artist_id,
year,
duration
FROM
staging_songs
);
""")

artist_table_insert = ("""
INSERT INTO artists

(
artist_id,
name,
location,
latitude,
longitude
)

(
SELECT
artist_id,
name,
location,
latitude,
longitude
FROM staging_songs
);
""")

time_table_insert = ("""
INSERT INTO time

(
start_time,
hour,
day,
week,
month,
year,
weekday
)

(
SELECT
timestamp 'epoch' + ts * interval '1 second' as start_time,
extract(hour from start_time),
extract(day from start_time),
extract(week from start_time),
extract(month from start_time),
extract(year from start_time),
extract(dow from start_time)
FROM staging_events
);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
