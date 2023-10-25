import configparser

# Read the configuration file
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Drop tables if any exists

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplays_table_drop = "DROP TABLE IF EXISTS songplays"
users_table_drop = "DROP TABLE IF EXISTS users"
songs_table_drop = "DROP TABLE IF EXISTS songs"
artists_table_drop = "DROP TABLE IF EXISTS artists"
timelog_table_drop = "DROP TABLE IF EXISTS timelog"

# Create Staging and analytical tables

staging_events_table_create= ("""
    CREATE TABLE staging_events (
       artist              VARCHAR(200),   
       auth                VARCHAR(20), 
       firstName           VARCHAR(50), 
       gender              VARCHAR(10), 
       iteminSession       INTEGER,
       lastName            VARCHAR(50),   
       length              DOUBLE PRECISION,
       level               VARCHAR(10),
       location            VARCHAR(100),
       method              VARCHAR(10),
       page                VARCHAR(20),
       registration        DOUBLE PRECISION,
       sessionId           INTEGER,
       song                VARCHAR(200) ,
       status              INTEGER,
       ts                  BIGINT,
       userAgent           VARCHAR(200) ,
       userid              INTEGER)    
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
      num_songs             INTEGER,
      artist_id             VARCHAR(30),
      artist_latitude       DOUBLE PRECISION,
      artist_longitude      DOUBLE PRECISION,
      artist_location       VARCHAR(300),
      artist_name           VARCHAR(300),
      song_id               VARCHAR(30),
      title                 VARCHAR(300),
      duration              DOUBLE PRECISION ,
      year                  INTEGER)

""")


songplays_table_create = ("""
   CREATE TABLE songplays(
      songplay_id    INTEGER IDENTITY(0,1) DISTKEY ,
      start_time     BIGINT SORTKEY,
      user_id        INTEGER,
      level          VARCHAR(10),
      song_id        VARCHAR(30),
      artist_id      VARCHAR(30),
      session_id     INTEGER,
      location       VARCHAR(100),
      user_agent     VARCHAR(200))
""")

users_table_create = ("""
   CREATE TABLE users(
      user_id        INTEGER SORTKEY,
      first_name     VARCHAR(50),
      last_name      VARCHAR(50),
      gender         VARCHAR(10),
      level          VARCHAR(10))
      diststyle all;               
""")

songs_table_create = ("""
   CREATE TABLE songs(
      song_id       VARCHAR(30) SORTKEY,
      title         VARCHAR(200) ,
      artist_id     VARCHAR(30),
      year          INTEGER,
      duration      DOUBLE PRECISION)
      diststyle all; 
""")

artists_table_create = ("""
   CREATE TABLE artists(
     artist_id        VARCHAR(30) SORTKEY,
     name             VARCHAR(100),
     location         VARCHAR(100),
     latitude         DOUBLE PRECISION,
     longitude        DOUBLE PRECISION)
     diststyle all;
""")

timelog_table_create = ("""
   CREATE TABLE timelog(
      start_time     timestamp DISTKEY SORTKEY,
      hour           INTEGER,
      day            INTEGER,
      week           INTEGER,
      month          INTEGER,
      year           INTEGER,
      weekday        INTEGER)      
""")

# Copy data from S3 in to Staging tables

staging_songs_copy = ("""
   copy staging_songs from 's3://udacity-dend/song_data/A/A/A'
   credentials 'aws_iam_role={}'
   json 'auto'
   region 'us-west-2';
""").format(config.get("IAM_ROLE","ARN"))

staging_events_copy = ("""
   copy staging_events from 's3://udacity-dend/log'
   credentials 'aws_iam_role={}'
   json 's3://udacity-dend/log_json_path.json'
   region 'us-west-2';
""").format(config.get("IAM_ROLE","ARN"))


# Insert data into analytical tables deom the staging table

songplays_table_insert = ("""
   INSERT INTO songplays (start_time, user_id, level, song_id, artist_id,session_id, location ,user_agent) 
   ( SELECT 
            l.ts AS start_time,
            l.userid AS user_id,
            l.level,
            s.song_id,
            s.artist_id,           
            l.sessionId AS session_id,
            l.location,
            l.userAgent
            FROM staging_songs s
            JOIN staging_events l 
            ON (s.artist_name = l.artist)
            WHERE l.page = 'NextSong')            
""")

users_table_insert = ("""
   INSERT INTO users(user_id, first_name, last_name, gender, level) 
  (SELECT distinct userid AS user_id , firstName AS first_name, lastName AS last_name, gender, level from staging_events)
""")

songs_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration) 
    (SELECT song_id, title, artist_id, year, duration from staging_songs)
""")

artists_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    (SELECT artist_id AS artist_id, artist_name AS name, artist_location AS location, artist_latitude AS latitude, artist_longitude AS longitude from staging_songs)
""")

timelog_table_insert = ("""
   INSERT INTO timelog(start_time, hour, day, week, month , year , weekday)
   (SELECT to_timestamp(to_char(l.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS') AS start_time,  
   EXTRACT(HOUR FROM start_time) AS hour,
   EXTRACT(DAY FROM start_time) AS day,
   EXTRACT(WEEK FROM start_time) AS week,
   EXTRACT(MONTH FROM start_time) AS month,
   EXTRACT(YEAR FROM start_time) AS year,
   EXTRACT(DOW FROM start_time) AS weekday
   FROM staging_events l
   WHERE l.page = 'NextSong')
""")


# Create list of queries for create, drop, copy and insert operations

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplays_table_create, users_table_create, songs_table_create, artists_table_create, timelog_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplays_table_drop, users_table_drop, songs_table_drop, artists_table_drop, timelog_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplays_table_insert, users_table_insert, songs_table_insert, artists_table_insert, timelog_table_insert]

