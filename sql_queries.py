import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop =    "DROP  TABLE   IF EXISTS  staging_events";
staging_songs_table_drop =     "DROP  TABLE   IF EXISTS  staging_songs";
songplay_table_drop =          "DROP  TABLE   IF EXISTS  songplays";
user_table_drop =              "DROP  TABLE   IF EXISTS  users";
song_table_drop =              "DROP  TABLE   IF EXISTS  song";
artist_table_drop =            "DROP  TABLE   IF EXISTS  artist";
time_table_drop =              "DROP  TABLE   IF EXISTS  time";

# CREATE TABLES

staging_events_table_create= ("""
    Create TABLE  staging_events
    ( 
    artist              VARCHAR,
    auth                VARCHAR,
    first_name          VARCHAR,
    gender              VARCHAR,
    item_in_session     INTEGER,
    last_name           VARCHAR,
    length              FLOAT,
    level               VARCHAR,
    location            VARCHAR,
    method              VARCHAR,
    page                VARCHAR,
    registration        FLOAT,
    session_id          INTEGER,
    song                VARCHAR,
    status              INTEGER,
    ts                  TIMESTAMP,
    user_agent          VARCHAR,
    userid              INTEGER
    )

""")

staging_songs_table_create =("""
    CREATE TABLE staging_songs
    ( 
    num_songs           INTEGER,
    artist_id           VARCHAR,
    artist_latitude     FLOAT,
    artist_longitude    FLOAT,
    artist_location     VARCHAR,
    artist_name         VARCHAR,
    song_id             VARCHAR,
    title               VARCHAR,
    duration            FLOAT,
    YEAR                INTEGER 
    )

""")

songplay_table_create = ("""
    Create TABLE  songplays    
    ( 
    songplay_id        INTEGER      NOT NULL IDENTITY(0,1) PRIMARY KEY ,
    start_time         TIMESTAMP    NOT NULL ,
    user_id            INTEGER      NOT NULL ,
    level              VARCHAR,
    song_id            VARCHAR      NOT NULL ,
    artist_id          VARCHAR,
    session_id         INTEGER,
    location           VARCHAR,
    useragent          VARCHAR

    )
""")

user_table_create =("""
    Create TABLE  users
    ( 
    user_id         INTEGER         NOT NULL  SORTKEY PRIMARY KEY ,
    first_name      VARCHAR         NOT NULL,
    last_name       VARCHAR         NOT NULL,
    gender          VARCHAR         NOT NULL,
    level           VARCHAR         NOT NULL
    )
""")

song_table_create = ("""
    Create TABLE  song    
    ( 
    song_id         VARCHAR         NOT NULL  SORTKEY PRIMARY KEY ,
    title           VARCHAR         NOT NULL,
    artist_id       VARCHAR         NOT NULL,
    year            VARCHAR         NOT NULL,
    duration        FLOAT
    )
""")

artist_table_create = ("""
    Create TABLE  artist   
    ( 
    artist_id       VARCHAR         NOT NULL  SORTKEY PRIMARY KEY ,
    name            VARCHAR         NOT NULL,
    location        VARCHAR,         
    lattitude       FLOAT,
    longitude       FLOAT
    )
""")

time_table_create = ("""
    Create TABLE  time    
    (
    start_time      TIMESTAMP      NOT NULL SORTKEY PRIMARY KEY ,
    hour            INTEGER,
    day             INTEGER,
    week            INTEGER,
    month           INTEGER,
    year            INTEGER,
    weekday         INTEGER
    )
""")

# STAGING TABLES

staging_events_copy =("""
    copy staging_events   from {BUCKET_PATH} 
    credentials 'aws_iam_role={USER_ROLE}' 
    region 'us-west-2' format as JSON {JSON_PATH} 
    timeformat as 'epochmillisecs';
""").format(BUCKET_PATH=config['S3']['LOG_DATA'],
            USER_ROLE=config['IAM_ROLE']['ARN'],
            JSON_PATH=config['S3']['LOG_JSONPATH'])

staging_songs_copy =("""
    copy  staging_songs from {BUCKET_PATH} 
    credentials 'aws_iam_role={USER_ROLE}' 
    region 'us-west-2' format as JSON 'auto' ;
""").format(BUCKET_PATH=config['S3']['SONG_DATA'],
            USER_ROLE=config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    insert into   songplays  (start_time, user_id, level, song_id, artist_id, session_id, location, useragent)

    select 
    distinct (e.ts) as start_time  
    ,e.userid  
    ,e.level  
    ,s.song_id
    ,s.artist_id
    ,e.session_id
    ,e.location
    ,e.user_agent as useragent

    from staging_events e
    join staging_songs s
    ON (e.song = s.title AND e.artist = s.artist_name)
""")

user_table_insert = ("""
    insert into users (user_id, first_name, last_name, gender, level)
    select 
    distinct(e.userid)  as user_id
    ,first_name
    ,last_name
    ,gender
    ,level
    from staging_events e
    where  (e.userid) is not null

""")

song_table_insert = ("""

    insert into song (song_id, title, artist_id, year, duration)
    select 
    distinct (s.song_id) as song_id
    ,s.title
    ,s.artist_id
    ,s.year
    ,s.duration
    from staging_songs s
    where (s.song_id) is not null 

""")

artist_table_insert = ("""
    insert into artist  (artist_id, name, location, lattitude, longitude)

    select distinct (s.artist_id) as artist_id
    ,s.artist_name         AS name
    ,s.artist_location     AS location
    ,s.artist_latitude     AS latitude
    ,s.artist_longitude    AS longitude
    from staging_songs s
    where (s.artist_id) is not null  
""")

time_table_insert = ("""

    INSERT INTO time (start_time, hour, day, week, month, year, weekday)

    SELECT  
    DISTINCT(e.ts)                       as start_time
    ,EXTRACT(hour      FROM e.ts)        as hour
    ,EXTRACT(day       FROM e.ts)        as day
    ,EXTRACT(week      FROM e.ts)        as week
    ,EXTRACT(month     FROM e.ts)        as month
    ,EXTRACT(year      FROM e.ts)        as year
    ,EXTRACT(dayofweek FROM e.ts)        as weekday

    FROM  staging_events  e;

""")

# Get Count of Tables 


get_counts = ("""
    SELECT COUNT(*) as Total_count   ,'staging_events'   as table_name FROM staging_events 
    union 
    SELECT COUNT(*) as Total_count   ,'staging_events'   as table_name FROM staging_songs 
    union 
    SELECT COUNT(*) as Total_count   ,'songplays'        as table_name FROM songplays 
    union 
    SELECT COUNT(*) as Total_count   ,'users'            as table_name FROM users
    union 
    SELECT COUNT(*) as Total_count   ,'song'            as table_name FROM song
    union 
    SELECT COUNT(*) as Total_count   ,'artist'          as table_name FROM artist
    union 
    SELECT COUNT(*) as Total_count   ,'time'             as table_name FROM time
""")


# QUERY LISTS
 

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
get_counts_queries = [get_counts]