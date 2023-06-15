class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

    drop_table_staging_events = ("""
        DROP TABLE IF EXISTS staging_events
    """)

    drop_table_staging_songs = ("""
        DROP TABLE IF EXISTS staging_songs
    """)

    drop_table_songplays = ("""
        DROP TABLE IF EXISTS songplays
    """)

    drop_table_users = ("""
        DROP TABLE IF EXISTS users
    """)

    drop_table_songs = ("""
        DROP TABLE IF EXISTS songs
    """)

    drop_table_artists = ("""
        DROP TABLE IF EXISTS artists
    """)

    drop_table_time = ("""
        DROP TABLE IF EXISTS time
    """)

    Create_table_staging_events = ("""
        CREATE TABLE IF NOT EXISTS staging_events (
            artist VARCHAR,
            auth VARCHAR,
            firstName VARCHAR,
            gender CHAR(1),
            itemInSession INTEGER,
            lastName VARCHAR,
            length FLOAT,
            level VARCHAR,
            location TEXT,
            method VARCHAR,
            page VARCHAR,
            registration FLOAT,
            sessionId INTEGER,
            song VARCHAR,
            status INTEGER,
            ts BIGINT,
            userAgent TEXT,
            userId INTEGER
        )
    """)

    Create_table_staging_songs = ("""
        CREATE TABLE IF NOT EXISTS staging_songs (
            num_songs INTEGER,
            artist_id VARCHAR,
            artist_name VARCHAR,
            artist_latitude FLOAT,
            artist_longitude FLOAT,
            artist_location TEXT,
            song_id VARCHAR,
            title VARCHAR,
            duration FLOAT,
            year INTEGER
        )
    """)

    Create_table_songplays = ("""
        CREATE TABLE IF NOT EXISTS songplays (
            songplay_id INTEGER IDENTITY(0,1) NOT NULL PRIMARY KEY,
            start_time TIMESTAMP,
            user_id INTEGER,
            level VARCHAR,
            song_id VARCHAR,
            artist_id VARCHAR,
            session_id INTEGER,
            location TEXT,
            user_agent TEXT
        )
    """)

    Create_table_users = ("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER NOT NULL PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            gender CHAR(1),
            level VARCHAR
        )
    """)

    Create_table_songs = ("""
        CREATE TABLE IF NOT EXISTS songs (
            song_id VARCHAR NOT NULL PRIMARY KEY,
            title VARCHAR,
            artist_id VARCHAR,
            year INT,
            duration FLOAT
        )
    """)

    Create_table_artists = ("""
        CREATE TABLE IF NOT EXISTS artists (
            artist_id VARCHAR NOT NULL PRIMARY KEY,
            name VARCHAR,
            location TEXT ,
            latitude FLOAT ,
            longitude FLOAT
        )
    """)

    Create_table_time = ("""
        CREATE TABLE IF NOT EXISTS time (
            start_time TIMESTAMP NOT NULL PRIMARY KEY,
            hour INTEGER,
            day INTEGER,
            week INTEGER,
            month INTEGER,
            year INTEGER,
            weekday VARCHAR
        )
    """)








        



        

        

        

