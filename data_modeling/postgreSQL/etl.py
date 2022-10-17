import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This function takes in a database cursor object and a filepath.
    It:
    - reads in a JSON file at the specified filepath and converts this into a dataframe object
    - extracts song data from the dataframe and loads this into the database
    - extracts artist data from the dataframe and loads this into the database
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This function takes in a database cursor object and a filepath.
    It:
    - reads in a JSON file at the specified filepath and converts this into a dataframe object
    - filters for song information only
    - converts time data into an appropriate format
    - extracts time data from the dataframe and loads this into the database
    - extracts song play data from the dataframe and loads this into the database
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.query('page == "NextSong"')

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('timestamp', 'hour', 'day', 'week of year', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid,\
                     artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This function takes in a database cursor object, a database connection, a filepath and a function.
    It extracts the file paths for all json files which use the filepath as a root path.
    It then iterates over all extracted file paths and applies the specified function to each file.
    After the external function is executed, changes are commited to the database.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    This function executes the functions defined above.
    It connects to a database and inserts data into it.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()