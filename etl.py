import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from datetime import datetime


def process_song_file(cur, filepath):
    """ Get information about songs and artists from raw data and introduce it
        in the SQL tables.

        Arguments:
            cur {object}: element that will represent a dataset determined by a T-SQL query. 
                          Cursors allow to traverse row by row, read and eventually modify this result set.
            filepath {str}: a string with the file path for each document.
            
        Returns:
            None
    
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data=[]
    song_data.append(df.song_id.values[0])
    song_data.append(df.title.values[0])
    song_data.append(df.artist_id.values[0])
    song_data.append(int(df.year.values[0]))
    song_data.append(df.duration.values[0])
    
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data=[]
    artist_data.append(df.artist_id.values[0])
    artist_data.append(df.artist_name.values[0])
    artist_data.append(df.artist_location.values[0])
    artist_data.append(df.artist_latitude.values[0])
    artist_data.append(df.artist_longitude.values[0]) 
    
    cur.execute(artist_table_insert, artist_data)

def get_fields_time(x):
    """ Get further information from datetime format 
    
        Arguments:
            x {dataframe}: each of the different rows per each log_data file
        
        Return:
            A list with all the values to insert into the time table
    """
    hour = x.hour
    day = x.day
    week = x.week
    month = x.month
    year = x.year
    weekday= x.dayofweek
    return [x,hour, day, week, month, year, weekday]

def process_log_file(cur, filepath):
    """ Get information about time and users from raw data and introduce it
        in the SQL tables and create the fact table songplays.

        Arguments:
            cur {object}: element that will represent a dataset determined by a T-SQL query. 
                          Cursors allow to traverse row by row, read and eventually modify this result set.
            filepath {str}: a string with the file path for each document.
            
        Returns:
            None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df_nxt = df.page=='NextSong'
    df = df[df_nxt]

    # convert timestamp column to datetime
    df.ts = df.ts/1000
    t = df.ts.map(datetime.fromtimestamp) 
    
    # insert time data records
    time_data = [get_fields_time(x) for x in t]
    column_labels = [ 'timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(time_data, columns=column_labels) 

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_data=[]
    for index, row in df.iterrows():
        user_id=row["userId"]
        first_name= row["firstName"]
        last_name= row["lastName"]
        gendre=row["gender"]
        level=row["level"]
    
        list_user=[user_id, first_name, last_name, gendre, level]
        if list_user not in user_data:
            user_data.append(list_user)
            
    column_labels = ['user_id', 'first_name', 'last_name', 'gender', 'level']

    user_df = pd.DataFrame(user_data, columns=column_labels)

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, tuple(row))

    # insert songplay records
    df.ts=df.ts.map(datetime.fromtimestamp)
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = []
        songplay_data.append(row.ts)
        songplay_data.append(row.userId)
        songplay_data.append(row.level)
        songplay_data.append(songid)
        songplay_data.append(artistid)
        songplay_data.append(row.sessionId)
        songplay_data.append(row.location)
        songplay_data.append(row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """ given a created DB space, this function gets the filepath and give it as 
        an arguments to th etwo functions that will process tha raw data.
        
        Arguments: 
            cur {object}: element that will represent a dataset determined by a T-SQL query. 
                          Cursors allow to traverse row by row, read and eventually modify this result set.
            conn {object}: The connector allows you to connect to the following databases and perform 
                           multiple database actions
            filepath {str}: a string with the file path for each document.
            func {function}: a specific function name to execute
            
        Return:
            None
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()