import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import json


def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    return all_files



def process_song_file(cur, conn):
    filepath1 = "data/song_data"
    filesArray = get_files(filepath1)
    # print(filesArray)
    json_DataFrame = pd.DataFrame(
        columns=['num_songs', 'artist_id', 'artist_latitude', 'artist_longitude', 'artist_location', 'artist_name',
                 'song_id', 'title', 'duration', 'year'])
    for index, jsFile in enumerate(filesArray):
        with open(os.path.join(filepath1, jsFile)) as json_file:
            json_text = json.load(json_file)
            num_songs = json_text['num_songs']
            artist_id = json_text['artist_id']
            artist_latitude = json_text['artist_latitude']
            artist_longitude = json_text['artist_longitude']
            artist_location = json_text['artist_location']
            artist_name = json_text['artist_name']
            song_id = json_text['song_id']
            title = json_text['title']
            duration = json_text['duration']
            year = json_text['year']
            json_DataFrame.loc[index] = [num_songs, artist_id, artist_latitude, artist_longitude, artist_location,
                                         artist_name, song_id, title, duration, year]
            songs_data = [song_id, title, artist_id, year, duration]
            artists_data = [artist_id, artist_name, artist_location, artist_latitude, artist_longitude]
            # print(json_DataFrame.loc[index])
            try:
                cur.execute(song_table_insert, songs_data)
                conn.commit()
            except psycopg2.Error as e:
                print(e)

            try:
                cur.execute(artist_table_insert, artists_data)
                conn.commit()
            except psycopg2.Error as e:
                print(e)

    print(json_DataFrame)


def process_logFile(curr, conn):
    filesPath = "data/log_data"
    logFilesArray = get_files(filesPath)
    jsonDataFrame = pd.DataFrame(
        columns=['artist', 'auth', 'firstName', 'gender', 'itemInSession', 'lastName', 'length', 'level', 'location',
                 'method', 'page', 'registration', 'sessionId', 'song', 'status', 'ts', 'userAgent', 'userId'])
    timeStampFrame = pd.DataFrame(columns=['hour', 'day', 'week', 'month', 'year', 'dayofweek'])
    user_df = pd.DataFrame(columns=['userId', 'firstName', 'lastName', 'gender', 'level'])
    for index, jsonFile in enumerate(logFilesArray):
        with open(os.path.join(filesPath, jsonFile)) as jsonF:
            jsonText = json.load(jsonF)  # reads/loads json Data in 2018-11-01-events.json file
            jsonFrame = pd.DataFrame(jsonText)
            for theIndex in jsonFrame.index:
                artist = jsonFrame['artist'][theIndex]
                auth = jsonFrame['auth'][theIndex]
                firstName = jsonFrame['firstName'][theIndex]
                gender = jsonFrame['gender'][theIndex]
                itemInSession = jsonFrame['itemInSession'][theIndex]
                lastName = jsonFrame['lastName'][theIndex]
                length = jsonFrame['length'][theIndex]
                level = jsonFrame['level'][theIndex]
                location = jsonFrame['location'][theIndex]
                method = jsonFrame['method'][theIndex]
                page = jsonFrame['page'][theIndex]
                registration = jsonFrame['registration'][theIndex]
                sessionId = jsonFrame['sessionId'][theIndex]
                song = jsonFrame['song'][theIndex]
                status = jsonFrame['status'][theIndex]
                ts = pd.to_datetime(jsonFrame['ts'][theIndex])
                timestamp = ts
                if page == "NextSong":
                    time_data = [ts.hour, ts.day, ts.week, ts.month, ts.year, ts.dayofweek]
                    timeStampFrame.loc[theIndex] = time_data
                    # print(timeStampFrame.loc[theIndex])
                    try:
                        curr.execute(time_table_insert, time_data)
                        conn.commit()
                    except psycopg2.Error as e:
                        print(e)
                userAgent = jsonFrame['userAgent'][theIndex]
                userId = jsonFrame['userId'][theIndex]
                user_data = [userId, firstName, lastName, gender, level]
                jsonDataFrame.loc[theIndex] = [artist, auth, firstName, gender, itemInSession, lastName, length, level,
                                               location, method, page, registration, sessionId, song, status, ts,
                                               userAgent, userId]
                try:
                    curr.execute(user_table_insert, user_data)
                    conn.commit()
                except psycopg2.Error as e:
                    print(e)

                # try:
                #     curr.execute(time_table_insert, time_data)
                #     conn.commit()
                # except psycopg2.Error as e:
                #     print(e)

    song_select = ("""SELECT songstable.song_id, artisttable.artist_id
        FROM songstable
        INNER JOIN artisttable on songstable.artist_id = artisttable.artist_id
        WHERE songstable.title = %s
        AND artisttable.name = %s
        AND songstable.duration = %s
        """)
    for index, row in jsonDataFrame.iterrows():
        curr.execute(song_select, (row.song, row.artist, row.length))
        results = curr.fetchone()
        if results:
            songid, artistid = results
            # print(songid + " " + artistid)
        else:
            songid, artistid = None, None
            # print("Songid and artistid are both NONE!")

        songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        curr.execute(songplay_table_insert, songplay_data)
        conn.commit()


def main():
    conn = psycopg2.connect("host=localhost dbname=udacitydb user=blaikebradford")
    cur = conn.cursor()
    process_song_file(cur, conn)
    process_logFile(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()





