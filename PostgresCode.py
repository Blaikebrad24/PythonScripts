import os
import json
from os.path import isfile, join
import numpy as numpy
import psycopg2
import pandas as pd
from pandas import DataFrame
from pandas.io.json import json_normalize
from pprint import pprint
import glob


class DatabaseConnection:
    def __init__(self):
        try:
            self.connection = psycopg2.connect(
                host="localhost",
                dbname="udacitydb",
                user="blaikebradford",
                password="",
                port=""
            )
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            print("Connection to database successful")
            print("----------------------------------")
            print("----------------------------------")
        except psycopg2.Error as e:
            print(e)

    ##################################################
    def create_general_table(self, tableName):

        try:
            checkTableExists = "DROP TABLE IF EXISTS " + tableName + ";"
            self.cursor.execute(checkTableExists)

        except psycopg2.Error as e:
            print(e)

        try:
            createTable = "CREATE TABLE " + tableName + " (num_songs varchar(256), artist_id varchar(256), artist_latitude varchar(256), artist_longitude varchar(256), artist_location varchar(256), artist_name varchar(256), song_id varchar(256), title varchar(256), duration varchar(256), year varchar(256))"
            self.cursor.execute(createTable)

        except psycopg2.Error as e:
            print(e)

    ##################################################
    def insert_general_record(self, tableName, insertList):

        try:
            self.cursor.execute(
                "INSERT INTO " + tableName + "(num_songs,artist_id,artist_latitude,artist_longitude,artist_location,artist_name,song_id,title,duration,year) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s );",
                insertList)

        except psycopg2.Error as e:
            print(e)

    #################################################
    def create_songs_Table(self, tableName):

        # create DB Table will check to see if the table already exists, if it does it drops(deletes) table then creates a new one in the next try block statement
        try:
            checkTableExists = "DROP TABLE IF EXISTS " + tableName + ';'  # if the table already exists DROP(delete table)
            self.cursor.execute(checkTableExists)

        except psycopg2.Error as e:
            print(e)

        try:
            create_table_command = "CREATE TABLE " + tableName + "(song_id varchar(256), title varchar(256), artist_id varchar(256), year varchar(256), duration integer NOT NULL)"
            self.cursor.execute(create_table_command)

        except psycopg2.Error as e:
            print(e)

    #########################################################
    def insert_songs_newRecord(self, tableName, insertList):

        try:
            self.cursor.execute(
                "INSERT INTO " + tableName + "(song_id,title,artist_id,year,duration) VALUES (%s, %s, %s, %s, %s);",
                insertList)
        except psycopg2.Error as e:
            print(e)

    #########################################################
    def create_artists_Table(self, tableName):

        # create DB Table will check to see if the table already exists, if it does it drops(deletes) table then creates a new one in the next try block statement
        try:
            checkTableExists = "DROP TABLE IF EXISTS " + tableName + ';'  # if the table already exists DROP(delete table)
            self.cursor.execute(checkTableExists)

        except psycopg2.Error as e:
            print(e)

        try:
            create_table_command = "CREATE TABLE " + tableName + "(artist_id varchar(256), name varchar(256), location varchar(256), latitude varchar(256), longitude varchar(256))"
            self.cursor.execute(create_table_command)

        except psycopg2.Error as e:
            print(e)

    #############################################################

    def create_time_Table(self, tableName):

        try:
            checkIfItExists = "DROP TABLE IF EXISTS " + tableName + ';'
            self.cursor.execute(checkIfItExists)
        except psycopg2.Error as e:
            print(e)

        try:
            createTableCommand = "CREATE TABLE " + tableName + "(hour varchar(256), day varchar(256), week varchar(256), month varchar(256), year varchar(256), dayofweek varchar(256))"
            self.cursor.execute(createTableCommand)

        except psycopg2.Error as e:
            print(e)

    ############################################################
    def create_user_Table(self, tableName):

        try:
            checkIfTableExists = "DROP TABLE IF EXISTS " + tableName + ";"
            self.cursor.execute(checkIfTableExists)

        except psycopg2.Error as e:
            print(e)

        try:
            createTableCommand = "CREATE TABLE " + tableName + "(userID varchar(256), firstName varchar(256), lastName varchar(256), gender varchar(256), level varchar(256))"
            self.cursor.execute(createTableCommand)

        except psycopg2.Error as e:
            print(e)

    #############################################################
    def create_songPlays_Table(self, tableName):

        try:
            checkTableExists = "DROP TABLE IF EXISTS " + tableName + ";"
            self.cursor.execute(checkTableExists)

        except psycopg2.Error as e:
            print(e)

        try:
            createTableCommand = "CREATE TABLE " + tableName + "(timestamp varchar(256), userID varchar(256), level varchar(256), songID varchar(256), artistID varchar(256), sessionID varchar(256), location varchar(256), userAgent varchar(256))"
            self.cursor.execute(createTableCommand)

        except psycopg2.Error as e:
            print(e)

    #############################################################

    def insert_time_Table(self, tableName, insertList):

        try:
            self.cursor.execute(
                "INSERT INTO " + tableName + "(hour, day, week, month, year, dayofweek) VALUES (%s, %s, %s, %s, %s, %s);",
                insertList)

        except psycopg2.Error as e:
            print(e)

    ############################################################
    def insert_songplays(self, tableName, insertList):
        try:
            self.cursor.execute(
                "INSERT INTO " + tableName + "(timestamp, userID, level, songid, artistid, sessionId, location, userAgent) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s);",
                insertList)
        except psycopg2.Error as e:
            print(e)

    ############################################################
    def insert_user_Table(self, tableName, insertList):

        try:
            self.cursor.execute(
                "INSERT INTO " + tableName + "(userId, firstName, lastName, gender, level) VALUES (%s, %s, %s, %s, %s);",
                insertList)

        except psycopg2.Error as e:
            print(e)

    ############################################################
    def insert_artists_newRecord(self, tableName, insertList):

        try:
            self.cursor.execute(
                "INSERT INTO " + tableName + "(artist_id,name,location,latitude,longitude) VALUES (%s, %s, %s, %s, %s);",
                insertList)
        except psycopg2.Error as e:
            print(e)

    #########################################################
    def drop_tables(self, deleteTableName):
        try:
            deleteTableCommand = "DROP TABLE " + deleteTableName
            self.cursor.execute(deleteTableCommand)

        except psycopg2.Error as e:
            print(e)

    #########################################################
    def queryEverything(self, queryTableNameCommand):

        try:
            self.cursor.execute("SELECT * FROM " + queryTableNameCommand)
            tables = self.cursor.fetchall()
            for table in tables:
                pprint(table)

        except psycopg2.Error as e:
            print(e)

    def querySelect(self, tableName):

        try:
            if tableName == "SongsTable":
                self.cursor.execute("SELECT song_id FROM " + tableName + ";")
                data = self.cursor.fetchall()
                return data
            else:
                print("You are querying the wrong table")

        except psycopg2.Error as e:
            print(e)

    def update_record(self, updateTableCommand):
        updateTable = updateTableCommand
        # example - "UPDATE 'tableName' SET 'columnHeader=#' WHERE 'id=#' "
        self.cursor.execute(updateTable)

    # def query_for_songplays(self, songsTableName, artistTableName):
#########################################################
def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    return all_files


if __name__ == '__main__':
    # creating an object to the DatabaseConnection class
    database_connection = DatabaseConnection()
    generalTable = "GeneralTable"
    songs_TableName = "SongsTable"
    artistsTable = "ArtistTable"
    timeTable = "TimeTable"
    userTable = "UserTable"
    songsPlay = "Songsplays"
    database_connection.create_general_table(generalTable)
    database_connection.create_songs_Table(songs_TableName)
    database_connection.create_artists_Table(artistsTable)
    database_connection.create_time_Table(timeTable)
    database_connection.create_user_Table(userTable)
    database_connection.create_songPlays_Table(songsPlay)
    file_path = "data/song_data"
    filesArray = get_files(file_path)
    json_DataFrame = pd.DataFrame(
        columns=['num_songs', 'artist_id', 'artist_latitude', 'artist_longitude', 'artist_location', 'artist_name',
                 'song_id', 'title', 'duration', 'year'])
    for index, jsFile in enumerate(filesArray):
        with open(os.path.join(file_path, jsFile)) as json_file:
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

            generalTable_data = [num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name,
                                 song_id, title, duration, year]
            songs_data = [song_id, title, artist_id, year, duration]
            artists_data = [artist_id, artist_name, artist_location, artist_latitude, artist_longitude]
            database_connection.insert_general_record(generalTable, generalTable_data)
            database_connection.insert_songs_newRecord(songs_TableName, songs_data)
            database_connection.insert_artists_newRecord(artistsTable, artists_data)
            # print(str(num_songs) + " " + artist_id + " " + str(artist_latitude) + " " + str(artist_longitude) + " " + artist_location + " " + artist_name + " " + song_id + " " + title + " " + str(duration) + " " + str(year))
            # inputString = str(num_songs) + " " + artist_id + " " + str(artist_latitude) + " " + str(artist_longitude) + " " + artist_location + " " + artist_name + " " + song_id + " " + title + " " + str(duration) + " " + str(year)
            # print(inputString)
            # print(type(json_text))

    print(json_DataFrame)


    print("Completed General, songs and artists tables.")
    print("=============")
    filesPath = "data/log_data"
    logFilesArray = get_files(filesPath)
    jsonDataFrame = pd.DataFrame(
        columns=['artist', 'auth', 'firstName', 'gender', 'itemInSession', 'lastName', 'length', 'level', 'location',
                 'method', 'page', 'registration', 'sessionId', 'song', 'status', 'ts', 'userAgent', 'userId'])
    timeStampFrame = pd.DataFrame(columns=['hour', 'day', 'week', 'month', 'year', 'dayofweek'])
    user_df = pd.DataFrame(columns=['userId', 'firstName', 'lastName', 'gender', 'level'])
    matchingCount = 0
    matchingSongs = 0
    unmatchingTypes = 0
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
                # for jsonIndex, row in json_DataFrame.iterrows():
                #     carryList = []
                #     if type(artist) == str and type(row.artist_name) == str and artist == row.artist_name:
                #         matchingCount += 1
                #         if type(song) == str and type(row.title) == str and song == row.title:
                #             matchingSongs += 1
                #             songID = row.song_id
                #             artistID = row.artist_id
                #             # print("Artist ID " + artistID + " song ID " + songID)
                #             # print(type(artistID))
                #             # print(type(songID))
                #             carryList.append(songID)
                #             carryList.append(artistID)
                #             print(carryList)
                #             print("=====================================")
                #     else:
                #         unmatchingTypes += 1
                time_data = [ts.hour, ts.day, ts.week, ts.month, ts.year, ts.dayofweek]
                database_connection.insert_time_Table(timeTable, time_data)
                timeStampFrame.loc[theIndex] = time_data
                userAgent = jsonFrame['userAgent'][theIndex]
                userId = jsonFrame['userId'][theIndex]
                user_df = [userId, firstName, lastName, gender, level]
                database_connection.insert_user_Table(userTable, user_df)
                # print(songPlaysData)
                # database_connection.insert_songplays(songsPlay, songPlaysData)
                jsonDataFrame.loc[theIndex] = [artist, auth, firstName, gender, itemInSession, lastName, length, level,
                                               location, method, page, registration, sessionId, song, status, ts,
                                               userAgent, userId]
    print(jsonDataFrame)
    song_select = ("""SELECT songstable.song_id, artisttable.artist_id
    FROM songstable
    INNER JOIN artisttable on songstable.artist_id = artisttable.artist_id
    WHERE songstable.title = %s
    AND artisttable.name = %s
    AND songstable.duration = %s
    """)
    # JOIN artisttable ON songstable.artist_id=artisttable.artist_id
    # WHERE songstable.title = %s
    # AND artisttable.name = %s
    # AND songstable.duration = %s
    # database_connection.cursor.execute(song_select)
    # results = database_connection.cursor.fetchall()
    # print(results)
    # print(results)
    for index, row in jsonDataFrame.iterrows():
        database_connection.cursor.execute(song_select, (row.song, row.artist, row.length))
        results = database_connection.cursor.fetchone()
        if results:
            songid, artistid = results
            # print(songid + " " + artistid)
        else:
            songid, artistid = None, None
            # print("Songid and artistid are both NONE!")



        songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        database_connection.insert_songplays(songsPlay, songplay_data)
