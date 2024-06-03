#!/usr/bin/env python
# coding: utf-8


import gspread
import mysqlcredentials as mc
import mysql.connector
from oauth2client.service_account import ServiceAccountCredentials

import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.engine import URL


creds_directory = '/home/elkin/etl/pipedrive/pipelines/GoogleSheetsToMySQL.json'

# initialize variables for gspread
scope = ['https://spreadsheets.google.com/feeds',
'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(creds_directory, scope)
client = gspread.authorize(creds)





# define method to pull data from spreadsheet
def GetSpreadsheetData(sheetName, worksheetIndex):
    sheet = client.open(sheetName).get_worksheet(worksheetIndex)
    return sheet.get_all_values()[1:]





def PreserveNULLValues(listName):
    print('Preserving NULL values…')
    for x in range(len(listName)):
        for y in range(len(listName[x])):
            if listName[x][y] == '':
                listName[x][y] = None
    print('NULL values preserved.')





data_jira = GetSpreadsheetData('db_jira', 1)





print(data_jira[0])
print(len(data_jira))





def WriteToMySQLTable_jira(sql_data, tableName):
    try:
        connection = mysql.connector.connect(
            user=mc.user,
            password=mc.password,
            host=mc.host,
            database=mc.database
        )
        sql_drop = " DROP TABLE IF EXISTS {} ".format(tableName)
        sql_create_table = """CREATE TABLE {}(
            Issue_type VARCHAR(100),
            Key_column VARCHAR(100),
            Summary VARCHAR(500),
            Assignee VARCHAR(100),
            Reporter VARCHAR(100),
            Priority VARCHAR(100),
            Status VARCHAR(100),
            Resolution VARCHAR(100),
            Created DATETIME,
            Updated DATE,
            Due DATE,
            Days_estimate VARCHAR(100),
            Resolved DATE,
            Status_category VARCHAR(100),
            Status_category_changed DATE,
            Status_transition VARCHAR(100),
            Status_transition_date DATE,
            Status_transition_from VARCHAR(100),
            Status_transition_id VARCHAR(100),
            Status_transition_to VARCHAR(100),
            Story_points_estimation VARCHAR(100)
            )""".format(tableName)

        sql_insert_statement = """INSERT INTO {}(
            Issue_type,
            Key_column,
            Summary,
            Assignee,
            Reporter,
            Priority,
            Status,
            Resolution,
            Created,
            Updated,
            Due,
            Days_estimate,
            Resolved,
            Status_category,
            Status_category_changed,
            Status_transition,
            Status_transition_date,
            Status_transition_from,
            Status_transition_id,
            Status_transition_to,
            Story_points_estimation)
        VALUES ( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""".format(tableName)

        cursor = connection.cursor()
        cursor.execute(sql_drop)
        print('Table {} has been dropped'.format(tableName))
        cursor.execute(sql_create_table)
        print('Table {} has been created'.format(tableName))

        for i in sql_data:
            cursor.execute(sql_insert_statement, i)

        connection.commit()
        print("Table {} successfully updated.".format(tableName))

    except mysql.connector.Error as error:
        connection.rollback()
        print("Error: {}. Table {} not updated!".format(error, tableName))

    finally:
        cursor.execute('SELECT COUNT(*) FROM {}'.format(tableName))
        rowCount = cursor.fetchone()[0]
        print(tableName, 'row count:', rowCount)
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed.")





PreserveNULLValues(data_jira)
WriteToMySQLTable_jira(data_jira, 'jira_test')


def updatetime():
    try:
        connection = mysql.connector.connect(host=mc.host,
                                             database=mc.database,
                                             user=mc.user,
                                             password=mc.password)

        print("Connected to the database is successful")

        connurl = URL.create("mysql+mysqlconnector", username=mc.user, password=mc.password, host=mc.host, database=mc.database)
        engine = create_engine(connurl)

        df = pd.DataFrame({'tableName': ['jira'], 'timestamp': [datetime.now()]})
        df.to_sql('update_time', con=engine, if_exists='append', index=False)
        print("Update time inserted successfully")

    except Exception as e:
        print("Error: ", e)
        print("Connection failed!")


updatetime()
