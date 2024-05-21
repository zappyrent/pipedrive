import gspread
import mysqlcredentials as mc
import mysql.connector
from oauth2client.service_account import ServiceAccountCredentials
import os

# initialize variables for gspread
creds_directory = str(os.path.dirname(os.path.abspath('__file__'))) + '/GoogleSheetsToMySQL.json'
scope = ['https://spreadsheets.google.com/feeds',
'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(creds_directory, scope)
client = gspread.authorize(creds)

# define method to pull data from spreadsheet
def GetSpreadsheetData(sheetName, worksheetIndex):
    sheet = client.open(sheetName).get_worksheet(worksheetIndex)
    return sheet.get_all_values()[1:]

data2=GetSpreadsheetData('db_adwords', 1)

print(data2[0])
print(len(data2))


# define method to write list of data to MySQL table
def WriteToMySQLTable2(sql_data, tableName):
    # we are using a try/except block (also called a try/catch block in other languages) which is good for error handling. It will “try” to execute anything in the “try” block, and if there is an error, it will report the error in the “except” block. Regardless of any errors, the “finally” block will always be executed.
    try:
        # Here we include the connection credentials for MySQL. We create a connection object that we pass the credentials to, and notice that we can specify the database which is ‘sys’ in the MySQLCredentials.py file because I’m using since I’m using the default database in MySQL Workbench 8.0.
        connection = mysql.connector.connect(
            user=mc.user,
            password=mc.password,
            host=mc.host,
            database=mc.database
        )
        # This command will drop the table, and we could just have the table name hardcoded into the string, but instead I am using the name of the table passed into the method. {} is a placeholder for what we want to pass into this string, and using .format(blah) we can pass the string name from the variable passed into the method here.
        sql_drop = " DROP TABLE IF EXISTS {} ".format(tableName)
        # Now we will create the table, and the triple quotes are used so that when we go to the next line of code, we remain in a string. Otherwise it will terminate the string at the end of the line, and we want ALL of this to be one giant string. When injecting data into VALUES, we use the placeholder %s for each column of data we have.
        sql_create_table = """CREATE TABLE {}( 
            Day DATE,
            Campaign VARCHAR(100),
            Region VARCHAR(100),
            Cost VARCHAR(100),
            Impressions VARCHAR(100),
            Clicks VARCHAR(100),
            Campaign_metabase VARCHAR(100)
            )""".format(tableName)

        sql_insert_statement = """INSERT INTO {}( 
            Day,
            Campaign,
            Region,
            Cost,
            Impressions,
            Clicks,
            Campaign_metabase)
        VALUES ( %s,%s,%s,%s,%s,%s,%s)""".format(tableName)
        # Here we create a cursor, which we will use to execute the MySQL statements above. After each statement is executed, a message will be printed to the console if the execution was successful.
        cursor = connection.cursor()
        cursor.execute(sql_drop)
        print('Table {} has been dropped'.format(tableName))
        cursor.execute(sql_create_table)
        print('Table {} has been created'.format(tableName))
        # We need to write each row of data to the table, so we use a for loop that will insert each row of data one at a time
        for i in sql_data:
            cursor.execute(sql_insert_statement, i)
        # Now we execute the commit statement, and print to the console that the table was updated successfully
        connection.commit()
        print("Table {} successfully updated.".format(tableName))
    # Errors are handled in the except block, and we will get the information printed to the console if there is an error
    except mysql.connector.Error as error:
        connection.rollback()
        print("Error: {}. Table {} not updated!".format(error, tableName))
    # We need to close the cursor and the connection, and this needs to be done regardless of what happened above.
    finally:
        cursor.execute('SELECT COUNT(*) FROM {}'.format(tableName))
        rowCount = cursor.fetchone()[0]
        print(tableName, 'row count:', rowCount)
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed.")

WriteToMySQLTable2(data2, 'marketing_adwords')
