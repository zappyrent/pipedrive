import pandas as pd
from datetime import datetime
import mysqlcredentials as mc
import mysql.connector
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

def updatetime():
    try:
        connection = mysql.connector.connect(host=mc.host,
                                             database=mc.database,
                                             user=mc.user,
                                             password=mc.password)

        print("Connected to the database is successful")

        connurl = URL.create("mysql+mysqlconnector", username=mc.user, password=mc.password, host=mc.host, database=mc.database)
        engine = create_engine(connurl)

        df = pd.DataFrame({'timestamp': [datetime.now()]})
        df.to_sql('update_time', con=engine, if_exists='append', index=False)
        print("Data inserted successfully")

    except Exception as e:
        print("Error: ", e)
        print("Connection failed!")


updatetime()

