#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import psycopg2
import os
import sys
import mysqlcredentials as mc
import mysql.connector
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import mysql.connector
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

#!/usr/bin/env python
# coding: utf-8

def connect():
    connection = None
    try:
        print('Connecting…')
        connection = psycopg2.connect(user="rmt",
                                      password="U9tt4[C$4Zv",
                                      host="postgres-production.cx53soegx3qk.eu-west-1.rds.amazonaws.com",
                                      dbname="postgres")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print('All good, Connection successful!')
    return connection





def sql_to_dataframe(conn, query, column_names):
   """
   Import data from a PostgreSQL database using a SELECT query
   """
   cursor = conn.cursor()
   try:
      cursor.execute(query)
   except (Exception, psycopg2.DatabaseError) as error:
       print("Error: %s" % error)
       cursor.close()
       return 1

   tuples_list = cursor.fetchall()
   cursor.close()
   # Now we need to transform the list into a pandas DataFrame:
   df = [list(i) for i in tuples_list]
   return df





#creating a query variable to store our query to pass into the function
query_booking_details = """SELECT booking_id,
                                  reason,
                                  lost,
                                  richieste_speciali,
                                  ready_for,
                                  certificazione_concordato,
                                  ape_richiesta,
                                  serve_planimetria,
                                  runner,
                                  ready_for_date
                           FROM booking_details"""
#creating a list with columns names to pass into the function
column_names_booking_details = ['booking_id','booking_id','reason','lost','richieste_speciali','ready_for','certificazione_concordato','ape_richiesta','serve_planimetria','runner','ready_for_date']

#creating a query variable to store our query to pass into the function
query_booking = """SELECT id,
                          user_id,
                          pz,
                          status,
                          created_at,
                          updated_at,
                          property_owner_id,
                          property_id,
                          pending_at,
                          accepted_at,
                          is_verification_successful,
                          booking_legacy_id,
                          verification_date,
                          currency,
                          setup_intent,
                          transfers_enabled,
                          parent_booking_id,
                          scheduled_checkin_datetime,
                          ready_for_credit_check
                    FROM bookings"""

#creating a list with columns names to pass into the function
column_names_booking = ['id','user_id','pz','status','created_at','updated_at','property_owner_id','property_id','pending_at','accepted_at','is_verification_successful','booking_legacy_id','verification_date','currency','setup_intent','transfers_enabled','parent_booking_id','scheduled_checkin_datetime','ready_for_credit_check']

#creating a query variable to store our query to pass into the function
query_booking_properties = """SELECT id,
                          owner_id,
                          pz,
                          status,
                          type,
                          created_at,
                          updated_at,
                          total_housemates,
                          without_images_at,
                          name,
                          source,
                          activated_at,
                          subscription,
                          cadastral_sheet,
                          cadastral_parcel,
                          cadastral_subordinate,
                          cadastral_category,
                          cadastral_income,
                          has_zappyrent_keys,
                          switch_bills_to_tenant_name,
                          exclusive,
                          runner_email,
                          allow_children,
                          allow_students,
                          max_number_of_guests,
                          disabled_at,
                          disabled_reason
                    FROM properties"""

#creating a list with columns names to pass into the function
column_names_booking_properties = ['id','owner_id','pz','status','type','created_at','updated_at','total_housemates','without_images_at','name','source','activated_at','subscription','cadastral_sheet','cadastral_parcel','cadastral_subordinate','cadastral_category','cadastral_income','has_zappyrent_keys','switch_bills_to_tenant_name','exclusive','runner_email','allow_children','allow_students','max_number_of_guests','disabled_at','disabled_reason']

#creating a query variable to store our query to pass into the function
query_lead_properties = """SELECT id,
                          my_phoner_id,
                          property_id,
                          landlord_id,
                          status,
                          phone_no,
                          email,
                          worked_by,
                          created_at,
                          updated_at,
                          source,
                          category,
                          name,
                          city,
                          pipedrive_id,
                          pipedrive_owner_id,
                          pipedrive_owner_email
                    FROM postgres.api_gateway.lead_properties
                """

#creating a list with columns names to pass into the function
column_names_lead_properties = ['id','my_phoner_id','property_id','landlord_id','status','phone_no','email','worked_by','created_at','updated_at','source','category','name','city','pipedrive_id','pipedrive_owner_id','pipedrive_owner_email']

#creating a query variable to store our query to pass into the function
query_visits = """SELECT id,
                          status,
                          type,
                          property_id,
                          user_id,
                          email,
                          phone_no,
                          proposed_date_time,
                          proposed_duration,
                          created_at,
                          updated_at,
                          event_id,
                          comment,
                          outcome,
                          alternative_date_time,
                          video_visit,
                          note,
                          source,
                          created_by,
                          deletion_reason,
                          deleted_by,
                          tenant_full_name,
                          is_verified,
                          number_of_adult_guests,
                          number_of_children_guests,
                          has_pets_guests,
                          lease_term_request,
                          tenant_profession
                    FROM postgres.api_gateway.visits
                """

#creating a list with columns names to pass into the function
column_names_visits = ['id','status','type','property_id','user_id','email','phone_no','proposed_date_time','proposed_duration','created_at','updated_at','event_id','comment','outcome','alternative_date_time','video_visit','note','source','created_by','deletion_reason','deleted_by','tenant_full_name','is_verified','number_of_adult_guests','number_of_children_guests','has_pets_guests','lease_term_request','tenant_profession']

#creating a query variable to store our query to pass into the function
query_client_account_managers = """SELECT client_id,
                                           account_manager_id
                                    FROM postgres.rmt.client_account_managers
                                """

#creating a list with columns names to pass into the function
column_names_client_account_manager = ['client_id','account_manager_id']

#creating a query variable to store our query to pass into the function
query_account_managers = """SELECT id,
                                   status,
                                   email,
                                   password,
                                   name,
                                   type,
                                   phone
                            FROM postgres.rmt.account_managers
                        """

#creating a list with columns names to pass into the function
column_names_account_managers = ['id','status','email','password','name','type','phone']

#creating a query variable to store our query to pass into the function
query_properties_next_available_slots = """SELECT id,
                                                  property_id,
                                                  next_available_slot,
                                                  created_at,
                                                  updated_at
                            FROM postgres.api_gateway.properties_next_available_slots
                        """

#creating a list with columns names to pass into the function
column_names_properties_next_available_slots = ['id','property_id','next_available_slot','created_at','updated_at']

#creating a query variable to store our query to pass into the function
query_listing_highlight = """SELECT listing_id,
                                    highlight
                            FROM postgres.rmt.listing_highlight
                        """

#creating a list with columns names to pass into the function
column_names_listing_highlight = ['listing_id','highlight']

#creating a query variable to store our query to pass into the function
query_events= """SELECT id,
                          sf_id,
                          subject,
                          location,
                          is_all_day,
                          start_date_time,
                          end_date_time,
                          start_date,
                          duration_in_min,
                          description,
                          lead_id,
                          status,
                          created_at,
                          updated_at,
                          created_by,
                          owner_id
                            FROM postgres.api_gateway.events
                        """

#creating a list with columns names to pass into the function
column_names_events = ['id','sf_id','subject','location','is_all_day','start_date_time','end_date_time','start_date','duration_in_min','description','lead_id','status','created_at','updated_at','created_by','owner_id']

#creating a query variable to store our query to pass into the function
query_properties2= """SELECT id,
                            owner_id,
                            pz,
                            status,
                            type,
                            created_at,
                            updated_at,
                            total_housemates,
                            without_images_at,
                            landlord_availability,
                            name,
                            activated_at,
                            subscription,
                            cadastral_sheet,
                            cadastral_parcel,
                            cadastral_subordinate,
                            cadastral_category,
                            cadastral_income,
                            has_zappyrent_keys,
                            switch_bills_to_tenant_name,
                            exclusive,
                            runner_email,
                            allow_children,
                            allow_students,
                            max_number_of_guests,
                            matterport_link,
                            disabled_at,
                            disabled_reason
                            FROM postgres.api_gateway.properties
                        """

#creating a list with columns names to pass into the function
column_names_properties2 = ['id','owner_id','pz','status','type','created_at','updated_at','total_housemates','without_images_at','landlord_availability','name','activated_at','subscription','cadastral_sheet','cadastral_parcel','cadastral_subordinate','cadastral_category','cadastral_income','has_zappyrent_keys','switch_bills_to_tenant_name','exclusive','runner_email','allow_children','allow_students','max_number_of_guests','matterport_link','disabled_at','disabled_reason']

                          #creating a query variable to store our query to pass into the function

query_booking_attachments= """SELECT id,
                          status,
                          type,
                          filename,
                          created_at,
                          updated_at,
                          booking_id,
                          retrieval_type,
                          description
                            FROM postgres.api_gateway.booking_attachments
                        """

#creating a list with columns names to pass into the function
column_names_booking_attachments = ['id','status','type','filename','created_at','updated_at','booking_id','retrieval_type','description']





#opening the connection
conn = connect()
#loading our dataframe
data_booking_details = sql_to_dataframe(conn, query_booking_details, column_names_booking_details)
data_bookings = sql_to_dataframe(conn, query_booking, column_names_booking)
data_booking_properties = sql_to_dataframe(conn, query_booking_properties, column_names_booking_properties)
data_lead_properties = sql_to_dataframe(conn, query_lead_properties, column_names_lead_properties)
data_visits = sql_to_dataframe(conn, query_visits, column_names_visits)
data_client_account_managers = sql_to_dataframe(conn, query_client_account_managers, column_names_client_account_manager)
data_account_managers = sql_to_dataframe(conn, query_account_managers, column_names_account_managers)
data_properties_next_available_slots = sql_to_dataframe(conn, query_properties_next_available_slots, column_names_properties_next_available_slots)
data_listing_highlight = sql_to_dataframe(conn, query_listing_highlight, column_names_listing_highlight)
data_events = sql_to_dataframe(conn, query_events, column_names_events)
data_properties = sql_to_dataframe(conn, query_properties2, column_names_properties2)
data_booking_attachments = sql_to_dataframe(conn, query_booking_attachments, column_names_booking_attachments)
#closing the connection
conn.close()
# Let’s see if we loaded the df successfully





df_booking = pd.DataFrame([data_bookings])





df_booking





def PreserveNULLValues(listName):
    print('Preserving NULL values…')
    for x in range(len(listName)):
        for y in range(len(listName[x])):
            if listName[x][y] == '':
                listName[x][y] = None
    print('NULL values preserved.')





# define method to write list of data to MySQL table
def WriteToMySQLTable_bookings(sql_data, tableName):
    try:
        connection = mysql.connector.connect(
        user = mc.user,
        password = mc.password,
        host = mc.host,
        database = mc.database
        )

        sql_drop = " DROP TABLE IF EXISTS {} ".format(tableName)
        sql_create_table = """CREATE TABLE {}(
                          id VARCHAR(100),
                          user_id VARCHAR(100),
                          pz VARCHAR(100),
                          status VARCHAR(100),
                          created_at DATE,
                          updated_at DATE,
                          property_owner_id VARCHAR(100),
                          property_id VARCHAR(100),
                          pending_at DATE,
                          accepted_at DATE,
                          is_verification_successful VARCHAR(100),
                          booking_legacy_id VARCHAR(100),
                          verification_date DATE,
                          currency VARCHAR(100),
                          setup_intent VARCHAR(100),
                          transfers_enabled VARCHAR(100),
                          parent_booking_id VARCHAR(100),
                          scheduled_checkin_datetime DATE,
                          ready_for_credit_check VARCHAR(100)
            )""".format(tableName)

        sql_insert_statement = """INSERT INTO {}(
                          id,
                          user_id,
                          pz,
                          status,
                          created_at,
                          updated_at,
                          property_owner_id,
                          property_id,
                          pending_at,
                          accepted_at,
                          is_verification_successful,
                          booking_legacy_id,
                          verification_date,
                          currency,
                          setup_intent,
                          transfers_enabled,
                          parent_booking_id,
                          scheduled_checkin_datetime,
                          ready_for_credit_check)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""".format(tableName)

        cursor = connection.cursor()
        cursor.execute(sql_drop)
        print('Table {} has been dropped'.format(tableName))
        cursor.execute(sql_create_table)
        print('Table {} has been created'.format(tableName))

        for i in sql_data:
            cursor.execute(sql_insert_statement,i)

        connection.commit()
        print("Table {} successfully updated.".format(tableName))

    except mysql.connector.Error as error :
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





# define method to write list of data to MySQL table
def WriteToMySQLTable_booking_details(sql_data, tableName):
    try:
        connection = mysql.connector.connect(
        user = mc.user,
        password = mc.password,
        host = mc.host,
        database = mc.database
        )

        sql_drop = " DROP TABLE IF EXISTS {} ".format(tableName)
        sql_create_table = """CREATE TABLE {}(
            booking_id VARCHAR(100),
            reason VARCHAR(100),
            lost VARCHAR(100),
            richieste_speciali VARCHAR(100),
            ready_for VARCHAR(100),
            certificazione_concordato VARCHAR(100),
            ape_richiesta VARCHAR(100),
            serve_planimetria VARCHAR(100),
            runner VARCHAR(100),
            ready_for_date DATE
            )""".format(tableName)

        sql_insert_statement = """INSERT INTO {}(
            booking_id,
            reason,
            lost,
            richieste_speciali,
            ready_for,
            certificazione_concordato,
            ape_richiesta,
            serve_planimetria,
            runner,
            ready_for_date)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""".format(tableName)

        cursor = connection.cursor()
        cursor.execute(sql_drop)
        print('Table {} has been dropped'.format(tableName))
        cursor.execute(sql_create_table)
        print('Table {} has been created'.format(tableName))

        for i in sql_data:
            cursor.execute(sql_insert_statement,i)

        connection.commit()
        print("Table {} successfully updated.".format(tableName))

    except mysql.connector.Error as error :
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





# define method to write list of data to MySQL table
def WriteToMySQLTable_booking_properties(sql_data, tableName):
    try:
        connection = mysql.connector.connect(
        user = mc.user,
        password = mc.password,
        host = mc.host,
        database = mc.database
        )

        sql_drop = " DROP TABLE IF EXISTS {} ".format(tableName)
        sql_create_table = """CREATE TABLE {}(
                          id VARCHAR(100),
                          owner_id VARCHAR(100),
                          pz VARCHAR(100),
                          status VARCHAR(100),
                          type VARCHAR(100),
                          created_at DATE,
                          updated_at DATE,
                          total_housemates VARCHAR(100),
                          without_images_at DATE,
                          name VARCHAR(100),
                          source VARCHAR(100),
                          activated_at DATE,
                          subscription VARCHAR(100),
                          cadastral_sheet VARCHAR(100),
                          cadastral_parcel VARCHAR(100),
                          cadastral_subordinate VARCHAR(100),
                          cadastral_category VARCHAR(100),
                          cadastral_income VARCHAR(100),
                          has_zappyrent_keys VARCHAR(100),
                          switch_bills_to_tenant_name VARCHAR(100),
                          exclusive VARCHAR(100),
                          runner_email VARCHAR(100),
                          allow_children VARCHAR(100),
                          allow_students VARCHAR(100),
                          max_number_of_guests VARCHAR(100),
                          disabled_at DATE,
                          disabled_reason VARCHAR(100)
            )""".format(tableName)

        sql_insert_statement = """INSERT INTO {}(
                          id,
                          owner_id,
                          pz,
                          status,
                          type,
                          created_at,
                          updated_at,
                          total_housemates,
                          without_images_at,
                          name,
                          source,
                          activated_at,
                          subscription,
                          cadastral_sheet,
                          cadastral_parcel,
                          cadastral_subordinate,
                          cadastral_category,
                          cadastral_income,
                          has_zappyrent_keys,
                          switch_bills_to_tenant_name,
                          exclusive,
                          runner_email,
                          allow_children,
                          allow_students,
                          max_number_of_guests,
                          disabled_at,
                          disabled_reason
            )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""".format(tableName)

        cursor = connection.cursor()
        cursor.execute(sql_drop)
        print('Table {} has been dropped'.format(tableName))
        cursor.execute(sql_create_table)
        print('Table {} has been created'.format(tableName))

        for i in sql_data:
            cursor.execute(sql_insert_statement,i)

        connection.commit()
        print("Table {} successfully updated.".format(tableName))

    except mysql.connector.Error as error :
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





# define method to write list of data to MySQL table
def WriteToMySQLTable_lead_properties(sql_data, tableName):
    try:
        connection = mysql.connector.connect(
        user = mc.user,
        password = mc.password,
        host = mc.host,
        database = mc.database
        )

        sql_drop = " DROP TABLE IF EXISTS {} ".format(tableName)
        sql_create_table = """CREATE TABLE {}(
                          id VARCHAR(100),
                          my_phoner_id VARCHAR(100),
                          property_id VARCHAR(100),
                          landlord_id VARCHAR(100),
                          status VARCHAR(100),
                          phone_no VARCHAR(100),
                          email VARCHAR(100),
                          worked_by VARCHAR(100),
                          created_at VARCHAR(100),
                          updated_at VARCHAR(100),
                          source VARCHAR(100),
                          category VARCHAR(100),
                          name VARCHAR(100),
                          city VARCHAR(100),
                          pipedrive_id VARCHAR(100),
                          pipedrive_owner_id VARCHAR(100),
                          pipedrive_owner_email VARCHAR(100)
                          )""".format(tableName)

        sql_insert_statement = """INSERT INTO {}(
                          id,
                          my_phoner_id,
                          property_id,
                          landlord_id,
                          status,
                          phone_no,
                          email,
                          worked_by,
                          created_at,
                          updated_at,
                          source,
                          category,
                          name,
                          city,
                          pipedrive_id,
                          pipedrive_owner_id,
                          pipedrive_owner_email
            )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""".format(tableName)

        cursor = connection.cursor()
        cursor.execute(sql_drop)
        print('Table {} has been dropped'.format(tableName))
        cursor.execute(sql_create_table)
        print('Table {} has been created'.format(tableName))

        for i in sql_data:
            cursor.execute(sql_insert_statement,i)

        connection.commit()
        print("Table {} successfully updated.".format(tableName))

    except mysql.connector.Error as error :
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





# define method to write list of data to MySQL table
def WriteToMySQLTable_visits(sql_data, tableName):
    try:
        connection = mysql.connector.connect(
        user = mc.user,
        password = mc.password,
        host = mc.host,
        database = mc.database
        )

        sql_drop = " DROP TABLE IF EXISTS {} ".format(tableName)
        sql_create_table = """CREATE TABLE {}(
                          id VARCHAR(100),
                          status VARCHAR(100),
                          type VARCHAR(100),
                          property_id VARCHAR(100),
                          user_id VARCHAR(100),
                          email VARCHAR(100),
                          phone_no VARCHAR(100),
                          proposed_date_time VARCHAR(100),
                          proposed_duration VARCHAR(100),
                          created_at VARCHAR(100),
                          updated_at VARCHAR(100),
                          event_id VARCHAR(100),
                          comment VARCHAR(100),
                          outcome VARCHAR(100),
                          alternative_date_time VARCHAR(100),
                          video_visit VARCHAR(100),
                          note VARCHAR(100),
                          source VARCHAR(100),
                          created_by VARCHAR(100),
                          deletion_reason VARCHAR(100),
                          deleted_by VARCHAR(100),
                          tenant_full_name VARCHAR(100),
                          is_verified VARCHAR(100),
                          number_of_adult_guests VARCHAR(100),
                          number_of_children_guests VARCHAR(100),
                          has_pets_guests VARCHAR(100),
                          lease_term_request VARCHAR(100),
                          tenant_profession VARCHAR(100)
                          )""".format(tableName)

        sql_insert_statement = """INSERT INTO {}(
                          id,
                          status,
                          type,
                          property_id,
                          user_id,
                          email,
                          phone_no,
                          proposed_date_time,
                          proposed_duration,
                          created_at,
                          updated_at,
                          event_id,
                          comment,
                          outcome,
                          alternative_date_time,
                          video_visit,
                          note,
                          source,
                          created_by,
                          deletion_reason,
                          deleted_by,
                          tenant_full_name,
                          is_verified,
                          number_of_adult_guests,
                          number_of_children_guests,
                          has_pets_guests,
                          lease_term_request,
                          tenant_profession
            )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""".format(tableName)

        cursor = connection.cursor()
        cursor.execute(sql_drop)
        print('Table {} has been dropped'.format(tableName))
        cursor.execute(sql_create_table)
        print('Table {} has been created'.format(tableName))

        for i in sql_data:
            cursor.execute(sql_insert_statement,i)

        connection.commit()
        print("Table {} successfully updated.".format(tableName))

    except mysql.connector.Error as error :
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





# define method to write list of data to MySQL table
def WriteToMySQLTable_client_account_managers(sql_data, tableName):
    try:
        connection = mysql.connector.connect(
        user = mc.user,
        password = mc.password,
        host = mc.host,
        database = mc.database
        )

        sql_drop = " DROP TABLE IF EXISTS {} ".format(tableName)

        sql_create_table = """CREATE TABLE {}(
                          client_id VARCHAR(100),
                          account_manager_id VARCHAR(100)
                          )""".format(tableName)

        sql_insert_statement = """INSERT INTO {}(
                          client_id,
                          account_manager_id)
        VALUES (%s,%s)""".format(tableName)

        cursor = connection.cursor()
        cursor.execute(sql_drop)
        print('Table {} has been dropped'.format(tableName))
        cursor.execute(sql_create_table)
        print('Table {} has been created'.format(tableName))

        for i in sql_data:
            cursor.execute(sql_insert_statement,i)

        connection.commit()
        print("Table {} successfully updated.".format(tableName))

    except mysql.connector.Error as error :
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





# define method to write list of data to MySQL table
def WriteToMySQLTable_account_managers(sql_data, tableName):
    try:
        connection = mysql.connector.connect(
        user = mc.user,
        password = mc.password,
        host = mc.host,
        database = mc.database
        )

        sql_drop = " DROP TABLE IF EXISTS {} ".format(tableName)

        sql_create_table = """CREATE TABLE {}(
                          id VARCHAR(100),
                          status VARCHAR(100),
                          email VARCHAR(100),
                          password VARCHAR(100),
                          name VARCHAR(100),
                          type VARCHAR(100),
                          phone VARCHAR(100)
                          )""".format(tableName)

        sql_insert_statement = """INSERT INTO {}(
                          id,
                          status,
                          email,
                          password,
                          name,
                          type,
                          phone)
        VALUES (%s,%s,%s,%s,%s,%s,%s)""".format(tableName)

        cursor = connection.cursor()
        cursor.execute(sql_drop)
        print('Table {} has been dropped'.format(tableName))
        cursor.execute(sql_create_table)
        print('Table {} has been created'.format(tableName))

        for i in sql_data:
            cursor.execute(sql_insert_statement,i)

        connection.commit()
        print("Table {} successfully updated.".format(tableName))

    except mysql.connector.Error as error :
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





# define method to write list of data to MySQL table
def WriteToMySQLTable_properties_next_available_slots(sql_data, tableName):
    try:
        connection = mysql.connector.connect(
        user = mc.user,
        password = mc.password,
        host = mc.host,
        database = mc.database
        )

        sql_drop = " DROP TABLE IF EXISTS {} ".format(tableName)

        sql_create_table = """CREATE TABLE {}(
                          id VARCHAR(100),
                          property_id VARCHAR(100),
                          next_available_slot VARCHAR(100),
                          created_at VARCHAR(100),
                          updated_at VARCHAR(100)
                          )""".format(tableName)

        sql_insert_statement = """INSERT INTO {}(
                          id,
                          property_id,
                          next_available_slot,
                          created_at,
                          updated_at)
        VALUES (%s,%s,%s,%s,%s)""".format(tableName)

        cursor = connection.cursor()
        cursor.execute(sql_drop)
        print('Table {} has been dropped'.format(tableName))
        cursor.execute(sql_create_table)
        print('Table {} has been created'.format(tableName))

        for i in sql_data:
            cursor.execute(sql_insert_statement,i)

        connection.commit()
        print("Table {} successfully updated.".format(tableName))

    except mysql.connector.Error as error :
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





# define method to write list of data to MySQL table
def WriteToMySQLTable_listing_highlight(sql_data, tableName):
    try:
        connection = mysql.connector.connect(
        user = mc.user,
        password = mc.password,
        host = mc.host,
        database = mc.database
        )

        sql_drop = " DROP TABLE IF EXISTS {} ".format(tableName)

        sql_create_table = """CREATE TABLE {}(
                          listing_id VARCHAR(100),
                          highlight VARCHAR(100)
                          )""".format(tableName)

        sql_insert_statement = """INSERT INTO {}(
                          listing_id,
                          highlight)
        VALUES (%s,%s)""".format(tableName)

        cursor = connection.cursor()
        cursor.execute(sql_drop)
        print('Table {} has been dropped'.format(tableName))
        cursor.execute(sql_create_table)
        print('Table {} has been created'.format(tableName))

        for i in sql_data:
            cursor.execute(sql_insert_statement,i)

        connection.commit()
        print("Table {} successfully updated.".format(tableName))

    except mysql.connector.Error as error :
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





# define method to write list of data to MySQL table
def WriteToMySQLTable_events(sql_data, tableName):
    try:
        connection = mysql.connector.connect(
        user = mc.user,
        password = mc.password,
        host = mc.host,
        database = mc.database
        )

        sql_drop = " DROP TABLE IF EXISTS {} ".format(tableName)

        sql_create_table = """CREATE TABLE {}(
                          id VARCHAR(100),
                          sf_id VARCHAR(100),
                          subject VARCHAR(100),
                          location VARCHAR(100),
                          is_all_day VARCHAR(100),
                          start_date_time VARCHAR(100),
                          end_date_time VARCHAR(100),
                          start_date VARCHAR(100),
                          duration_in_min VARCHAR(100),
                          description VARCHAR(100),
                          lead_id VARCHAR(100),
                          status VARCHAR(100),
                          created_at VARCHAR(100),
                          updated_at VARCHAR(100),
                          created_by VARCHAR(100),
                          owner_id VARCHAR(100)
                          )""".format(tableName)

        sql_insert_statement = """INSERT INTO {}(
                          id,
                          sf_id,
                          subject,
                          location,
                          is_all_day,
                          start_date_time,
                          end_date_time,
                          start_date,
                          duration_in_min,
                          description,
                          lead_id,
                          status,
                          created_at,
                          updated_at,
                          created_by,
                          owner_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""".format(tableName)

        cursor = connection.cursor()
        cursor.execute(sql_drop)
        print('Table {} has been dropped'.format(tableName))
        cursor.execute(sql_create_table)
        print('Table {} has been created'.format(tableName))

        for i in sql_data:
            cursor.execute(sql_insert_statement,i)

        connection.commit()
        print("Table {} successfully updated.".format(tableName))

    except mysql.connector.Error as error :
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





# define method to write list of data to MySQL table
def WriteToMySQLTable_properties(sql_data, tableName):
    try:
        connection = mysql.connector.connect(
        user = mc.user,
        password = mc.password,
        host = mc.host,
        database = mc.database
        )

        sql_drop = " DROP TABLE IF EXISTS {} ".format(tableName)

        sql_create_table = """CREATE TABLE {}(
                          id VARCHAR(100),
                            owner_id VARCHAR(100),
                            pz VARCHAR(100),
                            status VARCHAR(100),
                            type VARCHAR(100),
                            created_at VARCHAR(100),
                            updated_at DATE,
                            total_housemates VARCHAR(100),
                            without_images_at VARCHAR(100),
                            landlord_availability VARCHAR(100),
                            name VARCHAR(100),
                            activated_at DATE,
                            subscription VARCHAR(100),
                            cadastral_sheet VARCHAR(100),
                            cadastral_parcel VARCHAR(100),
                            cadastral_subordinate VARCHAR(100),
                            cadastral_category VARCHAR(100),
                            cadastral_income VARCHAR(100),
                            has_zappyrent_keys VARCHAR(100),
                            switch_bills_to_tenant_name VARCHAR(100),
                            exclusive VARCHAR(100),
                            runner_email VARCHAR(100),
                            allow_children VARCHAR(100),
                            allow_students VARCHAR(100),
                            max_number_of_guests VARCHAR(100),
                            matterport_link VARCHAR(100),
                            disabled_at VARCHAR(100),
                            disabled_reason VARCHAR(100)
                          )""".format(tableName)

        sql_insert_statement = """INSERT INTO {}(
                          id,
                            owner_id,
                            pz,
                            status,
                            type,
                            created_at,
                            updated_at,
                            total_housemates,
                            without_images_at,
                            landlord_availability,
                            name,
                            activated_at,
                            subscription,
                            cadastral_sheet,
                            cadastral_parcel,
                            cadastral_subordinate,
                            cadastral_category,
                            cadastral_income,
                            has_zappyrent_keys,
                            switch_bills_to_tenant_name,
                            exclusive,
                            runner_email,
                            allow_children,
                            allow_students,
                            max_number_of_guests,
                            matterport_link,
                            disabled_at,
                            disabled_reason)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""".format(tableName)

        cursor = connection.cursor()
        cursor.execute(sql_drop)
        print('Table {} has been dropped'.format(tableName))
        cursor.execute(sql_create_table)
        print('Table {} has been created'.format(tableName))

        for i in sql_data:
            cursor.execute(sql_insert_statement,i)

        connection.commit()
        print("Table {} successfully updated.".format(tableName))

    except mysql.connector.Error as error :
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





# define method to write list of data to MySQL table
def WriteToMySQLTable_booking_attachments(sql_data, tableName):
    try:
        connection = mysql.connector.connect(
        user = mc.user,
        password = mc.password,
        host = mc.host,
        database = mc.database
        )

        sql_drop = " DROP TABLE IF EXISTS {} ".format(tableName)

        sql_create_table = """CREATE TABLE {}(
                          id VARCHAR(100),
                          status VARCHAR(100),
                          type VARCHAR(100),
                          filename VARCHAR(100),
                          created_at DATE,
                          updated_at DATE,
                          booking_id VARCHAR(100),
                          retrieval_type VARCHAR(100),
                          description VARCHAR(100)
                          )""".format(tableName)

        sql_insert_statement = """INSERT INTO {}(
                          id,
                          status,
                          type,
                          filename,
                          created_at,
                          updated_at,
                          booking_id,
                          retrieval_type,
                          description)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""".format(tableName)

        cursor = connection.cursor()
        cursor.execute(sql_drop)
        print('Table {} has been dropped'.format(tableName))
        cursor.execute(sql_create_table)
        print('Table {} has been created'.format(tableName))

        for i in sql_data:
            cursor.execute(sql_insert_statement,i)

        connection.commit()
        print("Table {} successfully updated.".format(tableName))

    except mysql.connector.Error as error :
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


WriteToMySQLTable_bookings(data_bookings, 'bookings')





WriteToMySQLTable_booking_details(data_booking_details, 'booking_details')





WriteToMySQLTable_booking_properties(data_booking_properties, 'booking_properties')





WriteToMySQLTable_lead_properties(data_lead_properties, 'lead_properties')





WriteToMySQLTable_visits(data_visits, 'visits')





WriteToMySQLTable_client_account_managers(data_client_account_managers, 'client_account_managers')





WriteToMySQLTable_account_managers(data_account_managers, 'account_managers')





WriteToMySQLTable_properties_next_available_slots(data_properties_next_available_slots, 'properties_next_available_slots')





WriteToMySQLTable_listing_highlight(data_listing_highlight, 'listing_highlight')





WriteToMySQLTable_events(data_events, 'events')





WriteToMySQLTable_properties(data_properties, 'properties')





WriteToMySQLTable_booking_attachments(data_booking_attachments, 'booking_attachments')

def updatetime():
    try:
        connection = mysql.connector.connect(host=mc.host,
                                             database=mc.database,
                                             user=mc.user,
                                             password=mc.password)

        print("Connected to the database is successful")

        connurl = URL.create("mysql+mysqlconnector", username=mc.user, password=mc.password, host=mc.host, database=mc.database)
        engine = create_engine(connurl)

        df = pd.DataFrame({'tableName': ['bookings'], 'timestamp': [datetime.now()]})
        df.to_sql('update_time', con=engine, if_exists='append', index=False)
        print("Update time inserted successfully")

    except Exception as e:
        print("Error: ", e)
        print("Connection failed!")

updatetime()
