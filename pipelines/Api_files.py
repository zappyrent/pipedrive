#!/usr/bin/env python
# coding: utf-8


import requests
import pandas as pd
from pandas import json_normalize
from IPython.display import display
import gspread
import mysqlcredentials as mc
import mysql.connector
from oauth2client.service_account import ServiceAccountCredentials
import json
from datetime import datetime

# # Leads
# Replace 'YOUR_API_TOKEN' with your Pipedrive API token
api_token = '2be723d875edae489bdce028746e35bef6e3db80'

# # Base URL for the Pipedrive leads endpoint
# base_url = 'https://zappyrent.pipedrive.com/v1/leads'
#
# # Common request parameters (including your API token)
# common_params = {
#     'api_token': api_token,
#     'start': 0,       # The first page index
#     'limit': 500,     # The number of records per page
# }
#
# # Initialize a list to store all records
# all_leads = []
#
# while True:
#     # Make a GET request to fetch leads
#     response = requests.get(base_url, params=common_params)
#
#     # Check if the request was successful (status code 200)
#     if response.status_code == 200:
#         data = response.json()
#         leads = data['data']
#
#         # Add leads to the main list
#         all_leads.extend(leads)
#
#         # Check if there are more pages to be fetched
#         pagination_info = data['additional_data']['pagination']
#         if not pagination_info['more_items_in_collection']:
#             break
#
#         # Update the 'start' value for the next page
#         common_params['start'] += common_params['limit']
#     else:
#         print(f"Request error: {response.status_code} - {response.text}")
#         break
#
# # 'all_leads' now contains all leads in list format
# df_leads = json_normalize(all_leads)
# #df_leads.to_csv(r'C:\Users\PowerBI\Desktop\Power BI\pipedrive\dashboard_files\leads.txt',sep='\t',encoding='utf-8')
# print(f"Total leads found: {len(all_leads)}")
#
# df_leads.rename(columns={"71c903148dc11457f69aba83f90dc9d0ee7d57fb": "available_date",
#                          "e30d58a6ee435dd8c768a7c68abe54f72f53baa9": "mkt_acquisition_channel",
#                          "6bd95fa18eb839a1b909b550d7316dd8afb59f4f": "mkt_city_campaigns",
#                          "c7a0f20bd13245a3c3b459b5426ccd1c61782ae1": "mkt_acquisition_campaigns",
#                          "a15519619a00909f08af00216ea4c2ffb3963164": "cancelled_reason",
#                          "252a7fb8fdcdd7ff687c33d1bce5f964b874000b": "property_status",
#                          "e605cdbbb061dd00ea8252d1b410830cfac0de66": "property_type",
#                          "9c0928ae67f7b8698c4012ec4bbf334f23cf1a11": "uranus_dashboard",
#                          "974d7808fa9de9ee49266f33fabb309b109a1685": "property_id",
#                          "2c4832f2c2dca397c70c64bf0a478dbae635bff6": "city",
#                          "f4dc2b3f8b7965df490dc8b3c25fcbd090ec2b6d": "note",
#                          "e98d1f8ff53a34773103a9f2ce9b3ff6199537c5": "archived",
#                          "eaa56d256066650c5bb0a3b625f71dd6817eea28": "Lead created - Date",
#                          "8ce2908bbeeee7f6890ed042dc29f76c2840c5a9": "URL",
#                          "921f82c3f329d8eb8169864481113d0822472848": "Myphoner",
#                          "8eb0af5491820792745791a5c568447a11a6d1f7": "mkt_acquisition_source",
#                          "f1ba7aa9ea84dd08a93dd2b5847ff70ebfb517ed": "mkt_acquisition_content",
#                          "ebafffbe882cf7add8fc2f39f0f1545a8a644bcd": "Hinterland",
#                          "e7200b1990890527058a4df0f75976c7559f64fd": "mkt_acquisition_medium",
#                          "1b01467fb3947caaa5d55ffc13457786d1edded1": "mkt_acquisition_term",
#                          "6d531eec678f16a3bd63c414c4dc5a6310ca32db": "ref_code",
#                          "81cd4ac0649ffbe517006a8ab9a205f54d599d08": "referral_id",
#                          "c61aa7beae8120f1eeae79becaeb77321f7b88f5": "Address",
#                          "524a9902a56a12b832b0571105470c3fa8daf990": "Interessato",
#                          "eef838068043fa723f3ada7f366af5d138aba01a": "Aircall Tags",
#                          "8b4f04ce85d872c702e9ad430b709cead479d991": "Qualified Ready nel passato",
#                          "9869bde30bf6179eceeaaa12372ce9a918b1b41d": "codfisc",
#                          "158a26b7724e3ba1a3096cc0096344efa03aa24d": "Archiviation Date (Later)", "id": "lead.id"},
#                 inplace=True)
#
#
#
# # df_leads.head()
#
#
#
#
# df_leads['add_time'] = pd.to_datetime(df_leads['add_time'])
#
#
#
# df_leads['update_time'] = pd.to_datetime(df_leads['update_time'])
#
#
#
# df_leads['Lead created - Date'] = pd.to_datetime(df_leads['Lead created - Date'])
#
#
# # ### Lead Preproccessing
#
# df_leads['label_ids'] = 0
#
#
#
#
# df_leads = df_leads.fillna('NaN')
#
#
#
#
# df_leads['mkt_acquisition_term'][(df_leads['mkt_acquisition_channel'] == 'cpc') & (df_leads['cancelled_reason'] != '') & (df_leads['add_time'] >= '2024-04-01') ]
#
#
#
# #df_leads['mkt_acquisition_term'] = df_leads['mkt_acquisition_term'].str.replace(' ', '_')
#
#
#
# #df_leads[(df_leads['mkt_acquisition_channel']=='cpc') & (df_leads['add_time'] >= '2024-04-01')]
#
#
#
#
# df_leads = df_leads.values.tolist()
#
#
# # # Person
#
#
#
# ## Person
# # Replace 'YOUR_API_TOKEN' with your Pipedrive API token
# api_token = '2be723d875edae489bdce028746e35bef6e3db80'
#
# # Base URL for the Pipedrive leads endpoint
# base_url = ('https://zappyrent.pipedrive.com/v1/persons')
#
# # Common request parameters (including your API token)
# common_params = {
#     'api_token': api_token,
#     'start': 0,  # The first page index
#     'limit': 500,  # The number of records per page
# }
#
# # Initialize a list to store all records
# all_person = []
#
# while True:
#     # Make a GET request to fetch leads
#     response = requests.get(base_url, params=common_params)
#
#     # Check if the request was successful (status code 200)
#     if response.status_code == 200:
#         data = response.json()
#         deals = data['data']
#
#         # Add leads to the main list
#         all_person.extend(deals)
#
#         # Check if there are more pages to be fetched
#         pagination_info = data['additional_data']['pagination']
#         if not pagination_info['more_items_in_collection']:
#             break
#
#         # Update the 'start' value for the next page
#         common_params['start'] += common_params['limit']
#     else:
#         print(f"Request error: {response.status_code} - {response.text}")
#         break
#
#
#
# df_person = json_normalize(all_person)
# # df_person.to_csv(r'C:\Users\PowerBI\Desktop\Power BI\pipedrive\dashboard_files\person.txt', sep='\t', encoding='utf-8')
# print(f"Total person found: {len(all_person)}")
#
#
# # ### Person Preprocessing
#
# df_person['email'] = 0
# df_person['label_ids'] = 0
# df_person['im'] = 0
# df_person['org_id.label_ids'] = 0
#
#
#
#
# # Extract 'value' from 'phone' column
# df_person['phone'] = df_person['phone'].apply(lambda x: x[0]['value'] if x else None)
#
#
#
# df_person['add_time'] = pd.to_datetime(df_person['add_time'])
#
#
#
# df_person['update_time'] = pd.to_datetime(df_person['update_time'])
#
#
#
# df_person = df_person.fillna('NaN')
#
#
#
# df_person = df_person.values.tolist()


# # Deals



## DEALS
# Replace 'YOUR_API_TOKEN' with your Pipedrive API token
api_token = '2be723d875edae489bdce028746e35bef6e3db80'

# Base URL for the Pipedrive leads endpoint
base_url = ('https://zappyrent.pipedrive.com/v1/deals')

# Common request parameters (including your API token)
common_params = {
    'api_token': api_token,
    'start': 0,  # The first page index
    'limit': 500,  # The number of records per page
}

# Initialize a list to store all records
all_deals = []

while True:
    # Make a GET request to fetch leads
    response = requests.get(base_url, params=common_params)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        data = response.json()
        deals = data['data']

        # Add leads to the main list
        all_deals.extend(deals)

        # Check if there are more pages to be fetched
        pagination_info = data['additional_data']['pagination']
        if not pagination_info['more_items_in_collection']:
            break

        # Update the 'start' value for the next page
        common_params['start'] += common_params['limit']
    else:
        print(f"Request error: {response.status_code} - {response.text}")
        break




# 'all_leads' now contains all leads in list format
df_deals = json_normalize(all_deals)
# df_deals.to_csv(r'C:\Users\PowerBI\Desktop\Power BI\pipedrive\dashboard_files\deals.txt', sep='\t', encoding='utf-8')
print(f"Total deals found: {len(all_deals)}")




df_deals.rename(columns={"71c903148dc11457f69aba83f90dc9d0ee7d57fb": "available_date",
                         "e30d58a6ee435dd8c768a7c68abe54f72f53baa9": "mkt_acquisition_channel",
                         "6bd95fa18eb839a1b909b550d7316dd8afb59f4f": "mkt_city_campaigns",
                         "c7a0f20bd13245a3c3b459b5426ccd1c61782ae1": "mkt_acquisition_campaigns",
                         "a15519619a00909f08af00216ea4c2ffb3963164": "cancelled_reason",
                         "252a7fb8fdcdd7ff687c33d1bce5f964b874000b": "property_status",
                         "e605cdbbb061dd00ea8252d1b410830cfac0de66": "property_type",
                         "9c0928ae67f7b8698c4012ec4bbf334f23cf1a11": "uranus_dashboard",
                         "974d7808fa9de9ee49266f33fabb309b109a1685": "property_id",
                         "2c4832f2c2dca397c70c64bf0a478dbae635bff6": "city",
                         "f4dc2b3f8b7965df490dc8b3c25fcbd090ec2b6d": "note",
                         "e98d1f8ff53a34773103a9f2ce9b3ff6199537c5": "archived",
                         "eaa56d256066650c5bb0a3b625f71dd6817eea28": "Lead created - Date",
                         "8ce2908bbeeee7f6890ed042dc29f76c2840c5a9": "URL",
                         "921f82c3f329d8eb8169864481113d0822472848": "Myphoner",
                         "8eb0af5491820792745791a5c568447a11a6d1f7": "mkt_acquisition_source",
                         "f1ba7aa9ea84dd08a93dd2b5847ff70ebfb517ed": "mkt_acquisition_content",
                         "ebafffbe882cf7add8fc2f39f0f1545a8a644bcd": "Hinterland",
                         "e7200b1990890527058a4df0f75976c7559f64fd": "mkt_acquisition_medium",
                         "1b01467fb3947caaa5d55ffc13457786d1edded1": "mkt_acquisition_term",
                         "6d531eec678f16a3bd63c414c4dc5a6310ca32db": "ref_code",
                         "81cd4ac0649ffbe517006a8ab9a205f54d599d08": "referral_id",
                         "c61aa7beae8120f1eeae79becaeb77321f7b88f5": "Address",
                         "524a9902a56a12b832b0571105470c3fa8daf990": "Interessato",
                         "eef838068043fa723f3ada7f366af5d138aba01a": "Aircall Tags",
                         "8b4f04ce85d872c702e9ad430b709cead479d991": "Qualified Ready nel passato",
                         "9869bde30bf6179eceeaaa12372ce9a918b1b41d": "codfisc",
                         "158a26b7724e3ba1a3096cc0096344efa03aa24d": "Archiviation Date (Later)", "id": "deal.id"},
                inplace=True)


# ### Deals Preprocessing



df_deals['person_id.email'] = 0
df_deals['person_id.phone'] = 0
df_deals['org_id.label_ids'] = 0



df_deals['add_time'] = pd.to_datetime(df_deals['add_time'])




df_deals['update_time'] = pd.to_datetime(df_deals['update_time'])




df_deals['Lead created - Date'] = pd.to_datetime(df_deals['Lead created - Date'])



df_deals['won_time'] = pd.to_datetime(df_deals['won_time'])



df_deals = df_deals.fillna('NaN')



df_deals = df_deals.values.tolist()




# define method to write list of data to MySQL table
def WriteToMySQLTable(sql_data, tableName):
    try:
        connection = mysql.connector.connect(
            user=mc.user,
            password=mc.password,
            host=mc.host,
            database=mc.database)
        sql_drop = " DROP TABLE IF EXISTS {} ".format(tableName)
        sql_create_table = """CREATE TABLE {}( 
            deal_id VARCHAR(100),
            org_id VARCHAR(100),
            stage_id VARCHAR(100),
            title VARCHAR(100),
            value VARCHAR(100),
            acv VARCHAR(100),
            mrr VARCHAR(100),
            arr VARCHAR(100),
            currency VARCHAR(100),
            add_time DATE,
            update_time DATE,
            stage_change_time VARCHAR(100),
            active VARCHAR(100),
            deleted VARCHAR(100),
            status VARCHAR(100),
            probability VARCHAR(100),
            next_activity_date VARCHAR(100),
            next_activity_time VARCHAR(100),
            next_activity_id VARCHAR(100),
            last_activity_id VARCHAR(100),
            last_activity_date VARCHAR(100),
            lost_reason VARCHAR(100),
            visible_to VARCHAR(100),
            close_time VARCHAR(100),
            pipeline_id VARCHAR(100),
            won_time VARCHAR(100),
            first_won_time VARCHAR(100),
            lost_time VARCHAR(100),
            products_count VARCHAR(100),
            files_count VARCHAR(100),
            notes_count VARCHAR(100),
            followers_count VARCHAR(100),
            email_messages_count VARCHAR(100),
            activities_count VARCHAR(100),
            done_activities_count VARCHAR(100),
            undone_activities_count VARCHAR(100),
            participants_count VARCHAR(100),
            expected_close_date VARCHAR(100),
            last_incoming_mail_time VARCHAR(100),
            last_outgoing_mail_time VARCHAR(100),
            label VARCHAR(100),
            local_won_date VARCHAR(100),
            local_lost_date VARCHAR(100),
            local_close_date VARCHAR(100),
            origin VARCHAR(100),
            origin_id VARCHAR(100),
            channel VARCHAR(100),
            channel_id VARCHAR(100),
            stage_order_nr VARCHAR(100),
            person_name VARCHAR(100),
            org_name VARCHAR(100),
            next_activity_subject VARCHAR(100),
            next_activity_type VARCHAR(100),
            next_activity_duration VARCHAR(100),
            next_activity_note VARCHAR(100),
            formatted_value VARCHAR(100),
            weighted_value VARCHAR(100),
            formatted_weighted_value VARCHAR(100),
            weighted_value_currency VARCHAR(100),
            rotten_time VARCHAR(100),
            acv_currency VARCHAR(100),
            mrr_currency VARCHAR(100),
            arr_currency VARCHAR(100),
            owner_name VARCHAR(100),
            cc_email VARCHAR(100),
            available_date VARCHAR(100),
            mkt_acquisition_channel VARCHAR(100),
            mkt_city_campaigns VARCHAR(100),
            mkt_acquisition_campaigns VARCHAR(100),
            cancelled_reason VARCHAR(100),
            property_status VARCHAR(100),
            property_type VARCHAR(100),
            uranus_dashboard VARCHAR(100),
            property_id VARCHAR(100),
            city VARCHAR(100),
            note VARCHAR(100),
            archived VARCHAR(100),
            Lead_created_Date DATE,
            URL VARCHAR(100),
            Myphoner VARCHAR(100),
            mkt_acquisition_source VARCHAR(100),
            mkt_acquisition_content VARCHAR(100),
            Hinterland VARCHAR(100),
            mkt_acquisition_medium VARCHAR(100),
            mkt_acquisition_term VARCHAR(100),
            ref_code VARCHAR(100),
            referral_id VARCHAR(100),
            Address VARCHAR(100),
            Interessato VARCHAR(100),
            Aircall_Tags VARCHAR(100),
            Qualified_Ready_nel_passato VARCHAR(100),
            codfisc VARCHAR(100),
            Archiviation_Date_Later VARCHAR(100),
            7dad498e50b4e0bc327b113997ed188b832419ab VARCHAR(100),
            org_hidden VARCHAR(100),
            person_hidden VARCHAR(100),
            creator_user_id_id VARCHAR(100),
            creator_user_id_name VARCHAR(100),
            creator_user_id_email VARCHAR(100),
            creator_user_id_has_pic VARCHAR(100),
            creator_user_id_pic_hash VARCHAR(100),
            creator_user_id_active_flag VARCHAR(100),
            creator_user_id_value VARCHAR(100),
            user_id_id VARCHAR(100),
            user_id_name VARCHAR(100),
            user_id_email VARCHAR(100),
            user_id_has_pic VARCHAR(100),
            user_id_pic_hash VARCHAR(100),
            user_id_active_flag VARCHAR(100),
            user_id_value VARCHAR(100),
            person_id_active_flag VARCHAR(100),
            person_id_name VARCHAR(100),
            person_id_email VARCHAR(100),
            person_id_phone VARCHAR(100),
            person_id_owner_id VARCHAR(100),
            person_id_value INTEGER,
            person_id VARCHAR(100),
            org_id_name VARCHAR(100),
            org_id_people_count VARCHAR(100),
            org_id_owner_id INTEGER,
            org_id_address VARCHAR(100),
            org_id_active_flag VARCHAR(100),
            org_id_cc_email VARCHAR(100),
            org_id_label_ids VARCHAR(100),
            org_id_owner_name VARCHAR(100),
            org_id_value VARCHAR(100)
            )""".format(tableName)

        sql_insert_statement = """INSERT INTO {}( 
            deal_id,
            org_id,
            stage_id,
            title,
            value,
            acv,
            mrr,
            arr,
            currency,
            add_time,
            update_time,
            stage_change_time,
            active,
            deleted,
            status,
            probability,
            next_activity_date,
            next_activity_time,
            next_activity_id,
            last_activity_id,
            last_activity_date,
            lost_reason,
            visible_to,
            close_time,
            pipeline_id,
            won_time,
            first_won_time,
            lost_time,
            products_count,
            files_count,
            notes_count,
            followers_count,
            email_messages_count,
            activities_count,
            done_activities_count,
            undone_activities_count,
            participants_count,
            expected_close_date,
            last_incoming_mail_time,
            last_outgoing_mail_time,
            label,
            local_won_date,
            local_lost_date,
            local_close_date,
            origin,
            origin_id,
            channel,
            channel_id,
            stage_order_nr,
            person_name,
            org_name,
            next_activity_subject,
            next_activity_type,
            next_activity_duration,
            next_activity_note,
            formatted_value,
            weighted_value,
            formatted_weighted_value,
            weighted_value_currency,
            rotten_time,
            acv_currency,
            mrr_currency,
            arr_currency,
            owner_name,
            cc_email,
            available_date,
            mkt_acquisition_channel,
            mkt_city_campaigns,
            mkt_acquisition_campaigns,
            cancelled_reason,
            property_status,
            property_type,
            uranus_dashboard,
            property_id,
            city,
            note,
            archived,
            Lead_created_Date,
            URL,
            Myphoner,
            mkt_acquisition_source,
            mkt_acquisition_content,
            Hinterland,
            mkt_acquisition_medium,
            mkt_acquisition_term,
            ref_code,
            referral_id,
            Address,
            Interessato,
            Aircall_Tags,
            Qualified_Ready_nel_passato,
            codfisc,
            Archiviation_Date_Later,
            7dad498e50b4e0bc327b113997ed188b832419ab,
            org_hidden,
            person_hidden,
            creator_user_id_id,
            creator_user_id_name,
            creator_user_id_email,
            creator_user_id_has_pic,
            creator_user_id_pic_hash,
            creator_user_id_active_flag,
            creator_user_id_value,
            user_id_id,
            user_id_name,
            user_id_email,
            user_id_has_pic,
            user_id_pic_hash,
            user_id_active_flag,
            user_id_value,
            person_id_active_flag,
            person_id_name,
            person_id_email,
            person_id_phone,
            person_id_owner_id,
            person_id_value,
            person_id,
            org_id_name,
            org_id_people_count,
            org_id_owner_id,
            org_id_address,
            org_id_active_flag,
            org_id_cc_email,
            org_id_label_ids,
            org_id_owner_name,
            org_id_value)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""".format(tableName)

        cursor = connection.cursor()
        cursor.execute(sql_drop)
        print('Table {} has been dropped'.format(tableName))
        cursor.execute(sql_create_table)
        print('Table {} has been created'.format(tableName))

        for i in sql_data:
            cursor.execute(sql_insert_statement, i)

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




# define method to write list of data to MySQL table
def WriteToMySQLTable_person(sql_data, tableName):
    try:
        connection = mysql.connector.connect(
            user=mc.user,
            password=mc.password,
            host=mc.host,
            database=mc.database)
        sql_drop = " DROP TABLE IF EXISTS {} ".format(tableName)
        sql_create_table = """CREATE TABLE {}( 
                                    id VARCHAR(100),
                                    company_id VARCHAR(100),
                                    org_id VARCHAR(100),
                                    name VARCHAR(100),
                                    first_name VARCHAR(100),
                                    last_name VARCHAR(100),
                                    open_deals_count VARCHAR(100),
                                    related_open_deals_count VARCHAR(100),
                                    closed_deals_count VARCHAR(100),
                                    related_closed_deals_count VARCHAR(100),
                                    participant_open_deals_count VARCHAR(100),
                                    participant_closed_deals_count VARCHAR(100),
                                    email_messages_count VARCHAR(100),
                                    activities_count VARCHAR(100),
                                    done_activities_count VARCHAR(100),
                                    undone_activities_count VARCHAR(100),
                                    files_count VARCHAR(100),
                                    notes_count VARCHAR(100),
                                    followers_count VARCHAR(100),
                                    won_deals_count VARCHAR(100),
                                    related_won_deals_count VARCHAR(100),
                                    lost_deals_count VARCHAR(100),
                                    related_lost_deals_count VARCHAR(100),
                                    active_flag VARCHAR(100),
                                    phone VARCHAR(100),
                                    email VARCHAR(100),
                                    first_char VARCHAR(100),
                                    update_time DATE,
                                    delete_time VARCHAR(100),
                                    add_time DATE,
                                    visible_to VARCHAR(100),
                                    picture_id VARCHAR(100),
                                    next_activity_date DATE,
                                    next_activity_time VARCHAR(100),
                                    next_activity_id VARCHAR(100),
                                    last_activity_id VARCHAR(100),
                                    last_activity_date VARCHAR(100),
                                    last_incoming_mail_time VARCHAR(100),
                                    last_outgoing_mail_time VARCHAR(100),
                                    label VARCHAR(100),
                                    label_ids VARCHAR(100),
                                    im VARCHAR(100),
                                    postal_address VARCHAR(100),
                                    postal_address_lat VARCHAR(100),
                                    postal_address_long VARCHAR(100),
                                    postal_address_subpremise VARCHAR(100),
                                    postal_address_street_number VARCHAR(100),
                                    postal_address_route VARCHAR(100),
                                    postal_address_sublocality VARCHAR(100),
                                    postal_address_locality VARCHAR(100),
                                    postal_address_admin_area_level_1 VARCHAR(100),
                                    postal_address_admin_area_level_2 VARCHAR(100),
                                    postal_address_country VARCHAR(100),
                                    postal_address_postal_code VARCHAR(100),
                                    postal_address_formatted_address VARCHAR(100),
                                    notes VARCHAR(100),
                                    birthday VARCHAR(100),
                                    job_title VARCHAR(100),
                                    org_name VARCHAR(100),
                                    owner_name VARCHAR(100),
                                    primary_email VARCHAR(100),
                                    e590fd69916defff8f1245ccac5678d3336ba8dd VARCHAR(100),
                                    b20755ef11d478af2c8f2ddfddc84d61eca934fc VARCHAR(100),
                                    b7d9d2682c5cfc9d0d78d8dc13b28ff254022847 VARCHAR(100),
                                    95cb3ea1f52348d739b34d6b4d61208c5a24d160 VARCHAR(100),
                                    fe5989bb199ee84a6842ba7120507599616a8200 VARCHAR(100),
                                    cc_email VARCHAR(100),
                                    owner_id_id VARCHAR(100),
                                    owner_id_name VARCHAR(100),
                                    owner_id_email VARCHAR(100),
                                    owner_id_has_pic VARCHAR(100),
                                    owner_id_pic_hash VARCHAR(100),
                                    owner_id_active_flag VARCHAR(100),
                                    owner_id_value VARCHAR(100),
                                    org_id_name VARCHAR(100),
                                    org_id_people_count VARCHAR(100),
                                    org_id_owner_id VARCHAR(100),
                                    org_id_address VARCHAR(100),
                                    org_id_active_flag VARCHAR(100),
                                    org_id_cc_email VARCHAR(100),
                                    org_id_label_ids VARCHAR(100),
                                    org_id_owner_name VARCHAR(100),
                                    org_id_value VARCHAR(100),
                                    picture_id_item_type VARCHAR(100),
                                    picture_id_item_id VARCHAR(100),
                                    picture_id_active_flag VARCHAR(100),
                                    picture_id_add_time VARCHAR(100),
                                    picture_id_update_time VARCHAR(100),
                                    picture_id_added_by_user_id VARCHAR(100),
                                    picture_id_file_size VARCHAR(100),
                                    picture_id_pictures_128 VARCHAR(100),
                                    picture_id_pictures_512 VARCHAR(100),
                                    picture_id_value VARCHAR(100)
            )""".format(tableName)

        sql_insert_statement = """INSERT INTO {}( 
                                    id,
                                    company_id,
                                    org_id,
                                    name,
                                    first_name,
                                    last_name,
                                    open_deals_count,
                                    related_open_deals_count,
                                    closed_deals_count,
                                    related_closed_deals_count,
                                    participant_open_deals_count,
                                    participant_closed_deals_count,
                                    email_messages_count,
                                    activities_count,
                                    done_activities_count,
                                    undone_activities_count,
                                    files_count,
                                    notes_count,
                                    followers_count,
                                    won_deals_count,
                                    related_won_deals_count,
                                    lost_deals_count,
                                    related_lost_deals_count,
                                    active_flag,
                                    phone,
                                    email,
                                    first_char,
                                    update_time,
                                    delete_time,
                                    add_time,
                                    visible_to,
                                    picture_id,
                                    next_activity_date,
                                    next_activity_time,
                                    next_activity_id,
                                    last_activity_id,
                                    last_activity_date,
                                    last_incoming_mail_time,
                                    last_outgoing_mail_time,
                                    label,
                                    label_ids,
                                    im,
                                    postal_address,
                                    postal_address_lat,
                                    postal_address_long,
                                    postal_address_subpremise,
                                    postal_address_street_number,
                                    postal_address_route,
                                    postal_address_sublocality,
                                    postal_address_locality,
                                    postal_address_admin_area_level_1,
                                    postal_address_admin_area_level_2,
                                    postal_address_country,
                                    postal_address_postal_code,
                                    postal_address_formatted_address,
                                    notes,
                                    birthday,
                                    job_title,
                                    org_name,
                                    owner_name,
                                    primary_email,
                                    e590fd69916defff8f1245ccac5678d3336ba8dd,
                                    b20755ef11d478af2c8f2ddfddc84d61eca934fc,
                                    b7d9d2682c5cfc9d0d78d8dc13b28ff254022847,
                                    95cb3ea1f52348d739b34d6b4d61208c5a24d160,
                                    fe5989bb199ee84a6842ba7120507599616a8200,
                                    cc_email,
                                    owner_id_id,
                                    owner_id_name,
                                    owner_id_email,
                                    owner_id_has_pic,
                                    owner_id_pic_hash,
                                    owner_id_active_flag,
                                    owner_id_value,
                                    org_id_name,
                                    org_id_people_count,
                                    org_id_owner_id,
                                    org_id_address,
                                    org_id_active_flag,
                                    org_id_cc_email,
                                    org_id_label_ids,
                                    org_id_owner_name,
                                    org_id_value,
                                    picture_id_item_type,
                                    picture_id_item_id,
                                    picture_id_active_flag,
                                    picture_id_add_time,
                                    picture_id_update_time,
                                    picture_id_added_by_user_id,
                                    picture_id_file_size,
                                    picture_id_pictures_128,
                                    picture_id_pictures_512,
                                    picture_id_value
                                    )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""".format(tableName)

        cursor = connection.cursor()
        cursor.execute(sql_drop)
        print('Table {} has been dropped'.format(tableName))
        cursor.execute(sql_create_table)
        print('Table {} has been created'.format(tableName))

        for i in sql_data:
            cursor.execute(sql_insert_statement, i)

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




# define method to write list of data to MySQL table
def WriteToMySQLTable_leads(sql_data, tableName):
    try:
        connection = mysql.connector.connect(
            user=mc.user,
            password=mc.password,
            host=mc.host,
            database=mc.database)
        sql_drop = " DROP TABLE IF EXISTS {} ".format(tableName)
        sql_create_table = """CREATE TABLE {}( 
                                    lead_id VARCHAR(100),
                                    title VARCHAR(100),
                                    owner_id VARCHAR(100),
                                    creator_id VARCHAR(100),
                                    label_ids VARCHAR(100),
                                    value VARCHAR(100),
                                    expected_close_date VARCHAR(100),
                                    person_id INTEGER,
                                    organization_id VARCHAR(100),
                                    is_archived VARCHAR(100),
                                    source_name VARCHAR(100),
                                    origin VARCHAR(100),
                                    origin_id VARCHAR(100),
                                    channel VARCHAR(100),
                                    channel_id VARCHAR(100),
                                    was_seen VARCHAR(100),
                                    next_activity_id VARCHAR(100),
                                    add_time DATE,
                                    update_time DATE,
                                    cancelled_reason VARCHAR(100),
                                    visible_to VARCHAR(100),
                                    cc_email VARCHAR(100),
                                    mkt_acquisition_channel VARCHAR(100),
                                    mkt_city_campaigns VARCHAR(100),
                                    mkt_acquisition_campaign VARCHAR(100),
                                    property_status VARCHAR(100),
                                    property_type VARCHAR(100),
                                    property_id VARCHAR(100),
                                    city VARCHAR(100),
                                    mkt_acquisition_source VARCHAR(100),
                                    available_date VARCHAR(100),
                                    archived VARCHAR(100),
                                    Lead_created_Date DATE,
                                    codfisc VARCHAR(100),
                                    7dad498e50b4e0bc327b113997ed188b832419ab VARCHAR(100),
                                    uranus_dashboard VARCHAR(100),
                                    note VARCHAR(100),
                                    value_amount VARCHAR(100),
                                    value_currency VARCHAR(100),
                                    URL VARCHAR(100),
                                    Myphoner VARCHAR(100),
                                    Archiviation_Date_Later VARCHAR(100),
                                    ref_code VARCHAR(100),
                                    referral_id VARCHAR(100),
                                    mkt_acquisition_content VARCHAR(100),
                                    mkt_acquisition_term TEXT(200),
                                    mkt_acquisition_medium VARCHAR(100),
                                    Address VARCHAR(100),
                                    Interessato VARCHAR(100),
                                    Aircall_Tags VARCHAR(100),
                                    Qualified_Ready_nel_passato VARCHAR(100)                                    
            )""".format(tableName)

        sql_insert_statement = """INSERT INTO {}( 
                                    lead_id,
                                    title,
                                    owner_id,
                                    creator_id,
                                    label_ids,
                                    value,
                                    expected_close_date,
                                    person_id,
                                    organization_id,
                                    is_archived,
                                    source_name,
                                    origin,
                                    origin_id,
                                    channel,
                                    channel_id,
                                    was_seen,
                                    next_activity_id,
                                    add_time,
                                    update_time,
                                    cancelled_reason,
                                    visible_to,
                                    cc_email,
                                    mkt_acquisition_channel,
                                    mkt_city_campaigns,
                                    mkt_acquisition_campaign,
                                    property_status,
                                    property_type,
                                    property_id,
                                    city,
                                    mkt_acquisition_source,
                                    available_date,
                                    archived,
                                    Lead_created_Date,
                                    codfisc,
                                    7dad498e50b4e0bc327b113997ed188b832419ab,
                                    uranus_dashboard,
                                    note,
                                    value_amount,
                                    value_currency,
                                    URL,
                                    Myphoner,
                                    Archiviation_Date_Later,
                                    ref_code,
                                    referral_id,
                                    mkt_acquisition_content,
                                    mkt_acquisition_term,
                                    mkt_acquisition_medium,
                                    Address,
                                    Interessato,
                                    Aircall_Tags,
                                    Qualified_Ready_nel_passato
                                    )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""".format(tableName)

        cursor = connection.cursor()
        cursor.execute(sql_drop)
        print('Table {} has been dropped'.format(tableName))
        cursor.execute(sql_create_table)
        print('Table {} has been created'.format(tableName))
        j=1
        for i in sql_data:
            cursor.execute(sql_insert_statement, i)
            print(j)
            j=j+1

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



WriteToMySQLTable(df_deals,'deals_test')

# WriteToMySQLTable_leads(df_leads,'leads_test')
#
# WriteToMySQLTable_person(df_person,'person_test')






