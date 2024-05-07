## Create a DAG to run the ETL process for the leads data consuming the data from the leads REST API
## The API connection ID is pipe_leads returns a JSON on an object with the following structure: 'data'

from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from mysql.connector import Error
from dateutil import parser
import mysql.connector
import calendar
import requests
import base64
import json
import time
import re

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

script_create_table_numbers = """
    CREATE TABLE IF NOT EXISTS aircalls_numbers_dag (
                id BIGINT PRIMARY KEY,
                direct_link VARCHAR(255),
                name VARCHAR(255),
                digits VARCHAR(50),
                country VARCHAR(50),
                time_zone VARCHAR(50),
                open BOOLEAN,
                availability_status VARCHAR(50),
                is_ivr BOOLEAN,
                live_recording_activated BOOLEAN,
                priority INT,
                created_at DATETIME
            );
    """

script_insert_or_update_numbers = """
        INSERT INTO aircalls_numbers_dag (id, direct_link, name, digits, country, time_zone, open, availability_status, is_ivr, live_recording_activated, priority, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE 
        direct_link = VALUES(direct_link), 
        name = VALUES(name), 
        digits = VALUES(digits), 
        country = VALUES(country), 
        time_zone = VALUES(time_zone), 
        open = VALUES(open), 
        availability_status = VALUES(availability_status), 
        is_ivr = VALUES(is_ivr), 
        live_recording_activated = VALUES(live_recording_activated), 
        priority = VALUES(priority), 
        created_at = VALUES(created_at);
        """

script_create_table_users = """
    CREATE TABLE IF NOT EXISTS aircalls_users_dag (
                id INT PRIMARY KEY,
                direct_link VARCHAR(255),
                name VARCHAR(255),
                email VARCHAR(255),
                available BOOLEAN,
                availability_status VARCHAR(50),
                created_at DATETIME,
                time_zone VARCHAR(50),
                language VARCHAR(50),
                wrap_up_time INT
            );
"""
# Create insert/update script for users
script_insert_or_update_users = """
    INSERT INTO aircalls_users_dag (id, direct_link, name, email, available, availability_status, created_at, time_zone, language, wrap_up_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE 
    direct_link = VALUES(direct_link), 
    name = VALUES(name), 
    email = VALUES(email), 
    available = VALUES(available), 
    availability_status = VALUES(availability_status), 
    created_at = VALUES(created_at), 
    time_zone = VALUES(time_zone), 
    language = VALUES(language), 
    wrap_up_time = VALUES(wrap_up_time);
    """

script_create_table_calls = """
    CREATE TABLE IF NOT EXISTS aircalls_calls_dag (
                id BIGINT PRIMARY KEY,
                direct_link VARCHAR(255),
                direction VARCHAR(50),
                status VARCHAR(50),
                missed_call_reason VARCHAR(255),
                started_at DATETIME,
                answered_at DATETIME,
                ended_at DATETIME,
                duration INT,
                voicemail TEXT,
                asset TEXT,
                raw_digits VARCHAR(50),
                user_id BIGINT, 
                archived BOOLEAN,
                assigned_to TEXT, 
                transferred_by TEXT, 
                transferred_to TEXT, 
                number_id BIGINT, 
                cost DECIMAL(10,2),
                country_code_a2 VARCHAR(2),
                pricing_type VARCHAR(50)
            );
"""

# Create insert/update script for calls
script_insert_or_update_calls = """
    INSERT INTO aircalls_calls_dag (id, direct_link, direction, status, missed_call_reason, started_at, answered_at, ended_at, duration, voicemail, asset, raw_digits, user_id, archived, assigned_to, transferred_by, transferred_to, number_id, cost, country_code_a2, pricing_type) 
    VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s) ON DUPLICATE KEY UPDATE 
                direct_link = VALUES(direct_link),
                direction = VALUES(direction),
                status = VALUES(status),
                missed_call_reason = VALUES(missed_call_reason),
                started_at = VALUES(started_at),
                answered_at = VALUES(answered_at),
                ended_at = VALUES(ended_at),
                duration = VALUES(duration),
                voicemail = VALUES(voicemail),
                asset = VALUES(asset),
                raw_digits = VALUES(raw_digits),
                user_id = VALUES(user_id),
                archived = VALUES(archived),
                assigned_to = VALUES(assigned_to),
                transferred_by = VALUES(transferred_by),
                transferred_to = VALUES(transferred_to),
                number_id = VALUES(number_id),
                cost = VALUES(cost),
                country_code_a2 = VALUES(country_code_a2),
                pricing_type = VALUES(pricing_type);
"""

# Retrieve database configuration from Airflow Variable
aircalls_cron_schedule = Variable.get("aircalls_cron_schedule", default_var="*/2 * * * *")
api_test = Variable.get("api_aircalls_test",default_var="False").lower() == "true"
api_limit = Variable.get("pipedrive_api_limit", default_var="500")
api_start = Variable.get("pipedrive_api_start", default_var="0")
api_token = Variable.get("pipedrive_api_token")
api_aircalls_url = Variable.get("url_aircalls")
api_user = Variable.get("aircalls_api_user")
api_pass = Variable.get("aircalls_api_pass")
batch_size = Variable.get("aircalls_batch_size")
mysql_conn_id = 'my_sql'

create_tables_sql = [script_create_table_numbers, script_create_table_users, script_create_table_calls]


def insert_update_number(mysql_hook, number_data):
    print("Inserting/Updating number:", number_data)
    # Extract data from the number_data dictionary
    number_id = number_data.get("id")
    direct_link = number_data.get("direct_link")
    name = number_data.get("name")
    digits = number_data.get("digits")
    country = number_data.get("country")
    time_zone = number_data.get("time_zone")
    open = number_data.get("open")
    availability_status = number_data.get("availability_status")
    is_ivr = number_data.get("is_ivr")
    live_recording_activated = number_data.get("live_recording_activated")
    priority = number_data.get("priority")
    created_at = number_data.get("created_at")
    created_at_datetime = None
    if created_at is not None:
        # Parse the ISO formatted datetime string to a datetime object
        created_at_datetime = parser.parse(created_at)

    values = ( number_id, direct_link, name, digits, country, time_zone, open, availability_status, is_ivr, live_recording_activated, priority, created_at_datetime)
    print(f"User Values: {values}")

    try:
        mysql_hook.run(script_insert_or_update_numbers, parameters=values)
        print("User insert/update successful.")
    except Exception as e:
        print(f"Failed to insert/update number {user_id}: {e}")

    return number_id

def format_raw_digits(raw_digits):
    if raw_digits is None:
        return None
    # Remove all spaces from the string
    return raw_digits.replace(" ", "")

def insert_update_user(mysql_hook, user_data):
    print("Inserting/Updating user:", user_data)
    # Extract data from the user_data dictionary
    user_id = user_data.get("id")
    direct_link = user_data.get("direct_link")
    name = user_data.get("name")
    email = user_data.get("email")
    available = user_data.get("available")
    availability_status = user_data.get("availability_status")
    created_at = user_data.get("created_at")
    created_at_datetime = None
    if created_at is not None:
        # Parse the ISO formatted datetime string to a datetime object
        created_at_datetime = parser.parse(created_at)
    time_zone = user_data.get("time_zone")
    language = user_data.get("language")
    wrap_up_time = user_data.get("wrap_up_time")

    values = (user_id, direct_link, name, email, available, availability_status, created_at_datetime, time_zone, language, wrap_up_time)
    print(f"User Values: {values}")

    try:
        mysql_hook.run(script_insert_or_update_users, parameters=values)
        print("User insert/update successful.")
    except Exception as e:
        print(f"Failed to insert/update user {user_id}: {e}")

    return user_id

def insert_update_call(mysql_hook, call_data):
    print("Inserting/Updating call:", call_data)
    # Extract data from the call_data dictionary
    call_id = call_data.get("id")
    direct_link = call_data.get("direct_link")
    direction = call_data.get("direction")
    status = call_data.get("status")
    missed_call_reason = call_data.get("missed_call_reason")
    started_at = call_data.get("started_at")
    started_at_datetime = None
    if started_at is not None:
        # Parse the ISO formatted datetime string to a datetime object
        started_at_datetime = datetime.fromtimestamp(started_at)
    answered_at = call_data.get("answered_at")
    answered_at_datetime = None
    if answered_at is not None:
        # Parse the ISO formatted datetime string to a datetime object
        answered_at_datetime = datetime.fromtimestamp(answered_at)
    ended_at = call_data.get("ended_at")
    ended_at_datetime = None
    if ended_at is not None:
        # Parse the ISO formatted datetime string to a datetime object
        ended_at_datetime = datetime.fromtimestamp(ended_at)
    duration = call_data.get("duration")
    voicemail = call_data.get("voicemail")
    asset = call_data.get("asset")
    raw_digits = format_raw_digits(call_data.get("raw_digits"))
    # Validate and insert/update the user
    if call_data.get("user") is not None:
        user_id = insert_update_user(mysql_hook, call_data.get("user"))
    else:
        user_id = None
    archived = call_data.get("archived")
    assigned_to = call_data.get("assigned_to")
    transferred_by = call_data.get("transferred_by")
    transferred_to = call_data.get("transferred_to")
    # Validate and insert/update the number
    if call_data.get("number") is not None:
        number_id = insert_update_number(mysql_hook, call_data.get("number"))
    else:
        number_id = None

    cost = call_data.get("cost")
    country_code_a2 = call_data.get("country_code_a2")
    pricing_type = call_data.get("pricing_type")

    values = (
        call_id, direct_link, direction, status, missed_call_reason, started_at_datetime, answered_at_datetime,
        ended_at_datetime, duration, voicemail, asset, raw_digits, user_id, archived, assigned_to, transferred_by,
        transferred_to, number_id, cost, country_code_a2, pricing_type
    )
    print(f"Call Values: {values}")

    try:
        mysql_hook.run(script_insert_or_update_calls, parameters=values)
        print("Call insert/update successful.")
    except Exception as e:
        print(f"Failed to insert/update call {call_id}: {e}")


def get_primary_contact(contacts):
    print("Extracting primary contact from:", contacts)
    for contact in contacts:
        if contact.get("primary", False):
            primary_contact = contact.get("value")
            print(f"Primary contact found: {primary_contact}")
            return primary_contact
    print("No primary contact found.")
    return None


def format_phone_number(phone_number):
    print(f"Formatting phone number: {phone_number}")
    if not phone_number:
        print("No phone number provided.")
        return None  # Or an empty string or any default value you prefer

    cleaned_number = re.sub(r"(?!^\+)\D", "", phone_number)
    print(f"Cleaned number: {cleaned_number}")

    if cleaned_number.startswith("+"):
        return cleaned_number
    elif cleaned_number.startswith("800"):
        return cleaned_number
    elif cleaned_number.startswith("39"):
        return "+" + cleaned_number
    elif not cleaned_number.startswith("39") and len(cleaned_number) <= 10:
        return "+39" + cleaned_number
    else:
        return "+" + cleaned_number

def parse_date_time(data, field_name):
    raw_date_time = data.get(field_name)
    print(f"Parsing date/time for {field_name}: {raw_date_time}")
    if raw_date_time:
        try:
            parsed_date_time = datetime.strptime(raw_date_time, "%Y-%m-%d %H:%M:%S")
            print(f"Parsed date/time: {parsed_date_time}")
            return parsed_date_time
        except ValueError as e:
            print(f"Error parsing date/time for {field_name}: {e}")
    return None

def parse_date(data, field_name):
    raw_date = data.get(field_name)
    print(f"Attempting to parse date for '{field_name}': {raw_date}")
    if raw_date:
        try:
            parsed_date = datetime.strptime(raw_date, "%Y-%m-%d").date()
            print(f"Parsed date for '{field_name}': {parsed_date}")
            return parsed_date
        except ValueError as e:
            print(f"Error parsing date for '{field_name}': {raw_date}, Error: {e}")
    else:
        print(f"No date found for '{field_name}'.")
    return None

def parse_double(data, field_name):
    value = data.get(field_name)
    print(f"Attempting to parse double for '{field_name}': {value}")
    try:
        if value is None:
            print(f"No value provided for '{field_name}', returning None.")
            return None
        parsed_double = float(value)
        print(f"Parsed double for '{field_name}': {parsed_double}")
        return parsed_double
    except (TypeError, ValueError) as e:
        print(f"Error parsing double for '{field_name}': {value}, Error: {e}")
        return None

def datetime_to_unix_timestamp(date):
    return int(time.mktime(date.timetuple()))

def make_api_call_v2(url, user, passw):
    max_retries = 3
    retry_delay = 2  # seconds
    headers = {
        "Authorization": "Basic " + base64.b64encode(f"{user}:{passw}".encode()).decode()
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"API call failed, attempt {attempt + 1}. Retrying in {retry_delay} seconds.")
            if attempt + 1 == max_retries:
                raise
            time.sleep(retry_delay)

def process_response_and_insert_calls(response, mysql_conn_id):
    # Assuming `response` is a JSON object similar to what's returned from an API call
    calls = response.get('calls', [])
    total_processed = 0

    # Initialize MySqlHook
    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)

    for call in calls:
        # Here, you would have the logic to insert or update the call data
        # For simplicity, we assume a function `insert_or_update_call_data` exists
        insert_update_call(mysql_hook, call)
        total_processed += 1

    print(f"Processed {total_processed} records")

    # Fetching the next page link from the response for pagination
    meta = response.get('meta', {})
    next_page_link = meta.get('next_page_link', None)
    print(f"Page processed, next page link: {next_page_link}")
    print(f"Current page: {meta.get('current_page')}")

    return next_page_link, bool(next_page_link)

# First DAG - Original
def fetch_all_calls():
    start_date_str = Variable.get("aircalls_start_date", default_var="2024-03-10")
    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    # Define your SQL SELECT command
    sql_query = "SELECT started_at FROM pipedrive.aircalls_calls_dag ORDER BY started_at DESC LIMIT 1;"
    # Execute the SQL command and fetch the first record
    last_date_result = mysql_hook.get_first(sql_query)
    last_date = last_date_result[0] if last_date_result else None

    if last_date:
        start_year = last_date.year
        start_month = last_date.month
        start_day = last_date.day

    # Here, we use last_started_at_date as startDate and calculate endDate to be one month later
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    _, last_day = calendar.monthrange(start_date.year, start_date.month)
    end_date = datetime(start_date.year, start_date.month, last_day)
    print(f"Start date: {start_date}, End date: {end_date}")
    current_date = start_date
    while current_date <= end_date:
        print("Processing date:", current_date)

        start_of_day = datetime(current_date.year, current_date.month, current_date.day)
        end_of_day = start_of_day + timedelta(days=1, seconds=-1)
        start_timestamp = datetime_to_unix_timestamp(start_of_day)
        end_timestamp = datetime_to_unix_timestamp(end_of_day)

        # Constructing the API call with date filters for the current day
        api_url = f"{api_aircalls_url}&from={start_timestamp}&to={end_timestamp}"
        print("API URL:", api_url)
        has_more_data = True
        try:
            while has_more_data:
                response = make_api_call_v2(api_url, api_user, api_pass)
                calls = response.get("calls", [])
                meta = response.get("meta", {})
                print(f"API meta repsonse: {meta}")
                print(f'API calls to process: {len(calls)}')
                # Process the response, insert data into the database, etc.
                next_page_link, has_more_data = process_response_and_insert_calls(response, mysql_conn_id)
                if next_page_link:
                    api_url = next_page_link
                else:
                    has_more_data = False

        except Exception as e:
            print(f"Failed to make API call for {current_date}: {e}")

        if api_test:
            # If in test mode, break after processing the first day
            break

        current_date += timedelta(days=1)

# Defining the DAG
@dag(
    dag_id="aircalls_dag",
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description="A DAG to fetch data from an API for leads",
    schedule_interval=aircalls_cron_schedule,
    start_date=datetime(2024, 3, 10),
    catchup=False,
    tags=["example"],
)
def taskflow_api_fetch():
    start = DummyOperator(task_id='start')

    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=mysql_conn_id,  # Ensure this matches your actual connection ID
        sql=create_tables_sql,
    )
    @task
    def fetch_data():
        return fetch_all_calls()

    end = DummyOperator(task_id='end')

    start >> create_table >> fetch_data() >> end

# Assign the DAG to a variable
dag_instance = taskflow_api_fetch()