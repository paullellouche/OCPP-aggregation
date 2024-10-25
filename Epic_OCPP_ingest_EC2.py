import requests
import json
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client as Supa_Client
import numpy as np
import pandas as pd
import os

def upload_data(data, table_name, iso=True):
    """
    Uploads a list of dictionaries containing charging session data to Supabase.

    Args:
        data: A list of dictionaries with charging session information.
        table_name: The name of the Supabase table.
        iso: Boolean indicating if timestamps should be converted to ISO format.

    Returns:
        None
    """
    try:
        clear_table(table_name)
        # Insert the data into the table
        response = supabase.table(table_name).insert(data).execute()
        print(f"Successfully inserted {len(data)} rows into {table_name}!")
    except Exception as e:
        print(f"Error uploading data: {e}")

def upload_dataframe(df, table_name):
    """
    Uploads a DataFrame to Supabase.

    Args:
        df: A pandas DataFrame with the data to upload.
        table_name: The name of the Supabase table.

    Returns:
        None
    """
    try:
        data = df.to_dict(orient='records')
        upload_data(data, table_name, iso=False)
    except Exception as e:
        print(f"Error uploading DataFrame: {e}")

def clear_table(table_name):
    """
    Delete all rows from the specified Supabase table based on the presence of 'id' or 'post_id' key.

    Args:
        table_name: The name of the Supabase table.

    Returns:
        None
    """
    try:
        # Fetch a sample row to check for column names
        response = supabase.table(table_name).select('*').limit(1).execute()
        if not response.data:
            print(f"No data found in {table_name}.")
            return

        # Check for presence of 'id' or 'post_id' keys
        column_names = response.data[0].keys()
        if 'id' in column_names:
            supabase.table(table_name).delete().neq('id', 0).execute()
            print(f"All rows with 'id' key deleted from {table_name}!")
        elif 'post_id' in column_names:
            supabase.table(table_name).delete().neq('post_id', '').execute()
            print(f"All rows with 'post_id' key deleted from {table_name}!")
        else:
            print(f"Neither 'id' nor 'post_id' keys found in {table_name}. Cannot perform deletion.")

    except Exception as e:
        print(f"Error deleting rows: {e}")

def time_str_to_timedelta(time_str):
    h, m, s = map(int, time_str.split(':'))
    return timedelta(hours=h, minutes=m, seconds=s)

# Supabase init
supa_url = os.environ.get('SUPABASE_URL')
supa_key = os.environ.get('SUPABASE_KEY')
supabase: Supa_Client = create_client(supa_url, supa_key)

# Extract OCPP Request URLs
organization_id = "e6b91484-34e0-4c93-b1d2-8f0d692553d1"
base_url = "https://coralev.epiccharging.com/api/v1/charger-port/"
base_url_endpoint = "/logs"

epic_chargers = supabase.table('epic_chargers').select('*').execute()
df = pd.DataFrame(epic_chargers.data)
epic_chargers_df = df[['port_uuid', 'postID', 'status']].copy()

# EVStar logs API Headers
cookies = {
    '__ddg1_': 'LsilRXkU2FHikRZNWQeZ',
    '_ga': 'GA1.2.94562456.1724446077',
    'hubspotutk': 'a45646619349b8b266a5588948a9056c',
    '_gcl_au': '1.1.545918164.1724446077',
    '_gid': 'GA1.2.813068722.1725381928',
    '__hstc': '182312952.a45646619349b8b266a5588948a9056c.1724446076720.1724698336910.1725381927639.3',
    '__hssrc': '1',
}

headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-US',
    # 'cookie': '__ddg1_=LsilRXkU2FHikRZNWQeZ; _ga=GA1.2.94562456.1724446077; hubspotutk=a45646619349b8b266a5588948a9056c; _gcl_au=1.1.545918164.1724446077; _gid=GA1.2.813068722.1725381928; __hstc=182312952.a45646619349b8b266a5588948a9056c.1724446076720.1724698336910.1725381927639.3; __hssrc=1',
    'priority': 'u=1, i',
    'referer': 'https://coralev.epiccharging.com/chargers/362bec58-3f5d-49d1-a8bc-2b67757c3c78/tests/logs',
    'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'token-authorization': 'Bearer iLJ1bjX0kz5sRH4U0MOYzF1aqf1QEu',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
}

params = {
    'limit': '500',
    'offset': '0',
}

response_outputRecords = []

for index, row in epic_chargers_df.iterrows():
    url = f"{base_url}{row['port_uuid']}{base_url_endpoint}"

    response = requests.get(
        url,
        params=params,
        cookies=cookies,
        headers=headers,
    )

    if response.status_code == 200:
        response_data = response.json()
    else:
        print(f"Error: {response.status_code} - {response.text}")
        continue

    for log in response_data:
        timestamp = log['timestamp']
        message = log['msg']
        unique_id = f"{timestamp}-*-*-{message}-*-*-{row['port_uuid']}"

        recordObj = {
            'postID': row['postID'],
            'status': row['status'],
            'port_uuid': row['port_uuid'],
            'organization_id': organization_id,
            'timestamp': timestamp,
            'message': message,
            'unique_id': unique_id
        }

        response_outputRecords.append(recordObj)
    print(f"Appended logs for {index} chargers")

'''
seen_unique_ids = set()
filtered_records = []
for record in response_outputRecords:
    if record['unique_id'] not in seen_unique_ids:
        filtered_records.append(record)
        seen_unique_ids.add(record['unique_id'])

upload_data(filtered_records, 'epic_OCPP_ingest')
'''
# Convert the list of response output records to a DataFrame
response_outputRecords_df = pd.DataFrame(response_outputRecords)

# Fetch unique IDs from Supabase
ocpp_table = supabase.table('epic_OCPP_ingest').select('*').execute()
ocpp_table_df = pd.DataFrame(ocpp_table.data)

#Upload to backup table in case of faulty insert
upload_dataframe(ocpp_table_df, 'epic_OCPP_ingest_backup')

# Convert the timestamp columns to timezone-aware datetime using pandas
ocpp_table_df['record_timestamp'] = pd.to_datetime(ocpp_table_df['timestamp'], utc=True)
response_outputRecords_df['record_timestamp'] = pd.to_datetime(response_outputRecords_df['timestamp'], utc=True)

# Get the current time as tz-aware (UTC)
current_time_utc = datetime.now(timezone.utc)

# Filter records to keep only those from the last 6 hours
ocpp_table_df_last_6_hours = ocpp_table_df[ocpp_table_df['record_timestamp'] >= (current_time_utc - timedelta(hours=6))]

# Get the unique IDs from the last 6 hours
ocpp_table_unique_ids_set = set(ocpp_table_df['unique_id'])

# Filter out records older than 6 hours in the new response records
response_outputRecords_df = response_outputRecords_df[response_outputRecords_df['record_timestamp'] >= (current_time_utc - timedelta(hours=6))]

final_upload = []
uniq_tracker = []
# Iterate through response output records and insert into Supabase if unique


final_upload = []
for index, record in response_outputRecords_df.iterrows():
    if record['unique_id'] not in ocpp_table_unique_ids_set:
        # Convert Series (record) to dict for Supabase insertion
        record_dict = record.to_dict()
        record_dict['record_timestamp'] = record_dict['record_timestamp'].isoformat()
        final_upload.append(record_dict)

seen_unique_ids = set()
final_upload_unique = []
for record in final_upload:
    if record['unique_id'] not in seen_unique_ids:
        final_upload_unique.append(record)
        seen_unique_ids.add(record['unique_id'])

# Use upsert to handle duplicates automatically (based on unique_id)
supabase.table('epic_OCPP_ingest').upsert(final_upload_unique, on_conflict=["unique_id"]).execute()

