import requests
import json
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client as Supa_Client
import numpy as np
import pandas as pd
import re
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

def fetch_all_records(table_name, chunk_size=1000):
    offset = 0
    all_records = []
    while True:
        response = supabase.table(table_name).select('*').range(offset, offset + chunk_size - 1).execute()
        if len(response.data) == 0:
            break
        all_records.extend(response.data)
        offset += chunk_size
    return all_records

def time_str_to_timedelta(time_str):
    h, m, s = map(int, time_str.split(':'))
    return timedelta(hours=h, minutes=m, seconds=s)

def extract_uuid(message):
    match = re.search(uuid_pattern, message)
    if match:
        return match.group(1)  # Return the matched UUID
    else:
        return None  # Return None if no UUID is found

#MeterValues
def split_message_to_parts(message):
    try:
        # First, split by commas to isolate call type, messageid, and JSON part
        parts = message.split(',', 2)

        # Extract call type and message ID
        call_type = parts[0].strip()[2:].strip()  # Extract the call type by removing the leading "<- [" or "-> ["
        message_id = parts[1].strip()

        # Find the start of the JSON part and split the message
        split_point = parts[2].find('{')
        non_json_part = parts[2][:split_point].strip()  # Everything before the JSON

        json_str = parts[2][split_point:]  # The actual JSON string
        json_part = json.loads(json_str)  # Parse the JSON string

        return call_type, message_id, non_json_part, json_part

    except (IndexError, json.JSONDecodeError):
        return None, None, message, None

def extract_measurand_details(sampled_values, measurand):
    if sampled_values:
        for item in sampled_values:
            if item.get('measurand') == measurand:
                return item.get('value'), item.get('context'), item.get('unit')
    return None, None, None  # Return None for value, context, and unit if no match

def load_cached_data():
    """Load the cached data from a file."""
    if os.path.exists(CACHE_FILE):
        return pd.read_json(CACHE_FILE)
    return pd.DataFrame()  # Return empty DataFrame if no cache exists

# Supabase init
supa_url = "https://fhpjpebdlkimoawryvmf.supabase.co"
supa_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZocGpwZWJkbGtpbW9hd3J5dm1mIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTcwODEwMzAzMiwiZXhwIjoyMDIzNjc5MDMyfQ.h5vkW7HviEbgml_uyv5nRz09-O65Ypb1wjt8P5qZQfY"
supabase: Supa_Client = create_client(supa_url, supa_key)

# Define regex patterns
uuid_pattern = r'\"([a-f0-9\-]{36})\"'
# Cache file paths
CACHE_FILE = 'data_cache.json'
LAST_FETCH_FILE = 'last_fetch_timestamp.txt'


# Pull OCPP
ocpp_ingest_table = fetch_all_records('epic_OCPP_ingest')
ocpp_ingest_df = pd.DataFrame(ocpp_ingest_table)

# Separate IO and non-IO formatted messages
ocpp_io_unformatted_df = ocpp_ingest_df[ocpp_ingest_df['message'].str.contains('->|<-', regex=True)].copy()
ocpp_nonio_unformatted_df = ocpp_ingest_df[~ocpp_ingest_df['message'].str.contains('->|<-', regex=True)].copy()

# Break down JSON into columns
ocpp_io_unformatted_df['direction'] = np.where(
    ocpp_io_unformatted_df['message'].str.contains('<-'), 'CP to Server',
    np.where(ocpp_io_unformatted_df['message'].str.contains('->'), 'Server to CP', None)
)

ocpp_io_unformatted_df['transactionid'] = ocpp_io_unformatted_df['message'].apply(extract_uuid)
ocpp_io_unformatted_df.drop(columns=['record_timestamp'], inplace=True)

# Further categotization by message type

#MeterValues
ocpp_meter_values_df = ocpp_io_unformatted_df[
    ocpp_io_unformatted_df['message'].str.contains('MeterValues') &
    ~ocpp_io_unformatted_df['message'].str.contains('ChangeConfiguration')
].copy()


ocpp_meter_values_df.drop(columns=['id', 'created_at'], inplace=True)

ocpp_meter_values_df['call_type'], ocpp_meter_values_df['message_id'], ocpp_meter_values_df['non_json_part'], ocpp_meter_values_df['message_dict'] = zip(*ocpp_meter_values_df['message'].apply(split_message_to_parts))
ocpp_meter_values_df['meterValue'] = ocpp_meter_values_df['message_dict'].apply(lambda x: x.get('meterValue') if x else None)
ocpp_meter_values_df['transactionId'] = ocpp_meter_values_df['message_dict'].apply(lambda x: x.get('transactionId') if x else None)
ocpp_meter_values_df['sampled_value'] = ocpp_meter_values_df['message_dict'].apply(
    lambda x: x['meterValue'][0]['sampledValue'] if isinstance(x, dict) and 'meterValue' in x and x['meterValue'] and 'sampledValue' in x['meterValue'][0] else None
)

ocpp_meter_values_df['Temperature'], ocpp_meter_values_df['Temperature_context'], ocpp_meter_values_df['Temperature_unit'] = zip(*ocpp_meter_values_df['sampled_value'].apply(lambda x: extract_measurand_details(x, 'Temperature')))
ocpp_meter_values_df['Voltage'], ocpp_meter_values_df['Voltage_context'], ocpp_meter_values_df['Voltage_unit'] = zip(*ocpp_meter_values_df['sampled_value'].apply(lambda x: extract_measurand_details(x, 'Voltage')))
ocpp_meter_values_df['CurrentImport'], ocpp_meter_values_df['CurrentImport_context'], ocpp_meter_values_df['CurrentImport_unit'] = zip(*ocpp_meter_values_df['sampled_value'].apply(lambda x: extract_measurand_details(x, 'Current.Import')))
ocpp_meter_values_df['CurrentExport'], ocpp_meter_values_df['CurrentExport_context'], ocpp_meter_values_df['CurrentExport_unit'] = zip(*ocpp_meter_values_df['sampled_value'].apply(lambda x: extract_measurand_details(x, 'Current.Export')))
ocpp_meter_values_df['PowerFactor'], ocpp_meter_values_df['PowerFactor_context'], ocpp_meter_values_df['PowerFactor_unit'] = zip(*ocpp_meter_values_df['sampled_value'].apply(lambda x: extract_measurand_details(x, 'Power.Factor')))
ocpp_meter_values_df['PowerOffered'], ocpp_meter_values_df['PowerOffered_context'], ocpp_meter_values_df['PowerOffered_unit'] = zip(*ocpp_meter_values_df['sampled_value'].apply(lambda x: extract_measurand_details(x, 'Power.Offered')))
ocpp_meter_values_df['RPM'], ocpp_meter_values_df['RPM_context'], ocpp_meter_values_df['RPM_unit'] = zip(*ocpp_meter_values_df['sampled_value'].apply(lambda x: extract_measurand_details(x, 'RPM')))
ocpp_meter_values_df['VehicleSoC'], ocpp_meter_values_df['VehicleSoC_context'], ocpp_meter_values_df['VehicleSoC_unit'] = zip(*ocpp_meter_values_df['sampled_value'].apply(lambda x: extract_measurand_details(x, 'SOC')))
ocpp_meter_values_df['Energy_Active_Import'], ocpp_meter_values_df['Energy_Active_Import_context'], ocpp_meter_values_df['Energy_Active_Import_unit'] = zip(*ocpp_meter_values_df['sampled_value'].apply(lambda x: extract_measurand_details(x, 'Energy.Active.Import.Register')))


print(ocpp_meter_values_df)