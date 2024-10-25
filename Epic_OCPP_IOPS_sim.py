import requests
import json
from datetime import datetime, timedelta
from supabase import create_client, Client as Supa_Client
import pandas as pd
from pympler import asizeof
import psutil
import time

def measure_iops(upload_function, *args):
    """
    Measures IOPS during the actual upload to Supabase by tracking disk read/write operations.

    Args:
        upload_function: The function responsible for uploading data.
        *args: Arguments to pass to the upload function.

    Returns:
        None
    """
    # Capture initial disk I/O counters
    io_before = psutil.disk_io_counters()
    start_time = time.time()

    # Execute the actual upload function
    upload_function(*args)

    # Capture final disk I/O counters after the upload
    io_after = psutil.disk_io_counters()
    end_time = time.time()

    # Calculate elapsed time
    elapsed_time = end_time - start_time

    # Calculate IOPS
    read_iops = (io_after.read_count - io_before.read_count) / elapsed_time
    write_iops = (io_after.write_count - io_before.write_count) / elapsed_time

    # Print results
    print(f"Read IOPS: {read_iops:.2f}")
    print(f"Write IOPS: {write_iops:.2f}")
    print(f"Total IOPS: {read_iops + write_iops:.2f}")


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
        # Convert DataFrame to list of dictionaries (simulate local processing)
        data = df.to_dict(orient='records')

        # Measure IOPS during the actual upload
        measure_iops(upload_data, data, table_name)

    except Exception as e:
        print(f"Error uploading DataFrame: {e}")


# Supabase initialization (ensure to replace these with your real Supabase credentials)
supa_url = "https://fhpjpebdlkimoawryvmf.supabase.co"
supa_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZocGpwZWJkbGtpbW9hd3J5dm1mIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTcwODEwMzAzMiwiZXhwIjoyMDIzNjc5MDMyfQ.h5vkW7HviEbgml_uyv5nRz09-O65Ypb1wjt8P5qZQfY"
supabase: Supa_Client = create_client(supa_url, supa_key)

#Extract OCPP Request URLs
organization_id = "e6b91484-34e0-4c93-b1d2-8f0d692553d1"
base_url = "https://coralev.epiccharging.com/api/v1/charger-port/"
base_url_endpoint = "/logs"

epic_chargers = supabase.table('epic_chargers').select('*').execute()
df = pd.DataFrame(epic_chargers.data)
epic_chargers_df = df[['port_uuid', 'charger_point_id', 'status']].copy()

response_outputRecords = []

# Simulating the data collection and preparation
for index, row in epic_chargers_df.iterrows():
    url = f"{base_url}{row['port_uuid']}{base_url_endpoint}"

    response = requests.get(
        url,
        params={
            'limit': '500',
            'offset': '0',
        },
        cookies={
            '__ddg1_': 'LsilRXkU2FHikRZNWQeZ',
            '_ga': 'GA1.2.94562456.1724446077',
        },
        headers={
            'accept': 'application/json, text/plain, */*',
            'referer': 'https://coralev.epiccharging.com/chargers/362bec58-3f5d-49d1-a8bc-2b67757c3c78/tests/logs',
            'token-authorization': 'Bearer iLJ1bjX0kz5sRH4U0MOYzF1aqf1QEu',
        }
    )

    if response.status_code == 200:
        response_data = response.json()
    else:
        print(f"Error: {response.status_code} - {response.text}")

    for log in response_data:
        recordObj = {
            'charger_point_id': row['charger_point_id'],
            'status': row['status'],
            'port_uuid': row['port_uuid'],
            'organization_id': organization_id,
            'timestamp': log['timestamp'],
            'message': log['msg'],
        }
        response_outputRecords.append(recordObj)

    print(f"Appended logs for {index} chargers")

# Upload the prepared data to Supabase and measure IOPS
df_response_records = pd.DataFrame(response_outputRecords)
upload_dataframe(df_response_records[:34], 'epic_OCPP')

# Memory size of the response_outputRecords
size = asizeof.asizeof(response_outputRecords)
print(f"Total memory size of response_outputRecords: {size} bytes")
