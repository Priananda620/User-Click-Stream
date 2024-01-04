from celery import Celery
import pandas as pd
from pandas.errors import EmptyDataError
import os
from datetime import datetime, timedelta

BROKER_URL = 'redis://localhost:6379/0'
BACKEND_URL = 'redis://localhost:6379/1'

app = Celery('tasks', broker=BROKER_URL, backend=BACKEND_URL)
# current_date = datetime.now().strftime('%Y-%m-%d')
# csv_file_path = f'data/user_clicks_{current_date}.csv'
# try:
#     df = pd.read_csv(csv_file_path)
# except FileNotFoundError:
#     df = pd.DataFrame(columns=['user_id', 'timestamp', 'cctv_location'])

prevObjects = []

@app.task(name='user clicks')
def queue_click(data):
    # print(data)
    # print('\n\n\n')
    global prevObjects

    target_user_id = data['user_id']
    target_cctv_location = data['cctv_location']
    
    found = any(entry['user_id'] == target_user_id and entry['cctv_location'] == target_cctv_location for entry in prevObjects)
    
    execute(data)

    # if not found:
    #     execute(data)

    #     prevObjects.append(data)
    #     if len(prevObjects) >= 50:
    #         prevObjects.pop(0)

def execute(data):
    data['timestamp'] = pd.to_datetime(data['timestamp'])
    new_row = pd.DataFrame([data], columns=['user_id', 'timestamp', 'cctv_location'])

    current_date = datetime.now().strftime('%Y-%m-%d')

    # Construct the file name based on the current date
    csv_file_path = f'data/user_clicks_{current_date}.csv'

    if os.path.isfile(csv_file_path):
        try:
            # Try reading the existing CSV file
            existing_df = pd.read_csv(csv_file_path)
            
            # Check if the DataFrame has the expected column headers
            expected_headers = ['user_id', 'timestamp', 'cctv_location']
            if all(header in existing_df.columns for header in expected_headers):
                # Append the new row to the existing DataFrame
                # ---------------------
                
                target_user_id = data['user_id']
                target_cctv_location = data['cctv_location']
                current_timestamp = datetime.now()

                # Convert 'timestamp' column to datetime
                existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'])

                # Calculate the timestamp 5 minutes ago
                minutes_ago = current_timestamp - timedelta(minutes=1)

                # Query the DataFrame
                result_df = existing_df.query(f"user_id == '{target_user_id}' and cctv_location == '{target_cctv_location}' and timestamp > '{minutes_ago}'")

                # Check if the result is not empty
                if result_df.empty:
                    existing_df = pd.concat([existing_df, new_row], ignore_index=True)
                    
                    existing_df.to_csv(csv_file_path, index=False)
                
            else:
                # If headers don't match, create a new DataFrame with the new row
                os.remove(csv_file_path)
                df = new_row
                df.to_csv(csv_file_path, index=False)
        except EmptyDataError:
            # Handle the case where the CSV file is completely empty
            df = new_row
            df.to_csv(csv_file_path, index=False)
    else:
        # If the CSV file does not exist, create a new file with the new row
        df = new_row
        df.to_csv(csv_file_path, index=False)
