from celery import Celery
import pandas as pd
from pandas.errors import EmptyDataError
import os
import time
# from filelock import FileLock
from threading import Thread, Lock, Event
from multiprocessing import Lock as multiprocessingLock
from datetime import datetime, timedelta

BROKER_URL = 'redis://localhost:6379/0'
BACKEND_URL = 'redis://localhost:6379/1'

app = Celery('tasks', broker=BROKER_URL, backend=BACKEND_URL)

buffered_user_active_rows = []
buffered_user_active_lock = multiprocessingLock()


buffered_user_click_rows = []
buffered_user_click_lock = multiprocessingLock()


prevObjects = []


@app.task(name='user clicks')
def queue_click(data):
    global buffered_user_click_rows
    dir = f'data/user_clicks'


    with buffered_user_click_lock:
        if not buffered_user_click_rows:
            data['timestamp'] = pd.to_datetime(data['timestamp'])
            buffered_user_click_rows.append(data)
        else:
            current_timestamp = datetime.now()
            minutes_ago = current_timestamp - timedelta(minutes=1)

            existing_data_condition = any(
                item['user_id'] == data['user_id'] and item['cctv_location'] == data['cctv_location'] and item['timestamp'] > minutes_ago
                for item in buffered_user_click_rows
            )
            if not existing_data_condition:
                data['timestamp'] = pd.to_datetime(data['timestamp'])
                buffered_user_click_rows.append(data)

        print(f"clicks total : {len(buffered_user_click_rows)}")
        
        if len(buffered_user_click_rows) >= 100:
            if not os.path.exists(dir):
                try:
                    os.makedirs(dir)
                    logClick(buffered_user_click_rows)
                except OSError as e:
                    print(f"Error creating directory '{dir}': {e}")
            else:
                logClick(buffered_user_click_rows)   

            buffered_user_click_rows = []

@app.task(name='user active')
def queue_user_active_log(data):
    dir = f'data/user_active_log'
    global buffered_user_active_rows

    with buffered_user_active_lock:
        if not buffered_user_active_rows:
            # If buffer is empty, add the data directly
            data['timestamp'] = pd.to_datetime(data['timestamp'])
            buffered_user_active_rows.append(data)
        else:
            current_timestamp = datetime.now()
            minutes_ago = current_timestamp - timedelta(minutes=1)

            existing_data_condition = any(
                item['user_id'] == data['user_id'] and item['timestamp'] > minutes_ago
                for item in buffered_user_active_rows
            )
            if not existing_data_condition:
                data['timestamp'] = pd.to_datetime(data['timestamp'])
                buffered_user_active_rows.append(data)

        print(f"active total : {len(buffered_user_active_rows)}")

        if len(buffered_user_active_rows) >= 100:
            if not os.path.exists(dir):
                try:
                    os.makedirs(dir)
                    logUserActive(buffered_user_active_rows)
                except OSError as e:
                    print(f"Error creating directory '{dir}': {e}")
            else:
                logUserActive(buffered_user_active_rows)
            
            buffered_user_active_rows = []

# 6125
def logUserActive(data):
    # for item in data:
    #     item['timestamp'] = pd.to_datetime(item['timestamp'])
    # data['timestamp'] = pd.to_datetime(data['timestamp'])
    new_row = pd.DataFrame(data, columns=['user_id', 'timestamp'])

    current_date = datetime.now().strftime('%Y-%m-%d')

    csv_file_path = f'data/user_active_log/{current_date}.csv'

    if os.path.isfile(csv_file_path):
        try:
            existing_df = pd.read_csv(csv_file_path)
            
            expected_headers = ['user_id', 'timestamp']
            if all(header in existing_df.columns for header in expected_headers):
                existing_df = pd.concat([existing_df, new_row], ignore_index=True)
                
                existing_df.to_csv(csv_file_path, index=False)
                
            else:
                os.remove(csv_file_path)
                df = new_row
                df.to_csv(csv_file_path, index=False)
        except EmptyDataError:
            df = new_row
            df.to_csv(csv_file_path, index=False)
    else:
        df = new_row
        df.to_csv(csv_file_path, index=False)

def logClick(data):
    # for item in data:
    #     item['timestamp'] = pd.to_datetime(item['timestamp'])
    # data['timestamp'] = pd.to_datetime(data['timestamp'])
    new_row = pd.DataFrame(data, columns=['user_id', 'timestamp', 'cctv_location'])

    current_date = datetime.now().strftime('%Y-%m-%d')

    # Construct the file name based on the current date
    csv_file_path = f'data/user_clicks/{current_date}.csv'

    if os.path.isfile(csv_file_path):
        try:
            # Try reading the existing CSV file
            existing_df = pd.read_csv(csv_file_path)
            
            # Check if the DataFrame has the expected column headers
            expected_headers = ['user_id', 'timestamp', 'cctv_location']
            if all(header in existing_df.columns for header in expected_headers):
                # Append the new row to the existing DataFrame
                # ---------------------
                
                target_user_ids = list(set([item['user_id'] for item in data]))
                target_cctv_locations = list(set([item['cctv_location'] for item in data]))
                current_timestamp = datetime.now()

                # Convert 'timestamp' column to datetime
                existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'])

                # Calculate the timestamp 5 minutes ago
                minutes_ago = current_timestamp - timedelta(minutes=1)
                
                query = f"user_id in {target_user_ids} and cctv_location in {target_cctv_locations} and timestamp > '{minutes_ago}'"
                
                # Query the DataFrame
                result_df = existing_df.query(query)

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


def flush_buffer_thread(name, buffer, flush_function, flush_interval, lock):
    while True:
        time.sleep(flush_interval)
        print(f'flush buffer {name} {len(buffer)}')
        with lock:
            if buffer:
                flush_function(buffer)
                buffer.clear()  # Clear the buffer after flushing

# @app.task(name='buffer thread')
def start_buffer_threads():
    global buffered_user_click_rows, buffered_user_active_rows
    # Start a separate thread for flushing the user click buffer every 5 seconds
    flush_thread_clicks = Thread(target=flush_buffer_thread, args=('clicks', buffered_user_click_rows, logClick, 5, buffered_user_click_lock))
    flush_thread_clicks.daemon = True
    flush_thread_clicks.start()

    # Start a separate thread for flushing the user active buffer every 5 seconds
    flush_thread_active = Thread(target=flush_buffer_thread, args=('usrLog', buffered_user_active_rows, logUserActive, 5, buffered_user_active_lock))
    flush_thread_active.daemon = True
    flush_thread_active.start()

# start_buffer_threads()

# def monitor_buffer():
#     while True:
#         time.sleep(1)
#         print(len(buffered_user_active_rows))

# buffer_thread = Thread(target=monitor_buffer)
# buffer_thread.daemon = True
# buffer_thread.start()