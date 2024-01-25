from celery import Celery
import pandas as pd
from pandas.errors import EmptyDataError
import os
import time
# from filelock import FileLock
from threading import Thread, Lock, Event
from multiprocessing import Lock as multiprocessingLock
from datetime import datetime, timedelta
import glob
import config

BROKER_URL = config.REDIS_BROKER_URL
BACKEND_URL = config.REDIS_BACKEND_URL

app = Celery('tasks', broker=BROKER_URL, backend=BACKEND_URL)

buffered_user_active_rows = []
buffered_user_active_lock = multiprocessingLock()


buffered_user_click_rows = []
buffered_user_click_lock = multiprocessingLock()


prevObjects = []
MAX_ROW_PER_CSV = 50000
FLUSH_INTERVAL_SEC = 15
MAX_BUFFER_LEN = 50000

flushThreadStarted = False

@app.task(name='user clicks')
def queue_click(data):
    global buffered_user_click_rows
    
    # current_date = datetime.now().strftime('%Y-%m-%d')
    # dir = f'data/user_clicks/{current_date}/'


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
            else:
                print(f'------- BUFFER IS EXIST ------')
            ###########
            # data['timestamp'] = pd.to_datetime(data['timestamp'])
            # buffered_user_click_rows.append(data)

        print(f"clicks total : {len(buffered_user_click_rows)}")
        
        if len(buffered_user_click_rows) > MAX_BUFFER_LEN:
            flush_click_log(buffered_user_click_rows)


@app.task(name='user active')
def queue_user_active_log(data):
    # current_date = datetime.now().strftime('%Y-%m-%d')
    # dir = f'data/user_active_log/{current_date}/'
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
            else:
                print(f'-------IS EXIST------')
            ############
            # data['timestamp'] = pd.to_datetime(data['timestamp'])
            # buffered_user_active_rows.append(data)

        print(f"active total : {len(buffered_user_active_rows)}")

        if len(buffered_user_active_rows) > MAX_BUFFER_LEN:
            flush_user_active(buffered_user_active_rows)
            

def flush_user_active(data):
    current_date = datetime.now().strftime('%Y-%m-%d')
    dir = f'data/user_active_log/{current_date}/'
    if not os.path.exists(dir):
        try:
            os.makedirs(dir)
            logUserActive(data)
        except OSError as e:
            print(f"Error creating directory '{dir}': {e}")
    else:
        logUserActive(data)
    
    data.clear()

def flush_click_log(data):
    current_date = datetime.now().strftime('%Y-%m-%d')
    dir = f'data/user_clicks/{current_date}/'

    if not os.path.exists(dir):
        try:
            os.makedirs(dir)
            logClick(data)
        except OSError as e:
            print(f"Error creating directory '{dir}': {e}")
    else:
        logClick(data)   

    data.clear()

def get_latest_file_number(directory):
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    numbers = [int(file.split('.')[0]) for file in files]
    return max(numbers) if numbers else 0

def concat_all_csv_files(directory):
    csv_files = glob.glob(os.path.join(directory, '*.csv'))
    all_data = pd.DataFrame()

    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            all_data = pd.concat([all_data, df], ignore_index=True)
        except pd.errors.EmptyDataError:
            pass
    return all_data

def concat_last_nth_csv_files(directory, nth):
    csv_files = glob.glob(os.path.join(directory, '*.csv'))
    file_numbers = [int(os.path.splitext(os.path.basename(file))[0]) for file in csv_files]
    last_nth_file_numbers = sorted(file_numbers, reverse=True)[:nth]
    all_data = pd.DataFrame()
    for num in reversed(last_nth_file_numbers):
        try:
            df = pd.read_csv(os.path.join(directory, f"{num}.csv"))
            all_data = pd.concat([all_data, df], ignore_index=True)
        except pd.errors.EmptyDataError:
            all_data = pd.concat([all_data, pd.DataFrame(columns=df.columns)], ignore_index=False)

    return all_data

# 6125
def logUserActive(data):
    new_row = pd.DataFrame(data, columns=['user_id', 'timestamp'])

    current_date = datetime.now().strftime('%Y-%m-%d')

    base_csv_file_path = f'data/user_active_log/{current_date}/'

    latest_file_number = get_latest_file_number(base_csv_file_path)

    csv_file_path = f'{base_csv_file_path}{latest_file_number}.csv'


    if os.path.isfile(csv_file_path):
        print('---- EXIST FILE 0 ----')
        try:
            existing_df = pd.read_csv(csv_file_path)
            
            expected_headers = ['user_id', 'timestamp']
            if all(header in existing_df.columns for header in expected_headers):
                # existing_df = pd.concat([existing_df, new_row], ignore_index=True)
                # existing_df.to_csv(csv_file_path, index=False)

                ######## added filtering

                target_user_ids = list(set([item['user_id'] for item in data]))
                current_timestamp = datetime.now()

                existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'])

                minutes_ago = current_timestamp - timedelta(minutes=1)
                
                query = f"user_id in {target_user_ids} and timestamp > '{minutes_ago}'"
            
                result_df = existing_df.query(query)
                # result_df = concat_last_nth_csv_files(base_csv_file_path, 2).query(query)

                if result_df.empty:
                #     if len(existing_df) > 50:
                #         latest_file_number += 1
                #         csv_file_path = f'{base_csv_file_path}{latest_file_number}.csv'
                #         print(f'---- NEW FILE {latest_file_number} DUE TO ROW LIMIT ----')
                #         df = new_row
                #         df.to_csv(csv_file_path, index=False)
                #     else:
                #         existing_df = pd.concat([existing_df, new_row], ignore_index=True)
                        
                #         existing_df.to_csv(csv_file_path, index=False)

                    remaining_capacity = MAX_ROW_PER_CSV - len(existing_df)

                    if len(new_row) > remaining_capacity:
                        # Split the new row into chunks
                        new_row_chunk, remaining_new_row = new_row.iloc[:remaining_capacity], new_row.iloc[remaining_capacity:]
                        
                        # Append the chunk to the current file
                        existing_df = pd.concat([existing_df, new_row_chunk], ignore_index=True)
                        existing_df.to_csv(csv_file_path, index=False)

                        # Create a new file for the remaining new row
                        latest_file_number += 1
                        csv_file_path = f'{base_csv_file_path}{latest_file_number}.csv'
                        remaining_new_row.to_csv(csv_file_path, index=False)

                        print(f'---- NEW FILE {latest_file_number} DUE TO ROW LIMIT ----')
                    else:
                        # Append the entire new row to the existing file
                        existing_df = pd.concat([existing_df, new_row], ignore_index=True)
                        existing_df.to_csv(csv_file_path, index=False)
                else:
                    print(f'------- IS EXIST CSV ------')
            else:
                os.remove(csv_file_path)
                df = new_row
                df.to_csv(csv_file_path, index=False)
        except EmptyDataError:
            df = new_row
            df.to_csv(csv_file_path, index=False)
    else:
        print('---- NOT EXIST FILE 0 ----')
        df = new_row
        df.to_csv(csv_file_path, index=False)

def logClick(data):
    new_row = pd.DataFrame(data, columns=['user_id', 'timestamp', 'cctv_location'])

    current_date = datetime.now().strftime('%Y-%m-%d')

    
    base_csv_file_path = f'data/user_clicks/{current_date}/'

    latest_file_number = get_latest_file_number(base_csv_file_path)

    csv_file_path = f'{base_csv_file_path}{latest_file_number}.csv'

    if os.path.isfile(csv_file_path):
        print('---- EXIST FILE 1 ----')
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
                
                result_df = existing_df.query(query)
                # result_df = concat_last_nth_csv_files(base_csv_file_path, 2).query(query)

                if result_df.empty:
                    # existing_df = pd.concat([existing_df, new_row], ignore_index=True)
                    
                    # existing_df.to_csv(csv_file_path, index=False)

                    # if len(existing_df) > 50:
                    #     latest_file_number += 1
                    #     csv_file_path = f'{base_csv_file_path}{latest_file_number}.csv'
                    #     print(f'---- NEW FILE {latest_file_number} DUE TO ROW LIMIT ----')
                    #     df = new_row
                    #     df.to_csv(csv_file_path, index=False)
                    # else:
                    #     existing_df = pd.concat([existing_df, new_row], ignore_index=True)
                        
                    #     existing_df.to_csv(csv_file_path, index=False)

                    remaining_capacity = MAX_ROW_PER_CSV - len(existing_df)

                    if len(new_row) > remaining_capacity:
                        # Split the new row into chunks
                        new_row_chunk, remaining_new_row = new_row.iloc[:remaining_capacity], new_row.iloc[remaining_capacity:]
                        
                        # Append the chunk to the current file
                        existing_df = pd.concat([existing_df, new_row_chunk], ignore_index=True)
                        existing_df.to_csv(csv_file_path, index=False)

                        # Create a new file for the remaining new row
                        latest_file_number += 1
                        csv_file_path = f'{base_csv_file_path}{latest_file_number}.csv'
                        remaining_new_row.to_csv(csv_file_path, index=False)

                        print(f'---- NEW FILE {latest_file_number} DUE TO ROW LIMIT ----')
                    else:
                        # Append the entire new row to the existing file
                        existing_df = pd.concat([existing_df, new_row], ignore_index=True)
                        existing_df.to_csv(csv_file_path, index=False)
                else:
                    print(f'------- IS EXIST CSV ------')
                
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
        print('---- NOT EXIST FILE 1 ----')
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

@app.task(name='buffer thread')
def start_buffer_threads():
    global buffered_user_click_rows, buffered_user_active_rows, flushThreadStarted
    # Start a separate thread for flushing the user click buffer every 5 seconds
    if not flushThreadStarted:
        flush_thread_clicks = Thread(target=flush_buffer_thread, args=('CLICKS', buffered_user_click_rows, flush_click_log, FLUSH_INTERVAL_SEC, buffered_user_click_lock))
        flush_thread_clicks.daemon = True
        flush_thread_clicks.start()

        # Start a separate thread for flushing the user active buffer every 5 seconds
        flush_thread_active = Thread(target=flush_buffer_thread, args=('USRLOG', buffered_user_active_rows, flush_user_active, FLUSH_INTERVAL_SEC, buffered_user_active_lock))
        flush_thread_active.daemon = True
        flush_thread_active.start()

        flushThreadStarted = True

# start_buffer_threads()

# def monitor_buffer():
#     while True:
#         time.sleep(1)
#         print(len(buffered_user_active_rows))

# buffer_thread = Thread(target=monitor_buffer)
# buffer_thread.daemon = True
# buffer_thread.start()