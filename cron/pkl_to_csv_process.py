import pandas as pd #2.2.0 need PyArrow 
from datetime import datetime, timedelta
import glob
import os

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

def concat_last_nth_pkl_files(directory, nth):
    pkl_files = glob.glob(os.path.join(directory, '*.pkl'))
    file_numbers = [int(os.path.splitext(os.path.basename(file))[0]) for file in pkl_files]
    last_nth_file_numbers = sorted(file_numbers, reverse=True)[:nth]
    all_data = pd.DataFrame()
    for num in reversed(last_nth_file_numbers):
        try:
            df = pd.read_pickle(os.path.join(directory, f"{num}.pkl"))
            all_data = pd.concat([all_data, df], ignore_index=True)
        except pd.errors.EmptyDataError:
            all_data = pd.concat([all_data, pd.DataFrame(columns=df.columns)], ignore_index=False)
    return all_data

def concat_all_pkl_files(directory):
    pkl_files = glob.glob(os.path.join(directory, '*.pkl'))
    all_data = pd.DataFrame()

    for pkl_file in pkl_files:
        try:
            df = pd.read_pickle(pkl_file)
            all_data = pd.concat([all_data, df], ignore_index=True)
        except pd.errors.EmptyDataError:
            pass
    return all_data

home_directory = os.path.expanduser("~")
base_dir = f'{home_directory}/flask/User-Click-Stream'

current_date = datetime.now().strftime('%Y-%m-%d')
base_csv_file_path = f'{base_dir}/data/user_active_log/{current_date}/'
latest_file_number = concat_all_pkl_files(base_csv_file_path)


output_dir = f'{base_dir}/output/'
if not os.path.exists(output_dir):
    try:
        os.makedirs(output_dir)
    except OSError as e:
        print(f"Error creating directory '{dir}': {e}")

latest_file_number.to_csv(f'{output_dir}{current_date}.csv', index=False)

current_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
print(f"[{current_timestamp}] pkl to csv output to {current_date}.csv")


# df = pd.read_csv(f'output/{current_date}.csv')
# df['user_id'] = df['user_id'].str.extract(r'user-(\d+)_activeLogs').astype(int)
# all_user_ids = set(range(1000000))
# existing_user_ids = set(df['user_id'])
# missing_user_ids = all_user_ids - existing_user_ids
# duplicate_user_ids = df[df.duplicated('user_id')]['user_id']
# print(f"Missing user IDs: {missing_user_ids}, Duplicate user IDs: {duplicate_user_ids}")
