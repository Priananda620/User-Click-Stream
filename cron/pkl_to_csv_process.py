import pandas as pd #2.2.0 need PyArrow 
from datetime import datetime, timedelta
import glob
import os

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

def get_subdirectories(parent_dir):
    subdirectories = []
    for entry in os.listdir(parent_dir):
        full_path = os.path.join(parent_dir, entry)
        if os.path.isdir(full_path):
            subdirectories.append(full_path)
    return subdirectories

def append_child_dir(subdirectories, child):
    subdirectories_with_date = []
    for subdir in subdirectories:
        subdir_with_date = os.path.join(subdir, child)
        subdirectories_with_date.append(subdir_with_date)
    return subdirectories_with_date

def storeOutput(df, child_dir, base_dir, output_date):
    output_dir = f'{base_dir}/output/{child_dir}/'

    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir)
        except OSError as e:
            print(f"Error creating directory '{dir}': {e}")

    df.to_csv(f'{output_dir}{output_date}.csv', index=False)

home_directory = os.path.expanduser("~")
base_dir = f'{home_directory}/flask/User-Click-Stream'

subdirs = get_subdirectories(f'{base_dir}/data')
current_date = datetime.now().strftime('%Y-%m-%d')
subdirs = append_child_dir(subdirs, current_date)
# print(subdirs)

for subdir in subdirs:
    components = subdir.split(os.sep)
    second_last_child = components[-2]
    date = os.path.basename(subdir)

    subdir_df = concat_all_pkl_files(subdir)
    storeOutput(subdir_df, second_last_child, base_dir, date)

    current_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    print(f"[{current_timestamp}] (topic:{second_last_child}) pkl to csv output to {date}.csv")
    

# base_csv_file_path = f'{base_dir}/data/user_active_log/{current_date}/'
# latest_file_number = concat_all_pkl_files(base_csv_file_path)

# output_dir = f'{base_dir}/output/'
# if not os.path.exists(output_dir):
#     try:
#         os.makedirs(output_dir)
#     except OSError as e:
#         print(f"Error creating directory '{dir}': {e}")

# latest_file_number.to_csv(f'{output_dir}{current_date}.csv', index=False)

# current_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
# print(f"[{current_timestamp}] pkl to csv output to {current_date}.csv")


# df = pd.read_csv(f'output/{current_date}.csv')
# df['user_id'] = df['user_id'].str.extract(r'user-(\d+)_activeLogs').astype(int)
# all_user_ids = set(range(1000000))
# existing_user_ids = set(df['user_id'])
# missing_user_ids = all_user_ids - existing_user_ids
# duplicate_user_ids = df[df.duplicated('user_id')]['user_id']
# print(f"Missing user IDs: {missing_user_ids}, Duplicate user IDs: {duplicate_user_ids}")
