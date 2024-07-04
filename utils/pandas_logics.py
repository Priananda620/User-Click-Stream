import pandas as pd
import glob
import os
from datetime import datetime, timedelta
from typing import List, Optional

__all__ = ['is_file_empty', 'read_and_concat_csv', 'get_inactive_user']

def is_file_empty(file_path: str) -> bool:
    with open(file_path, 'r') as file:
        for line in file:
            if line.strip():  # Check if line contains non-whitespace characters
                return False
    return True

def read_and_concat_csv(directory: str, nth_file:int=None, file_date_reverse:bool=True, sort_timestamp_asc:bool=True) -> Optional[pd.DataFrame]:
    csv_files = [file for file in os.listdir(directory) if file.endswith('.csv')]
    sorted_files = sorted(csv_files, reverse=file_date_reverse)

    if nth_file is not None:
        selected_files = sorted_files[:nth_file]
    else:
        selected_files = sorted_files
    dfs = []
    for file in selected_files:
        file_path = os.path.join(directory, file)
        if not is_file_empty(file_path):
            df = pd.read_csv(file_path)
            dfs.append(df)
    if len(dfs) == 0:
        return None
    concatenated_df = pd.concat(dfs, ignore_index=True)
    concatenated_df.sort_values(by='timestamp', ascending=sort_timestamp_asc, inplace=True)
    return concatenated_df

def get_inactive_user(dataframe: pd.DataFrame, n_hours: int) -> "tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, List[datetime]]":
    # a = b - c
    current_timestamp = datetime.now()
    days_to_subtract, remaining_hours = divmod(n_hours, 24)
    subtracted_time = current_timestamp - timedelta(days=days_to_subtract, hours=remaining_hours)

    dataframe['timestamp'] = pd.to_datetime(dataframe['timestamp'])
    users_active_in_time_period = dataframe[(dataframe['timestamp'] >= subtracted_time) & (dataframe['timestamp'] <= current_timestamp)].drop_duplicates(subset=['user_id']).sort_values(by='timestamp', ascending=True)
    users_active_before_time_period = dataframe[(dataframe['timestamp'] < subtracted_time) & (dataframe['timestamp'] <= current_timestamp)].drop_duplicates(subset=['user_id']).sort_values(by='timestamp', ascending=True)

    inactive_user_for_n_hours = users_active_before_time_period[~users_active_before_time_period['user_id'].isin(users_active_in_time_period['user_id'])]

    return inactive_user_for_n_hours, users_active_before_time_period, users_active_in_time_period, [subtracted_time, current_timestamp]


