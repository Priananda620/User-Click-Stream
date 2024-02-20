import pandas as pd
import os
from datetime import datetime, timedelta

def get_player_ids_from_log():
    current_date = datetime.now().strftime('%Y-%m-%d')
    base_file_path = f'output/user_active_log/{current_date}'
    file_path = f'{base_file_path}.csv'
    six_hours_ago = datetime.now() - timedelta(hours=6)

    try:
        if os.path.isfile(file_path):
            existing_df = pd.read_csv(file_path)
            existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'])
            filtered_df = existing_df[existing_df['timestamp'] >= six_hours_ago]
            player_ids = filtered_df['player_id'].tolist()

            return player_ids if player_ids else []
        else:
            return []
    except Exception as e:
        print(f"Error occurred: {e}")
        return None

print(get_player_ids_from_log())