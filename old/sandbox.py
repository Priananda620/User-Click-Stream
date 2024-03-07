import pandas as pd
import os
from datetime import datetime, timedelta

def get_player_ids_from_log(current_date):
    base_file_path = f'output/user_active_log/{current_date}'
    file_path = f'{base_file_path}.csv'
    six_hours_ago = datetime.now() - timedelta(hours=6)

    try:
        if os.path.isfile(file_path):
            existing_df = pd.read_csv(file_path)
            existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'])
            filtered_df = existing_df[existing_df['timestamp'] >= six_hours_ago]
            filtered_df = filtered_df.dropna(subset=['player_id'])
            player_ids = filtered_df['player_id'].tolist()

            return player_ids if player_ids else []
        else:
            return []
    except Exception as e:
        print(f"Error occurred: {e}")
        return None

def filter_player_ids_is_in_cctv_ids(unique_player_ids, cctv_ids_to_check, current_date):
    player_ids_cctv_found = []
    base_file_path = f'output/user_active_log/{current_date}'
    file_path = f'{base_file_path}.csv'
    try:
        if os.path.isfile(file_path):
            df = pd.read_csv(file_path)
            for player_id in unique_player_ids:
                player_id_to_filter = player_id
                filtered_df = df[df['player_id'] == player_id_to_filter]

                last_row_index = filtered_df.iloc[[-1]]
                cctv_routes_list = eval(last_row_index['cctv_routes'].iloc[0])
                cctv_favorite_list = eval(last_row_index['cctv_favorite'].iloc[0])

                cctv_routes_list = [int(route_id) for route_id in cctv_routes_list]
                cctv_favorite_list = [int(favorite_id) for favorite_id in cctv_favorite_list]

                combined_list = list(set(cctv_routes_list + cctv_favorite_list))

                found = False
                for cctv_id in cctv_ids_to_check:
                    if cctv_id in combined_list:
                        player_ids_cctv_found.append(player_id)
                        found = True
                        break

                print(f'{player_id} {combined_list} {cctv_ids_to_check} : {found}')

            return player_ids_cctv_found
        else:
            return []
    except Exception as e:
        print(f"Error occurred: {e}")
        return None
    

current_date = datetime.now().strftime('%Y-%m-%d')
unique_player_ids = list(set(get_player_ids_from_log(current_date)))

print(unique_player_ids)
cctv_ids_to_check = [7]
player_ids_cctv_found = list(set(filter_player_ids_is_in_cctv_ids(unique_player_ids, cctv_ids_to_check, current_date)))
print(player_ids_cctv_found)