import pandas as pd
from datetime import datetime, timedelta

# Sample DataFrame creation (replace this with your actual DataFrame)
current_date = datetime.now().strftime('%Y-%m-%d')

# Construct the file name based on the current date
csv_file_path = f'data/user_clicks_{current_date}.csv'
existing_df = pd.read_csv(csv_file_path)

# Your target values
target_user_id = 'user-1'
target_cctv_location = 'CCTV-4'

existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'])

# Step 3: Get current timestamp and calculate 5 minutes ago
current_time = datetime.now()
five_minutes_ago = current_time - timedelta(minutes=5)

# Step 4: Use boolean indexing to filter rows
filtered_df = existing_df[existing_df['timestamp'] > five_minutes_ago]
result_df = existing_df.query(f"user_id == '{target_user_id}' and cctv_location == '{target_cctv_location}' and timestamp > '{five_minutes_ago}'")

# ========================================
duplicate_rows = existing_df[existing_df.duplicated(['user_id', 'cctv_location'], keep=False)]

# Display the duplicated rows
print(duplicate_rows)
# print(result_df)
