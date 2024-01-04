import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# Directory containing CSV files
data_directory = 'data'

# List to store DataFrames from each CSV file
dfs = []

# Iterate over CSV files in the directory
for filename in os.listdir(data_directory):
    if filename.endswith('.csv'):
        file_path = os.path.join(data_directory, filename)
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)
        # Append the DataFrame to the list
        dfs.append(df)

# Concatenate all DataFrames in the list
final_df = pd.concat(dfs, ignore_index=True)

# Convert 'timestamp' to datetime
final_df['timestamp'] = pd.to_datetime(final_df['timestamp'])

# Create 'date' column
final_df['date'] = final_df['timestamp'].dt.date

# K-Means Clustering
# Consider only the time in minutes for clustering
final_df['minutes'] = final_df['timestamp'].dt.hour * 60 + final_df['timestamp'].dt.minute

# Select relevant features
features = final_df[['minutes']]

# Standardize features
scaler = StandardScaler()
features_standardized = scaler.fit_transform(features)

# Perform K-Means clustering (you can adjust the number of clusters as needed)
kmeans = KMeans(n_clusters=3, random_state=42)
final_df['cluster'] = kmeans.fit_predict(features_standardized)

# Plotting logic
plt.figure(figsize=(12, 6))

# Count Plot
plt.subplot(1, 2, 1)
sns.countplot(x='cctv_location', data=final_df, palette='viridis', hue='cluster')
plt.title('Number of Clicks per CCTV Location (Clustered)')
plt.xlabel('CCTV Location')
plt.ylabel('Number of Clicks')
plt.xticks(rotation=45, ha='right')

# Scatter Plot for Clustering
plt.subplot(1, 2, 2)
sns.scatterplot(x='minutes', y='cctv_location', data=final_df, hue='cluster', palette='viridis')
plt.title('K-Means Clustering of Clicks over Time')
plt.xlabel('Time in Minutes')
plt.ylabel('CCTV Location')
plt.xticks(rotation=45, ha='right')

plt.tight_layout()

# Save the plot
output_folder = 'plot'
os.makedirs(output_folder, exist_ok=True)
output_file = os.path.join(output_folder, 'plot_clustered.png')
plt.savefig(output_file)

current_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
print(f"[{current_timestamp}] Ran ")
