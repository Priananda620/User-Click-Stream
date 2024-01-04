# app.py
from flask import Flask, jsonify
from kafka import KafkaConsumer
import json
from redis import Redis
import pandas as pd
from tasks import queue_click
from celery import Celery

app = Flask(__name__)
# redis = Redis(host='localhost', port=6379)

# Define the path to the CSV file
csv_file_path = 'user_clicks.csv'

# Create an empty DataFrame or load existing data if the file exists
try:
    df = pd.read_csv(csv_file_path)
except FileNotFoundError:
    df = pd.DataFrame(columns=['user_id', 'timestamp', 'cctv_location'])

# Kafka consumer configuration
kafka_consumer = KafkaConsumer(
    'clicks', 
    bootstrap_servers='172.27.16.1:9092',
    value_deserializer=lambda x: x.decode('utf-8')
)

# Function to process click data and enqueue CSV writing
def process_click(data):
    global df
    # Append the data to the DataFrame
    df = pd.concat([df, pd.DataFrame(data)], ignore_index=True)
    
    # Save the DataFrame to the CSV file
    df.to_csv(csv_file_path, index=False)

# Kafka message consumer
def kafka_consumer_task():
    for message in kafka_consumer:
        # Convert message value (assumed to be JSON) to Python dictionary
        # data = eval(message.value)
        # process_click(data)
        json_data = json.loads(message.value)
        print(f"Received message: {json_data}")
        queue_click.delay(json_data)

# Run Kafka consumer as a separate thread
import threading
kafka_thread = threading.Thread(target=kafka_consumer_task)
kafka_thread.start()

# Endpoint to check the status of the CSV writing process
@app.route('/status')
def status():
    return jsonify({'message': 'CSV writing process is running.'})

if __name__ == '__main__':
    app.run(debug=True)
