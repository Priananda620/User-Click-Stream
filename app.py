# app.py
from flask import Flask, jsonify
from kafka import KafkaConsumer
import json
from redis import Redis
import pandas as pd
from tasks import queue_click, queue_user_active_log, start_buffer_threads
from celery import Celery
import threading

app = Flask(__name__)
# redis = Redis(host='localhost', port=6379)

csv_file_path = 'user_clicks.csv'
try:
    df = pd.read_csv(csv_file_path)
except FileNotFoundError:
    df = pd.DataFrame(columns=['user_id', 'timestamp', 'cctv_location'])

kafka_consumer_clicks = KafkaConsumer(
    'clicks', 
    bootstrap_servers='172.27.16.1:9092',
    value_deserializer=lambda x: x.decode('utf-8')
)
kafka_consumer_user_active_log = KafkaConsumer(
    'user_active_log', 
    bootstrap_servers='172.27.16.1:9092',
    value_deserializer=lambda x: x.decode('utf-8')
)


def process_click(data):
    global df
    df = pd.concat([df, pd.DataFrame(data)], ignore_index=True)
    df.to_csv(csv_file_path, index=False)

# Kafka message consumer
def kafka_consumer_click_task():
    for message in kafka_consumer_clicks:
        # data = eval(message.value)
        # process_click(data)
        json_data = json.loads(message.value)
        print(f"Received message (topic: clicks): {json_data}")
        queue_click.delay(json_data)

def kafka_consumer_user_active_task():
    for message in kafka_consumer_user_active_log:
        json_data = json.loads(message.value)
        print(f"Received message (topic: user_active_log): {json_data}")
        queue_user_active_log.delay(json_data)

@app.route('/status')
def status():
    return jsonify({'message': 'CSV writing process is running.'})

start_buffer_threads.delay()

kafka_thread_clicks = threading.Thread(target=kafka_consumer_click_task)
kafka_user_active = threading.Thread(target=kafka_consumer_user_active_task)
kafka_thread_clicks.start()
kafka_user_active.start()
kafka_thread_clicks.join()
kafka_user_active.join()

if __name__ == '__main__':
    app.run(debug=True)
