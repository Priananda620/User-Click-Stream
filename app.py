#create_ app.py
from flask import Flask, jsonify
from kafka import KafkaConsumer
import json
from redis import Redis
import pandas as pd
from tasks.tasks import queue_click, queue_user_active_log, start_buffer_threads
from celery import Celery
import threading
from concurrent.futures import ProcessPoolExecutor
import config
from blueprints import status

kafka_consumer_clicks = KafkaConsumer(
    'clicks', 
    bootstrap_servers=config.BOOTSTRAP_SERVER,
    value_deserializer=lambda x: x.decode('utf-8')
)
kafka_consumer_user_active_log = KafkaConsumer(
    'user_active_log', 
    bootstrap_servers=config.BOOTSTRAP_SERVER,
    value_deserializer=lambda x: x.decode('utf-8')
)

def kafka_consumer_click_task():
    for message in kafka_consumer_clicks:
        json_data = json.loads(message.value)
        print(f"Received message (topic: clicks): {json_data}")
        queue_click.delay(json_data)

def kafka_consumer_user_active_task():
    for message in kafka_consumer_user_active_log:
        json_data = json.loads(message.value)
        print(f"Received message (topic: user_active_log): {json_data}")
        queue_user_active_log.delay(json_data)

def create_app() -> Flask:
    app = Flask("User Logs")
    app.config.from_object(config)

    with app.app_context():
        app.register_blueprint(status.status_blueprint)
        for i in range(100):
            start_buffer_threads.delay()

        kafka_thread_clicks = threading.Thread(target=kafka_consumer_click_task)
        kafka_user_active = threading.Thread(target=kafka_consumer_user_active_task)
        kafka_thread_clicks.start()
        kafka_user_active.start()
        kafka_thread_clicks.join()
        kafka_user_active.join()

        # with ProcessPoolExecutor() as executor:
        #     executor.submit(kafka_consumer_click_task)
        #     executor.submit(kafka_consumer_user_active_task)

        return app

if __name__ == "__main__":
    app = Flask("User Logs")
    app.config.from_object(config)
    app.run(debug=True)