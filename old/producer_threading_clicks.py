import json
import random
from datetime import datetime
from kafka import KafkaProducer
import time
import threading
import os
import sys

current = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

sys.path.append(current)

import config

bootstrap_servers = config.BOOTSTRAP_SERVER
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

local_data = []

totalClicks = 0
totalUserActive = 0

def generate_click_messages(thread_id, topicName):
    global totalClicks
    for i in range(1000000):
        # user_id = f"user-{random.randint(1, 50)}"
        user_id = f"user-{totalClicks}"
        current_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        cctv_location = f"CCTV-{random.randint(1, 50)}"

        json_data = {
            "user_id": f"{user_id}_clicks",
            "timestamp": current_timestamp,
            "cctv_location": cctv_location
        }

        producer.send(topicName, json_data)
        # producer.flush()
        totalClicks += 1
        print(f"[{current_timestamp}] ({totalClicks}) clicks | T{thread_id}")

        # delay = random.randint(5, 5) / 1000.0
        # time.sleep(delay)


thread1_clicks = threading.Thread(target=generate_click_messages, args=(1,'clicks',))
thread2_clicks = threading.Thread(target=generate_click_messages, args=(2,'clicks',))
thread1_clicks.start()
thread2_clicks.start()
thread1_clicks.join()
thread2_clicks.join()