import json
import random
from datetime import datetime
from kafka import KafkaProducer
import time
import threading

# bootstrap_servers = ['172.27.16.1:9092']
bootstrap_servers = ['10.13.111.40:9092']
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

local_data = []

totalClicks = 0
totalUserActive = 0

def generate_click_messages(thread_id, topicName):
    global totalClicks
    while True:
        user_id = f"user-{random.randint(1, 50)}"
        # user_id = f"user-{totalClicks}"
        current_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        cctv_location = f"CCTV-{random.randint(1, 50)}"

        json_data = {
            "user_id": f"{user_id}_clicks",
            "timestamp": current_timestamp,
            "cctv_location": cctv_location,
            "thread_id": thread_id
        }

        producer.send(topicName, json_data)
        # producer.flush()
        totalClicks += 1
        print(f"[{current_timestamp}] ({totalClicks}) clicks | T{thread_id}")

        # delay = random.randint(10, 80) / 1000.0
        # time.sleep(delay)

def generate_user_active_log(thread_id, topicName):
    global totalUserActive
    while True:

        user_id = f"user-{random.randint(1, 50)}"
        # user_id = f"user-{totalUserActive}"
        current_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        json_data = {
            "user_id": f"{user_id}_activeLogs",
            "timestamp": current_timestamp
        }

        producer.send(topicName, json_data)

        # producer.flush()
        totalUserActive += 1
        print(f"[{current_timestamp}] ({totalUserActive}) user_active_log | T{thread_id}")

        # delay = random.randint(10, 80) / 1000.0
        # time.sleep(delay)


thread1_usrActiveLog = threading.Thread(target=generate_user_active_log, args=(1,'user_active_log',))
thread2_usrActiveLog = threading.Thread(target=generate_user_active_log, args=(2,'user_active_log',))
thread1_clicks = threading.Thread(target=generate_click_messages, args=(1,'clicks',))
thread2_clicks = threading.Thread(target=generate_click_messages, args=(2,'clicks',))

thread1_usrActiveLog.start()
thread2_usrActiveLog.start()
thread1_clicks.start()
thread2_clicks.start()

thread1_usrActiveLog.join()
thread2_usrActiveLog.join()
thread1_clicks.join()
thread2_clicks.join()
