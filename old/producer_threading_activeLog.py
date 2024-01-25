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

def generate_user_active_log(thread_id, topicName):
    global totalUserActive
    for i in range(1000000):

        # user_id = f"user-{random.randint(1, 50)}"
        user_id = f"user-{totalUserActive}"
        current_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        json_data = {
            "user_id": f"{user_id}_activeLogs",
            "timestamp": current_timestamp
        }

        producer.send(topicName, json_data)

        # producer.flush()
        totalUserActive += 1
        print(f"[{current_timestamp}] ({totalUserActive}) user_active_log | T{thread_id}")

        delay = random.randint(100, 100) / 1000.0
        time.sleep(delay)


thread1_usrActiveLog = threading.Thread(target=generate_user_active_log, args=(1,'user_active_log',))
thread2_usrActiveLog = threading.Thread(target=generate_user_active_log, args=(2,'user_active_log',))
thread1_usrActiveLog.start()
thread2_usrActiveLog.start()
thread1_usrActiveLog.join()
thread2_usrActiveLog.join()
