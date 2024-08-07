import json
import random
from datetime import datetime
from kafka import KafkaProducer
import time

import os
import sys

current = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

sys.path.append(current)
# print(sys.path)

import config

bootstrap_servers = config.BOOTSTRAP_SERVER

producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
local_data = []
totalUserActive = 0
topicName = 'user_active_log'

def generate_user_active_log():
    global totalUserActive
    # while True:
    for i in range(1000000):
        # user_id = f"user-{random.randint(1, 50)}"
        user_id = f"user-{totalUserActive}"
        current_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        cctv_routes = [random.randint(1, 1000) for _ in range(3)]
        cctv_favorite = [random.randint(1, 2000) for _ in range(12)]

        json_data = {
            "user_id": f"{user_id}_activeLogs",
            "player_id": f"{user_id}_player_id",
            "cctv_routes": cctv_routes,
            "cctv_favorite": cctv_favorite,
            "timestamp": current_timestamp
        }

        producer.send(topicName, json_data)

        # producer.flush()
        totalUserActive += 1
        print(f"[{current_timestamp}] ({totalUserActive}) user_active_log")

        delay = random.randint(1000, 1000) / 1000.0
        time.sleep(delay)

generate_user_active_log()

# while True:
#     # Define user_id
#     user_id = f"user-{random.randint(1, 100)}"

#     # Get the current timestamp
#     current_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

#     # Generate random CCTV location
#     cctv_location = f"CCTV-{random.randint(1, 15)}"

#     # Define JSON data with random values
#     json_data = {
#         "user_id": user_id,
#         "timestamp": current_timestamp,
#         "cctv_location": cctv_location
#     }
    
#     if(random.randint(0, 50) % 2 == 0):
#         local_data = []
#         local_data.append(json_data)
    
#     # Publish JSON data in the defined topic
#     producer.send(topicName, json_data)

#     # Flush and close the producer
#     producer.flush()

#     if(random.randint(0, 50) % 2 == 0):
#         if local_data and len(local_data) > 0:
#             producer.send(topicName, local_data[0])
#             producer.flush()
#             producer.send(topicName, local_data[0])
#             producer.flush()
#             producer.send(topicName, local_data[0])
#             producer.flush()

#     # Print message
#     print(f"[{current_timestamp}] Message Sent ")
#     # Generate random delay between 5000 to 10000 milliseconds
#     delay = random.randint(1000, 3000) / 1000.0
#     time.sleep(delay)
