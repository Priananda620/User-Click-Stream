import json
import random
from datetime import datetime
from kafka import KafkaProducer
import time

# Define server with port
bootstrap_servers = ['172.27.16.1:9092']

# Define topic name where the message will be published
topicName = 'clicks'

# Initialize producer variable
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
local_data = []



while True:
    # Define user_id
    user_id = f"user-{random.randint(1, 50)}"

    # Get the current timestamp
    current_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    # Generate random CCTV location
    cctv_location = f"CCTV-{random.randint(1, 50)}"

    # Define JSON data with random values
    json_data = {
        "user_id": user_id,
        "timestamp": current_timestamp,
        "cctv_location": cctv_location
    }
    # Publish JSON data in the defined topic
    producer.send(topicName, json_data)
    

    # if random.randint(2, 2) % 2 == 0:
    #     producer.send(topicName, json_data)
    #     producer.send(topicName, json_data)
    #     producer.send(topicName, json_data)
    #     producer.send(topicName, json_data)
    
    producer.flush()

    # Print message
    print(f"[{current_timestamp}] Message Sent ")
    # Generate random delay between 5000 to 10000 milliseconds
    delay = random.randint(10, 50) / 1000.0
    time.sleep(delay)


while True:
    # Define user_id
    user_id = f"user-{random.randint(1, 100)}"

    # Get the current timestamp
    current_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    # Generate random CCTV location
    cctv_location = f"CCTV-{random.randint(1, 15)}"

    # Define JSON data with random values
    json_data = {
        "user_id": user_id,
        "timestamp": current_timestamp,
        "cctv_location": cctv_location
    }
    
    if(random.randint(0, 50) % 2 == 0):
        local_data = []
        local_data.append(json_data)
    
    # Publish JSON data in the defined topic
    producer.send(topicName, json_data)

    # Flush and close the producer
    producer.flush()

    if(random.randint(0, 50) % 2 == 0):
        if local_data and len(local_data) > 0:
            producer.send(topicName, local_data[0])
            producer.flush()
            producer.send(topicName, local_data[0])
            producer.flush()
            producer.send(topicName, local_data[0])
            producer.flush()

    # Print message
    print(f"[{current_timestamp}] Message Sent ")
    # Generate random delay between 5000 to 10000 milliseconds
    delay = random.randint(1000, 3000) / 1000.0
    time.sleep(delay)
