import json
import random
from datetime import datetime
from kafka import KafkaProducer
import time
import concurrent.futures
import multiprocessing
import config

def generate_user_active_log(start, end):
    # Define server with port
    bootstrap_servers = config.BOOTSTRAP_SERVER
    topicName = 'user_active_log'

    # Initialize producer variable
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    local_data = []
    total_user_active = 0

    for i in range(start, end):
        user_id = f"user-{i}"
        current_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        json_data = {
            "user_id": f"{user_id}_activeLogs",
            "timestamp": current_timestamp
        }

        producer.send(topicName, json_data)

        total_user_active += 1

        print(f"[{current_timestamp}] ({total_user_active}) user_active_log")

        delay = random.randint(1, 1) / 1000.0
        time.sleep(delay)

    producer.close()

if __name__ == '__main__':
    start_time = time.time()
    num_processes = 4
    total_users = 1000000

    # Divide the range of users among processes
    ranges = [(i * (total_users // num_processes), (i + 1) * (total_users // num_processes)) for i in range(num_processes)]

    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(generate_user_active_log, *zip(*ranges))

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total execution time: {elapsed_time} seconds")
