from kafka import KafkaConsumer

# consumer = KafkaConsumer(
#     'clicks',                    # Topic to subscribe to
#     bootstrap_servers='172.27.16.1:9092',
#     group_id='my-group',          # Consumer group identifier
#     auto_offset_reset='earliest', # Start reading from the beginning of the topic if no offset is stored
#     enable_auto_commit=True,      # Automatically commit offsets
#     value_deserializer=lambda x: x.decode('utf-8')  # Deserialize message value as UTF-8 string
# )
consumer = KafkaConsumer('clicks', bootstrap_servers='172.27.16.1:9092')

# Start consuming messages
try:
    print("Listening for messages on the 'clicks' topic...")
    for message in consumer:
        print(f"Received message: {message}")

except KeyboardInterrupt:
    print("KeyboardInterrupt: Stopping the consumer.")
finally:
    # Close the consumer
    consumer.close()
