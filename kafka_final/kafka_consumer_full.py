from kafka import KafkaConsumer
import time
import os
import csv

consumer = KafkaConsumer(
    'model_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id=None
)

DEVICE_ID = os.getpid()

print(f"Consumer {DEVICE_ID} started...")

for message in consumer:
    receive_time = time.time()

    # Extract send time from headers
    headers = dict(message.headers)
    send_time = float(headers[b'send_time'].decode())

    latency = receive_time - send_time

    # Save file
    filename = f"received_{DEVICE_ID}.bin"
    with open(filename, "wb") as f:
        f.write(message.value)

    print(f"Device {DEVICE_ID} received model")
    print(f"Latency: {latency:.3f} seconds")

    # Save results
    with open("results_kafka.csv", "a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            "Kafka",
            len(message.value),
            DEVICE_ID,
            latency
        ])