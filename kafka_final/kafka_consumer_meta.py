from kafka import KafkaConsumer
import json
import requests
import time
import csv
import os

def process_model(model, start_time, device_id):
    url = model.get("url")

    if not url:
        print("Skipping invalid message:", model)
        return

    try:
        response = requests.get(url)

        filename = f"downloaded_{device_id}_{model.get('version','v1')}.bin"
        with open(filename, "wb") as f:
            f.write(response.content)

        latency = time.time() - start_time

        print(f"Device {device_id} downloaded model")
        print(f"Latency: {latency:.3f} seconds")

        with open("results_kafka.csv", "a", newline="") as file:
            writer = csv.writer(file)
            writer.writerow([
                "Kafka-Metadata",
                len(response.content),
                device_id,
                latency
            ])

    except Exception as e:
        print("Download failed:", e)


consumer = KafkaConsumer(
    'meta_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id="meta_group",
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

DEVICE_ID = os.getpid()

print(f"Metadata Consumer {DEVICE_ID} started...")

for message in consumer:
    start_time = time.time()
    data = message.value

    # Debug (optional but useful)
    print("Received:", type(data), data)

    # Case 1: list of models
    if isinstance(data, list):
        for model in data:
            process_model(model, start_time, DEVICE_ID)

    # Case 2: single model (correct case)
    elif isinstance(data, dict):
        process_model(data, start_time, DEVICE_ID)

    else:
        print("Unknown message format:", data)