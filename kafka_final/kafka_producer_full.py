from kafka import KafkaProducer
import time
import sys
import os

# Get model path from argument
file_path = sys.argv[1]

producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Read file
with open(file_path, "rb") as f:
    data = f.read()

send_time = time.time()

# Send message with timestamp
producer.send(
    'model_topic',
    value=data,
    headers=[('send_time', str(send_time).encode())]
)

producer.flush()

print(f"Model sent: {file_path}")
print(f"Size: {len(data)} bytes")