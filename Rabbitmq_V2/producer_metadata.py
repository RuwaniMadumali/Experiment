import pika
import json
import sys
import time

# -------------------------------------------------
# MODEL CONFIGURATION
# -------------------------------------------------

model_files = {
    "model_10MB.pth": {
        "version": "v1",
        "model_size": 10
    },
    "model_50MB.pth": {
        "version": "v2",
        "model_size": 50
    },
    "model_100MB.pth": {
        "version": "v3",
        "model_size": 100
    }
}

# -------------------------------------------------
# COMMAND LINE ARGUMENTS
# -------------------------------------------------

if len(sys.argv) < 3:
    print("Usage:")
    print("python producer_metadata.py model_10MB.pth 1")
    print("python producer_metadata.py model_50MB.pth 3")
    print("python producer_metadata.py model_100MB.pth 5")
    sys.exit(1)

model_file = sys.argv[1]
device_count = int(sys.argv[2])

if model_file not in model_files:
    print(f"Unknown model: {model_file}")
    sys.exit(1)

# -------------------------------------------------
# MODEL INFORMATION
# -------------------------------------------------

model_info = model_files[model_file]

metadata = {
    "version": model_info["version"],
    "url": f"http://localhost:8000/{model_file}",
    "model_size": model_info["model_size"],
    "model_file": model_file
}

# -------------------------------------------------
# RABBITMQ CONNECTION
# -------------------------------------------------

connection = pika.BlockingConnection(
    pika.ConnectionParameters("localhost")
)

channel = connection.channel()

channel.queue_declare(queue="meta_queue")

# -------------------------------------------------
# SEND METADATA TO EACH DEVICE
# -------------------------------------------------

for i in range(device_count):

    channel.basic_publish(
        exchange="",
        routing_key="meta_queue",
        body=json.dumps(metadata)
    )

    print(
        f"Metadata sent for {model_file} "
        f"({i + 1}/{device_count})"
    )

    # Small gap between messages
    time.sleep(0.1)

print("\nMetadata distribution completed.")

print(
    f"Model: {model_file} | "
    f"Size: {model_info['model_size']} MB | "
    f"Devices: {device_count}"
)

connection.close()
