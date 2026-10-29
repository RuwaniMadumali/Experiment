import requests
import json
import pika
import time
import os


def callback(ch, method, properties, body):

    try:

        # -------------------------------------------------
        # START LATENCY TIMER
        # -------------------------------------------------

        start_time = time.time()

        # -------------------------------------------------
        # RECEIVE METADATA
        # -------------------------------------------------

        data = json.loads(body.decode("utf-8"))

        version = data["version"]
        url = data["url"]
        model_size = data["model_size"]
        model_file = data["model_file"]

        print(
            f"Metadata received for {model_file}",
            flush=True
        )

        # -------------------------------------------------
        # DOWNLOAD MODEL
        # -------------------------------------------------

        response = requests.get(
            url,
            timeout=300
        )

        response.raise_for_status()

        # -------------------------------------------------
        # SAVE MODEL
        # -------------------------------------------------

        device_id = os.getpid()

        output_file = f"downloaded_model_{device_id}.pth"

        with open(output_file, "wb") as f:
            f.write(response.content)

        # -------------------------------------------------
        # CALCULATE LATENCY
        # -------------------------------------------------

        latency = time.time() - start_time

        latency_ms = latency * 1000

        # -------------------------------------------------
        # OUTPUT
        # -------------------------------------------------

        print(
            f"Device {device_id} received file. "
            f"Latency: {latency:.2f}s "
            f"({latency_ms:.2f} ms)",
            flush=True
        )

        print(
            f"Model: {model_file} | "
            f"Size: {model_size}MB | "
            f"Version: {version}",
            flush=True
        )

        # -------------------------------------------------
        # ACKNOWLEDGE MESSAGE
        # -------------------------------------------------

        ch.basic_ack(
            delivery_tag=method.delivery_tag
        )

        # -------------------------------------------------
        # STOP AFTER ONE MODEL
        # -------------------------------------------------

        ch.stop_consuming()

    except Exception as e:

        print(
            f"Error processing metadata: {e}",
            flush=True
        )

        ch.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=False
        )

        ch.stop_consuming()


# -------------------------------------------------
# RABBITMQ CONNECTION
# -------------------------------------------------

connection = pika.BlockingConnection(
    pika.ConnectionParameters("localhost")
)

channel = connection.channel()

channel.queue_declare(queue="meta_queue")

channel.basic_consume(
    queue="meta_queue",
    on_message_callback=callback,
    auto_ack=False
)

print(
    "Waiting for metadata...",
    flush=True
)

channel.start_consuming()

connection.close()

