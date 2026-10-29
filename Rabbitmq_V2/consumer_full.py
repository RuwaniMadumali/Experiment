import pika
import time
import csv
import os

DEVICE_ID = os.getpid()  # unique device id

def callback(ch, method, properties, body):
    receive_time = time.time()

    with open(f"received_{DEVICE_ID}.pth", "wb") as f:
        f.write(body)

    latency = receive_time - float(properties.headers['send_time'])

    print(f"Device {DEVICE_ID} received file. Latency: {latency:.2f}s")

    # Save result
    with open("results.csv", "a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["RabbitMQ", len(body), DEVICE_ID, latency])

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='model_queue')

channel.basic_consume(
    queue='model_queue',
    on_message_callback=callback,
    auto_ack=True
)

channel.start_consuming()