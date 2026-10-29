import pika
import time

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

channel.queue_declare(queue='model_queue')

models = [
    "model_10MB.pth",
    "model_50MB.pth",
    "model_100MB.pth"
]

for file_path in models:

    with open(file_path, "rb") as f:
        data = f.read()

    send_time = time.time()

    channel.basic_publish(
        exchange='',
        routing_key='model_queue',
        body=data,
        properties=pika.BasicProperties(
            headers={
                'send_time': str(send_time),
                'file_name': file_path
            }
        )
    )

    print(f"{file_path} sent")

connection.close()