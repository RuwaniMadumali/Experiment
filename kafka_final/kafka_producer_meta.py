from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

metadata = [
    {
        "version": "v1",
        "url": "http://localhost:8000/model_10MB.pth",
        "checksum": "abc111"
    },
    {
        "version": "v2",
        "url": "http://localhost:8000/model_50MB.pth",
        "checksum": "abc222"
    },
    {
        "version": "v3",
        "url": "http://localhost:8000/model_100MB.pth",
        "checksum": "abc333"
    }
]
for model in metadata:
    producer.send(
        topic='meta_topic',
        key=model["version"].encode('utf-8'),   # helps partitioning + tracking
        value=model
    )
producer.flush()
producer.close()

print("All metadata sent successfully")