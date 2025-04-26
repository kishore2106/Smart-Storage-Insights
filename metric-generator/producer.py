# Simulated storage metrics producer

from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

DEVICE_IDS = ['dev01', 'dev02', 'dev03']

while True:
    metric = {
        'device_id': random.choic(DEVICE_IDS),
        'timestamp': time.time(),
        'iops': random.randint(100, 10000),
        'latency': round(random.uniform(1.0, 15.0), 2),
        'capacity_used': round(random.uniform(100, 900), 2)
    }
    print(f"Sending: {metric}")
    producer.send('storage_metrics', metric)
    time.sleep(2)