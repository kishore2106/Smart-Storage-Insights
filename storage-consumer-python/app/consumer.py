# app/consumer.py

from kafka import KafkaConsumer
import json
from app.db import MongoDB
from app.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_GROUP_ID

class StorageMetricConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',      # Start reading from the beginning
            enable_auto_commit=True            # Commit offsets automatically
        )
        self.db = MongoDB()

    def consume(self):
        print("Kafka Consumer started. Waiting for messages...")
        for message in self.consumer:
            metric = message.value
            print(f"Received message: {metric}")

            try:
                # Save to MongoDB
                self.db.save_metric(metric)
            except Exception as e:
                print(f"Error saving to MongoDB: {e}")
