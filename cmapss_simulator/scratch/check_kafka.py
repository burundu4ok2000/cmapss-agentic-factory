from kafka import KafkaConsumer
import os

brokers = os.getenv("KAFKA_BROKERS", "localhost:9092")
topic = "engine_telemetry"

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[brokers],
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    consumer_timeout_ms=5000
)

count = 0
for message in consumer:
    count += 1
    if count % 1000 == 0:
        print(f"Read {count} messages...")
    if count >= 10000:
        break

print(f"Total read: {count}")
