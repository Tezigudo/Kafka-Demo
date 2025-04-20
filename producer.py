from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

orders = [
    {"user": "alice", "item": "Burger"},
    {"user": "bob", "item": "Pizza"},
    {"user": "alice", "item": "Sushi"},
    {"user": "alice", "item": "Pad Thai"},
    {"user": "bob", "item": "Salad"},
]

for order in orders:
    producer.send("food-orders", order)
    print("✅ Sent:", order)
    time.sleep(1)

producer.flush()
