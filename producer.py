from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

orders = [
    {"user": "alice", "items": ["Sushi"]},
    {"user": "bob", "items": ["Ramen", "Green Tea"]},
    {"user": "alice", "items": ["Tempura"]},
    {"user": "charlie", "items": ["Donburi"]},
    {"user": "bob", "items": ["Miso Soup"]}
]

print("🚀 Sending food orders with varied delays...\n")

for order in orders:
    delay = random.uniform(0.5, 2.5)  # 0.5 to 2.5 seconds delay between events
    producer.send("food-orders", order)
    print(f"✅ Sent: {order} (delay = {delay:.1f}s)")
    time.sleep(delay)

producer.flush()
