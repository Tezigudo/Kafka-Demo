from kafka import KafkaConsumer
import json
from collections import defaultdict

# Simulate stateful processing: track how many times each user orders
order_counts = defaultdict(int)

# Kafka consumer setup
consumer = KafkaConsumer(
    'food-orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("📊 Stream processor running...\n")

# Process stream in real time
for message in consumer:
    order = message.value
    user = order.get("user", "unknown")
    items = ", ".join(order.get("items", []))

    # Update state
    order_counts[user] += 1

    # Output insight
    print(f"🧾 {user} ordered: {items} | Total Orders: {order_counts[user]}")
