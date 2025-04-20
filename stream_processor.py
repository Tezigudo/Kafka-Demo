from kafka import KafkaConsumer
import json
from collections import defaultdict

# Create a Kafka consumer
consumer = KafkaConsumer(
    'food-orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Simulate Kafka Streams: live aggregation
order_counts = defaultdict(int)

print("📊 Starting Kafka Stream Processor...\n")

for message in consumer:
    order = message.value
    user = order.get("user")
    order_counts[user] += 1
    print(f"🧾 {user} placed order for {order['item']} (Total Orders: {order_counts[user]})")
