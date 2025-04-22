from kafka import KafkaConsumer
import json

# Create a Kafka consumer subscribed to the 'food-orders' topic
consumer = KafkaConsumer(
    'food-orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # start from beginning if no offset
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("📦 Waiting for new food orders...\n")

# Continuously listen for messages
for message in consumer:
    order = message.value
    print(order)
    print(f"🛎️ Order received: {order['user']} ordered {order['item']}")
