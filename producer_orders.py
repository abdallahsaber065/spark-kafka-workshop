from kafka import KafkaProducer
import json
import time


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


orders = [
    {"customer": "Ali", "product": "Laptop", "price": 1200},
    {"customer": "Sara", "product": "Phone", "price": 800},
    {"customer": "Ali", "product": "Mouse", "price": 25},
    {"customer": "Mona", "product": "Laptop", "price": 1100},
    {"customer": "Sara", "product": "Keyboard", "price": 75},
    {"customer": "Omar", "product": "Phone", "price": 650},
]


for order in orders:
    producer.send("orders", value=order)
    print("Sent:", order)
    time.sleep(2)


producer.flush()
producer.close()