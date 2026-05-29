"""
Simple producer — sends one order per second to the 'orders' topic.
"""
import json
import random
import time
import uuid
from kafka import KafkaProducer

ITEMS = ["laptop", "headphones", "keyboard", "monitor", "mouse", "webcam", "desk", "chair"]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print("Sending orders... (Ctrl+C to stop)\n")

try:
    while True:
        order = {
            "order_id": str(uuid.uuid4())[:8],
            "item":     random.choice(ITEMS),
            "amount":   round(random.uniform(10, 1500), 2),
            "customer": f"customer_{random.randint(1, 10)}",
        }
        producer.send("orders", value=order)
        producer.flush()

        tag = "BIG  " if order["amount"] > 500 else "small"
        print(f"[{tag}]  {order['item']:<12}  ${order['amount']:>8.2f}  (order {order['order_id']})")
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopped.")
finally:
    producer.close()
