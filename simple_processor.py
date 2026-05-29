"""
Simple stream processor — reads from 'orders', routes to 'large-orders' or 'normal-orders'.

  orders  →  [processor]  →  large-orders   (amount > $500)
                          →  normal-orders   (amount <= $500)
"""
import json
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print("Processor running — routing orders (Ctrl+C to stop)\n")

try:
    for message in consumer:
        order = message.value

        if order["amount"] > 500:
            topic = "large-orders"
            label = ">>> LARGE"
        else:
            topic = "normal-orders"
            label = "    normal"

        producer.send(topic, value=order)
        producer.flush()

        print(f"[{label}]  {order['item']:<12}  ${order['amount']:>8.2f}  → {topic}")

except KeyboardInterrupt:
    print("\nStopped.")
finally:
    consumer.close()
    producer.close()
