"""
Payment transaction producer — simulates a real-time payment service.

Publishes events to 'raw-transactions' topic. 10% of events are deliberately
anomalous (high amount + high velocity) to trigger the ML fraud detector.
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "raw-transactions"

MERCHANTS = {
    "grocery_store":   {"risk": "low",    "avg_amount": 65},
    "online_retail":   {"risk": "medium", "avg_amount": 120},
    "gas_station":     {"risk": "low",    "avg_amount": 45},
    "luxury_goods":    {"risk": "high",   "avg_amount": 2000},
    "crypto_exchange": {"risk": "high",   "avg_amount": 5000},
    "restaurant":      {"risk": "low",    "avg_amount": 35},
    "airline":         {"risk": "medium", "avg_amount": 450},
    "pharmacy":        {"risk": "low",    "avg_amount": 25},
}

USER_PROFILES = {f"user_{i:03d}": {"avg_monthly_spend": random.uniform(500, 5000)} for i in range(1, 21)}


def make_normal_transaction(user_id: str) -> dict:
    merchant = random.choice(list(MERCHANTS.keys()))
    base_amount = MERCHANTS[merchant]["avg_amount"]
    amount = round(random.gauss(base_amount, base_amount * 0.2), 2)
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "amount": max(1.0, amount),
        "merchant_category": merchant,
        "merchant_risk": MERCHANTS[merchant]["risk"],
        "currency": "USD",
        "country": random.choice(["US", "US", "US", "US", "CA", "GB"]),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "is_injected_anomaly": False,
    }


def make_anomalous_transaction(user_id: str) -> dict:
    """High-amount transaction on high-risk merchant — should trigger fraud alert."""
    merchant = random.choice(["luxury_goods", "crypto_exchange"])
    amount = round(random.uniform(8000, 25000), 2)
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "amount": amount,
        "merchant_category": merchant,
        "merchant_risk": MERCHANTS[merchant]["risk"],
        "currency": "USD",
        "country": random.choice(["RU", "NG", "CN", "US"]),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "is_injected_anomaly": True,
    }


def on_send_success(record_metadata):
    print(f"  [OK]    {record_metadata.topic}[{record_metadata.partition}] "
          f"offset={record_metadata.offset}")


def on_send_error(exc):
    print(f"  [ERROR] Delivery failed: {exc}")


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=5,
        batch_size=16384,
    )

    print(f"Producer connected to {BOOTSTRAP_SERVERS}")
    print(f"Publishing to topic: {TOPIC}")
    print("-" * 60)

    try:
        msg_count = 0
        while True:
            user_id = random.choice(list(USER_PROFILES.keys()))

            # 10% chance of anomalous transaction
            if random.random() < 0.10:
                txn = make_anomalous_transaction(user_id)
                label = "ANOMALY"
            else:
                txn = make_normal_transaction(user_id)
                label = "normal "

            print(f"[{label}] user={txn['user_id']} amount=${txn['amount']:>10.2f} "
                  f"merchant={txn['merchant_category']}")

            producer.send(
                TOPIC,
                key=user_id,               # partition by user_id for ordering
                value=txn,
            ).add_callback(on_send_success).add_errback(on_send_error)

            msg_count += 1

            # Flush every 10 messages
            if msg_count % 10 == 0:
                producer.flush()
                print(f"  --- Flushed {msg_count} messages ---")

            # Simulate realistic arrival rate: 1-3 transactions/second
            time.sleep(random.uniform(0.3, 1.0))

    except KeyboardInterrupt:
        print(f"\nProducer stopped. Total messages sent: {msg_count}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
