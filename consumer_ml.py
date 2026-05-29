"""
ML Inference Consumer — real-time fraud detection pipeline.

Stage 1: Consume raw-transactions
Stage 2: Compute real-time features (velocity, z-score, merchant risk score)
Stage 3: Run Isolation Forest anomaly detection
Stage 4: Route to flagged-transactions or approved-transactions

This demonstrates the core pattern of a streaming ML pipeline:
    raw events → feature engineering → model inference → downstream routing
"""

import json
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from sklearn.ensemble import IsolationForest

BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "raw-transactions"
FLAGGED_TOPIC = "flagged-transactions"
APPROVED_TOPIC = "approved-transactions"
CONSUMER_GROUP = "ml-inference-group"

MERCHANT_RISK_SCORES = {
    "low": 0.1,
    "medium": 0.4,
    "high": 0.9,
}

COUNTRY_RISK_SCORES = {
    "US": 0.0, "CA": 0.1, "GB": 0.1,
    "RU": 0.8, "NG": 0.8, "CN": 0.6,
}


class OnlineFeatureStore:
    """
    Lightweight in-memory feature store.

    In production this would be backed by Redis, Aerospike, or
    a managed feature store (Vertex AI Feature Store, SageMaker Feature Store).
    """

    def __init__(self, window_seconds: int = 60):
        self.window_seconds = window_seconds
        # user_id → deque of (timestamp, amount)
        self._txn_windows: dict[str, deque] = defaultdict(deque)
        # user_id → list of historical amounts for baseline
        self._user_history: dict[str, list] = defaultdict(list)

    def record(self, user_id: str, amount: float, ts: float):
        window = self._txn_windows[user_id]
        window.append((ts, amount))
        # evict events outside the rolling window
        cutoff = ts - self.window_seconds
        while window and window[0][0] < cutoff:
            window.popleft()
        # keep last 100 transactions for baseline stats
        history = self._user_history[user_id]
        history.append(amount)
        if len(history) > 100:
            history.pop(0)

    def velocity_count(self, user_id: str) -> int:
        """Number of transactions in the last window_seconds."""
        return len(self._txn_windows[user_id])

    def velocity_amount(self, user_id: str) -> float:
        """Total amount spent in the last window_seconds."""
        return sum(amt for _, amt in self._txn_windows[user_id])

    def z_score(self, user_id: str, amount: float) -> float:
        """How many std-devs is this amount from the user's historical mean?"""
        history = self._user_history[user_id]
        if len(history) < 3:
            return 0.0
        mean = np.mean(history)
        std = np.std(history) or 1.0
        return (amount - mean) / std


def build_feature_vector(txn: dict, store: OnlineFeatureStore) -> list[float]:
    """
    Converts a raw transaction into a numeric feature vector for the model.

    Features:
      [0] amount (log-scaled)
      [1] txn count in last 60s (velocity)
      [2] total amount in last 60s
      [3] z-score of amount vs. user history
      [4] merchant risk score
      [5] country risk score
    """
    user_id = txn["user_id"]
    amount = txn["amount"]

    return [
        np.log1p(amount),
        store.velocity_count(user_id),
        np.log1p(store.velocity_amount(user_id)),
        store.z_score(user_id, amount),
        MERCHANT_RISK_SCORES.get(txn.get("merchant_risk", "medium"), 0.4),
        COUNTRY_RISK_SCORES.get(txn.get("country", "US"), 0.3),
    ]


class StreamingFraudDetector:
    """
    Isolation Forest model that initialises with synthetic baseline data
    and can be updated online as real data arrives.
    """

    CONTAMINATION = 0.05  # expected 5% anomaly rate

    def __init__(self):
        self.model = IsolationForest(
            n_estimators=100,
            contamination=self.CONTAMINATION,
            random_state=42,
        )
        self._warm_up()
        self._prediction_count = 0

    def _warm_up(self):
        """Train on synthetic baseline data so the model is ready from message #1."""
        rng = np.random.RandomState(42)
        # 950 normal transactions
        normal = rng.randn(950, 6) * [0.5, 1, 0.5, 1, 0.1, 0.1] + [3.5, 2, 4, 0, 0.1, 0.0]
        # 50 anomalous transactions
        anomalous = rng.randn(50, 6) * [1, 2, 1, 2, 0.1, 0.1] + [9, 8, 8, 5, 0.9, 0.8]
        X = np.vstack([normal, anomalous])
        self.model.fit(X)
        print("  [MODEL] Isolation Forest trained on 1000 baseline samples")

    def score(self, features: list[float]) -> float:
        """
        Returns anomaly score in [0, 1].
        score >= 0.5 → flagged as suspicious.
        """
        X = np.array(features).reshape(1, -1)
        # decision_function returns negative scores for anomalies
        raw = self.model.decision_function(X)[0]
        # normalise to [0, 1] — higher = more anomalous
        score = 1.0 - (raw - self.model.offset_) / (
            -self.model.offset_ + 1e-9
        )
        score = float(np.clip(score, 0.0, 1.0))
        self._prediction_count += 1
        return score


def main():
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        max_poll_records=50,
    )

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks=1,
        linger_ms=10,
    )

    store = OnlineFeatureStore(window_seconds=60)
    detector = StreamingFraudDetector()

    print(f"ML Consumer started — listening on '{INPUT_TOPIC}'")
    print(f"Consumer group: {CONSUMER_GROUP}")
    print("-" * 70)

    stats = {"processed": 0, "flagged": 0, "approved": 0, "total_latency_ms": 0.0}

    try:
        for message in consumer:
            receive_time = time.time()
            txn = message.value
            user_id = txn["user_id"]
            amount = txn["amount"]

            # Parse event timestamp for end-to-end latency measurement
            event_ts = datetime.fromisoformat(txn["timestamp"]).timestamp()
            event_ts_float = event_ts

            # Stage 1: update feature store
            store.record(user_id, amount, receive_time)

            # Stage 2: build feature vector
            features = build_feature_vector(txn, store)

            # Stage 3: model inference
            anomaly_score = detector.score(features)

            # Stage 4: build enriched result payload
            result = {
                **txn,
                "anomaly_score": round(anomaly_score, 4),
                "features": {
                    "log_amount":       round(features[0], 3),
                    "velocity_count":   int(features[1]),
                    "velocity_amount":  round(features[2], 3),
                    "z_score":          round(features[3], 3),
                    "merchant_risk":    round(features[4], 3),
                    "country_risk":     round(features[5], 3),
                },
                "processed_at": datetime.now(timezone.utc).isoformat(),
            }

            latency_ms = (time.time() - receive_time) * 1000
            stats["total_latency_ms"] += latency_ms
            stats["processed"] += 1

            # Stage 5: route based on score
            if anomaly_score >= 0.5:
                stats["flagged"] += 1
                producer.send(FLAGGED_TOPIC, key=user_id, value=result)
                flag = "FLAGGED  ⚠"
            else:
                stats["approved"] += 1
                producer.send(APPROVED_TOPIC, key=user_id, value=result)
                flag = "approved  ✓"

            injected = " [INJECTED]" if txn.get("is_injected_anomaly") else ""
            print(
                f"[{flag}] user={user_id} amount=${amount:>10.2f} "
                f"score={anomaly_score:.3f} latency={latency_ms:.1f}ms{injected}"
            )

            # Print rolling stats every 20 messages
            if stats["processed"] % 20 == 0:
                avg_lat = stats["total_latency_ms"] / stats["processed"]
                flag_rate = stats["flagged"] / stats["processed"] * 100
                print(
                    f"\n  --- Stats: processed={stats['processed']} "
                    f"flagged={stats['flagged']} ({flag_rate:.1f}%) "
                    f"avg_latency={avg_lat:.1f}ms ---\n"
                )

    except KeyboardInterrupt:
        print(f"\nConsumer stopped.")
        if stats["processed"] > 0:
            avg_lat = stats["total_latency_ms"] / stats["processed"]
            print(f"  Total processed : {stats['processed']}")
            print(f"  Flagged          : {stats['flagged']} ({stats['flagged']/stats['processed']*100:.1f}%)")
            print(f"  Approved         : {stats['approved']}")
            print(f"  Avg latency      : {avg_lat:.1f}ms")
    finally:
        producer.flush()
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
