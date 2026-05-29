"""
Multi-LLM API Fan-out Demo — Kafka-based rate-limited worker pools.

Demonstrates Section 3.6 of the README:
  - Three priority topics: genai.llm.requests-high/medium/low
  - Two worker pools consuming in parallel (each simulates a different LLM provider)
  - Per-worker token-bucket rate limiter (configurable RPM per pool)
  - Dead-letter routing for requests that exhaust their retry budget
  - Live dashboard: queue depth, provider split, latency, drop rate

Run:
    python llm_multi_api.py

Set ANTHROPIC_API_KEY to get real Claude responses; otherwise runs in mock mode.
"""

import json
import os
import random
import threading
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP_SERVERS = "localhost:9092"
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

TOPICS = {
    "high":        "genai.llm.requests-high",
    "medium":      "genai.llm.requests-medium",
    "low":         "genai.llm.requests-low",
    "responses":   "genai.llm.responses",
    "dead_letter": "genai.llm.dead-letter",
}

SAMPLE_PROMPTS = [
    ("Summarise the latest trends in vector databases in 3 bullets."),
    ("Explain the difference between RAG and fine-tuning for LLMs."),
    ("Write a Python function that retries an HTTP call with exponential backoff."),
    ("What is the CAP theorem and how does it apply to Kafka?"),
    ("Draft a one-paragraph executive summary of Apache Kafka for a CTO."),
    ("How does Kafka's log compaction work?"),
    ("What are the trade-offs between Kafka Streams and Apache Flink?"),
    ("Explain token bucket rate limiting with a code example."),
    ("What is model drift and how do you detect it in production?"),
    ("Describe the RLHF process used to train ChatGPT."),
]


# ---------------------------------------------------------------------------
# Token-bucket rate limiter
# ---------------------------------------------------------------------------

class TokenBucket:
    """Thread-safe token bucket — enforces requests-per-minute limit."""

    def __init__(self, rpm: int):
        self._capacity = rpm
        self._tokens = float(rpm)
        self._refill_rate = rpm / 60.0   # tokens per second
        self._lock = threading.Lock()
        self._last_refill = time.monotonic()

    def acquire(self, timeout: float = 5.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return True
            time.sleep(0.05)
        return False   # timed out — caller should route to dead-letter

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._capacity, self._tokens + elapsed * self._refill_rate)
        self._last_refill = now

    @property
    def available(self) -> float:
        with self._lock:
            self._refill()
            return self._tokens


# ---------------------------------------------------------------------------
# Shared metrics (written by workers, read by dashboard)
# ---------------------------------------------------------------------------

class Metrics:
    def __init__(self):
        self._lock = threading.Lock()
        self.processed = defaultdict(int)      # worker_name → count
        self.dead_lettered = 0
        self.latencies: deque = deque(maxlen=200)
        self.provider_calls = defaultdict(int) # provider → count

    def record(self, worker: str, provider: str, latency_ms: float):
        with self._lock:
            self.processed[worker] += 1
            self.provider_calls[provider] += 1
            self.latencies.append(latency_ms)

    def record_dead_letter(self):
        with self._lock:
            self.dead_lettered += 1

    def snapshot(self) -> dict:
        with self._lock:
            total = sum(self.processed.values())
            avg_lat = sum(self.latencies) / len(self.latencies) if self.latencies else 0
            return {
                "total": total,
                "by_worker": dict(self.processed),
                "by_provider": dict(self.provider_calls),
                "dead_lettered": self.dead_lettered,
                "avg_latency_ms": round(avg_lat, 1),
            }


METRICS = Metrics()


# ---------------------------------------------------------------------------
# LLM call — real or mock
# ---------------------------------------------------------------------------

def call_llm(prompt: str, provider: str) -> tuple[str, float]:
    """Returns (response_text, latency_ms)."""
    start = time.time()

    if provider == "claude" and ANTHROPIC_API_KEY:
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",  # cheapest/fastest for demo
                max_tokens=150,
                messages=[{"role": "user", "content": prompt}],
            )
            text = msg.content[0].text
        except Exception as e:
            text = f"[API ERROR] {e}"
    else:
        # Mock: simulate variable latency per provider
        latency_sim = {"claude": 0.4, "gpt4o": 0.6, "gemini": 0.35, "fallback": 0.2}
        time.sleep(latency_sim.get(provider, 0.3) + random.uniform(0, 0.15))
        text = (
            f"[MOCK/{provider.upper()}] This is a simulated response to: "
            f"'{prompt[:50]}...' — set ANTHROPIC_API_KEY for real Claude responses."
        )

    latency_ms = (time.time() - start) * 1000
    return text, latency_ms


# ---------------------------------------------------------------------------
# Worker pool — one thread per worker instance
# ---------------------------------------------------------------------------

class LLMWorker(threading.Thread):

    def __init__(
        self,
        name: str,
        provider: str,
        rpm_limit: int,
        topic_list: list[str],
        group_id: str,
        producer: KafkaProducer,
        max_retries: int = 3,
        stop_event: threading.Event = None,
    ):
        super().__init__(daemon=True, name=name)
        self.worker_name = name
        self.provider = provider
        self.rate_limiter = TokenBucket(rpm=rpm_limit)
        self.topic_list = topic_list
        self.group_id = group_id
        self.producer = producer
        self.max_retries = max_retries
        self.stop_event = stop_event or threading.Event()

    def run(self):
        consumer = KafkaConsumer(
            *self.topic_list,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=self.group_id,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            key_deserializer=lambda b: b.decode("utf-8") if b else None,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            max_poll_records=5,
            consumer_timeout_ms=2000,
        )

        while not self.stop_event.is_set():
            for message in consumer:
                if self.stop_event.is_set():
                    break

                req = message.value
                retry_count = req.get("_retry_count", 0)

                # Try to acquire rate-limit token
                if not self.rate_limiter.acquire(timeout=2.0):
                    # Rate limit exhausted — dead-letter if max retries reached
                    if retry_count >= self.max_retries:
                        self._dead_letter(req, reason="rate_limit_exhausted")
                    else:
                        # Re-publish with incremented retry count (back-pressure)
                        req["_retry_count"] = retry_count + 1
                        topic = message.topic  # same priority tier
                        self.producer.send(topic, key=req["request_id"], value=req)
                    continue

                # Call LLM
                response_text, latency_ms = call_llm(req["prompt"], self.provider)

                response = {
                    "request_id": req["request_id"],
                    "session_id": req.get("session_id"),
                    "priority": req.get("priority"),
                    "prompt": req["prompt"],
                    "response": response_text,
                    "provider": self.provider,
                    "worker": self.worker_name,
                    "latency_ms": round(latency_ms, 1),
                    "retry_count": retry_count,
                    "responded_at": datetime.now(timezone.utc).isoformat(),
                }

                self.producer.send(
                    TOPICS["responses"],
                    key=req["request_id"],
                    value=response,
                )

                METRICS.record(self.worker_name, self.provider, latency_ms)

        consumer.close()

    def _dead_letter(self, req: dict, reason: str):
        dlq_event = {**req, "dead_letter_reason": reason,
                     "dead_lettered_at": datetime.now(timezone.utc).isoformat()}
        self.producer.send(TOPICS["dead_letter"], key=req["request_id"], value=dlq_event)
        METRICS.record_dead_letter()


# ---------------------------------------------------------------------------
# Producer — publishes requests to priority topics
# ---------------------------------------------------------------------------

def run_producer(producer: KafkaProducer, total: int = 20):
    priority_dist = [("high", 0.20), ("medium", 0.50), ("low", 0.30)]

    print(f"\n[PRODUCER] Sending {total} LLM requests across 3 priority tiers")
    print("-" * 65)

    for i in range(total):
        roll = random.random()
        cumulative = 0
        priority = "medium"
        for p, prob in priority_dist:
            cumulative += prob
            if roll < cumulative:
                priority = p
                break

        prompt = random.choice(SAMPLE_PROMPTS)
        req = {
            "request_id": str(uuid.uuid4()),
            "session_id": f"sess_{random.randint(1000, 9999)}",
            "user_tier": priority,
            "priority": priority,
            "prompt": prompt,
            "submitted_at": datetime.now(timezone.utc).isoformat(),
            "_retry_count": 0,
        }

        topic = TOPICS[priority]
        producer.send(topic, key=req["request_id"], value=req)
        print(f"  [{priority.upper():^6}] #{i+1:02d} → {prompt[:60]}")
        time.sleep(random.uniform(0.1, 0.4))

    producer.flush()
    print(f"\n[PRODUCER] Done — {total} requests published")


# ---------------------------------------------------------------------------
# Response consumer — prints what comes back from workers
# ---------------------------------------------------------------------------

def run_response_monitor(stop_event: threading.Event, max_messages: int = 20):
    consumer = KafkaConsumer(
        TOPICS["responses"],
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="response-monitor",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=8000,
    )

    print(f"\n[RESPONSE MONITOR] Listening on '{TOPICS['responses']}'")
    print("-" * 65)

    count = 0
    for message in consumer:
        if stop_event.is_set() or count >= max_messages:
            break
        r = message.value
        count += 1
        print(
            f"  [{r['priority'].upper():^6}] worker={r['worker']:20s} "
            f"latency={r['latency_ms']:>6.0f}ms  "
            f"{r['response'][:60]}..."
        )

    consumer.close()


# ---------------------------------------------------------------------------
# Live dashboard — prints rolling stats every 3 seconds
# ---------------------------------------------------------------------------

def run_dashboard(stop_event: threading.Event):
    time.sleep(2)  # let workers start first
    while not stop_event.is_set():
        time.sleep(3)
        snap = METRICS.snapshot()
        print(
            f"\n  ┌── LIVE METRICS ──────────────────────────────────────┐\n"
            f"  │  Total processed : {snap['total']:<5}  Dead-lettered: {snap['dead_lettered']:<5}│\n"
            f"  │  Avg latency     : {snap['avg_latency_ms']:<6.1f}ms                       │\n"
            f"  │  By worker       : {snap['by_worker']}  │\n"
            f"  │  By provider     : {snap['by_provider']}   │\n"
            f"  └──────────────────────────────────────────────────────┘"
        )


# ---------------------------------------------------------------------------
# Admin — create topics if they don't exist
# ---------------------------------------------------------------------------

def ensure_topics():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    new_topics = [
        NewTopic(TOPICS["high"],        num_partitions=6, replication_factor=1),
        NewTopic(TOPICS["medium"],      num_partitions=4, replication_factor=1),
        NewTopic(TOPICS["low"],         num_partitions=2, replication_factor=1),
        NewTopic(TOPICS["responses"],   num_partitions=6, replication_factor=1),
        NewTopic(TOPICS["dead_letter"], num_partitions=1, replication_factor=1),
    ]
    try:
        admin.create_topics(new_topics, validate_only=False)
        print("[ADMIN] Topics created")
    except TopicAlreadyExistsError:
        print("[ADMIN] Topics already exist")
    finally:
        admin.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 65)
    print("  MULTI-LLM API FAN-OUT DEMO")
    print(f"  Mode: {'Claude Haiku (real API)' if ANTHROPIC_API_KEY else 'MOCK (set ANTHROPIC_API_KEY for real calls)'}")
    print("=" * 65)

    ensure_topics()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks=1,
        linger_ms=5,
    )

    stop_event = threading.Event()

    # Worker Pool A — "Claude" pool, reads all 3 priority topics, 30 RPM
    worker_a = LLMWorker(
        name="claude-pool-a",
        provider="claude",
        rpm_limit=30,
        topic_list=[TOPICS["high"], TOPICS["medium"], TOPICS["low"]],
        group_id="llm-workers-claude",
        producer=producer,
        stop_event=stop_event,
    )

    # Worker Pool B — "Fallback" pool, also reads all 3, 60 RPM (higher throughput)
    worker_b = LLMWorker(
        name="fallback-pool-b",
        provider="fallback",
        rpm_limit=60,
        topic_list=[TOPICS["high"], TOPICS["medium"], TOPICS["low"]],
        group_id="llm-workers-fallback",
        producer=producer,
        stop_event=stop_event,
    )

    # Response monitor
    monitor_thread = threading.Thread(
        target=run_response_monitor,
        args=(stop_event, 20),
        daemon=True,
    )

    # Dashboard
    dashboard_thread = threading.Thread(
        target=run_dashboard,
        args=(stop_event,),
        daemon=True,
    )

    # Start workers and monitors
    worker_a.start()
    worker_b.start()
    monitor_thread.start()
    dashboard_thread.start()

    # Give workers time to join consumer groups before producer fires
    time.sleep(3)

    # Run producer
    run_producer(producer, total=20)

    # Wait for all responses to be processed (max 60s)
    monitor_thread.join(timeout=60)
    stop_event.set()

    producer.flush()
    producer.close()

    # Final stats
    snap = METRICS.snapshot()
    print("\n" + "=" * 65)
    print("  FINAL SUMMARY")
    print(f"  Total processed  : {snap['total']}")
    print(f"  Dead-lettered    : {snap['dead_lettered']}")
    print(f"  Avg latency      : {snap['avg_latency_ms']}ms")
    print(f"  By provider      : {snap['by_provider']}")
    print(f"  By worker        : {snap['by_worker']}")
    print("=" * 65)
    print("\nTopics used:")
    for k, v in TOPICS.items():
        print(f"  {k:12s} → {v}")


if __name__ == "__main__":
    main()
