"""
GenAI Streaming Pipeline — Kafka + Groq API.

Demonstrates the full RAG + LLM pattern over Kafka:
  1. Producer: publishes customer support queries to 'llm-requests'
  2. Consumer: enriches each query with mock context (simulates vector-store retrieval)
  3. Calls Groq API for a grounded response
  4. Publishes response to 'llm-responses'
  5. Simulates user feedback → 'feedback-events' (RLHF loop)

Set GROQ_API_KEY in your environment before running:
  export GROQ_API_KEY=gsk_...

Without an API key the pipeline runs in MOCK mode — responses are
generated locally so you can observe the Kafka data flow regardless.
"""

import json
import os
import random
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer

load_dotenv(Path(__file__).parent.parent / ".env")

BOOTSTRAP_SERVERS = "localhost:9092"
REQUESTS_TOPIC = "llm-requests"
RESPONSES_TOPIC = "llm-responses"
FEEDBACK_TOPIC = "feedback-events"
CONSUMER_GROUP = "genai-llm-workers"

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = "llama-3.3-70b-versatile"

# ---------------------------------------------------------------------------
# Mock knowledge base — in production this would be a vector store query
# ---------------------------------------------------------------------------
KNOWLEDGE_BASE = {
    "billing": [
        "Billing cycles run on the 1st of each month.",
        "Refunds are processed within 5-7 business days.",
        "Annual plans receive a 20% discount vs monthly.",
    ],
    "technical": [
        "API rate limits: 1000 req/min on Standard plan, 5000 req/min on Pro.",
        "Webhook retries follow exponential backoff (1s, 2s, 4s, 8s, max 3 retries).",
        "SDK supports Python 3.9+, Node 18+, Java 17+.",
    ],
    "account": [
        "Passwords must be at least 12 characters with one special character.",
        "2FA can be enabled under Settings → Security.",
        "Account deletion requests take 30 days to process (GDPR compliance).",
    ],
    "general": [
        "Support hours: Monday–Friday 9am–6pm ET.",
        "Enterprise SLA guarantees 99.9% uptime and 4-hour response time.",
    ],
}

SAMPLE_QUERIES = [
    ("billing",   "Why was I charged twice this month?"),
    ("billing",   "How do I get a refund for my last invoice?"),
    ("billing",   "What is the difference between monthly and annual pricing?"),
    ("technical", "My webhooks are failing, what should I check?"),
    ("technical", "What Python version does your SDK require?"),
    ("technical", "How do I increase my API rate limit?"),
    ("account",   "How do I enable two-factor authentication?"),
    ("account",   "I forgot my password, how do I reset it?"),
    ("general",   "What are your support hours?"),
    ("general",   "Does your Enterprise plan have an SLA?"),
]


def retrieve_context(category: str) -> list[str]:
    """Simulates a vector store similarity search — returns top-K relevant docs."""
    docs = KNOWLEDGE_BASE.get(category, KNOWLEDGE_BASE["general"])
    return random.sample(docs, min(2, len(docs)))


def call_groq(query: str, context_docs: list[str]) -> str:
    """Calls Groq API. Falls back to mock if no API key is set."""
    if not GROQ_API_KEY:
        return (
            f"[MOCK RESPONSE] Based on the context: {context_docs[0] if context_docs else 'N/A'}. "
            f"Answer to '{query[:60]}...': Please refer to our documentation for detailed guidance."
        )

    try:
        from groq import Groq
        client = Groq(api_key=GROQ_API_KEY)

        context_text = "\n".join(f"- {doc}" for doc in context_docs)
        system_prompt = (
            "You are a helpful customer support agent. Answer questions using only "
            "the provided context. Be concise and friendly. If the context doesn't "
            "cover the question, say so and offer to escalate."
        )
        user_message = f"Context:\n{context_text}\n\nCustomer question: {query}"

        completion = client.chat.completions.create(
            model=GROQ_MODEL,
            max_tokens=300,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
        )
        return completion.choices[0].message.content

    except Exception as e:
        return f"[ERROR] LLM call failed: {e}"


# ---------------------------------------------------------------------------
# Producer — publishes customer queries
# ---------------------------------------------------------------------------

def run_query_producer(producer: KafkaProducer, num_queries: int = 15):
    print(f"\n[PRODUCER] Publishing {num_queries} customer queries to '{REQUESTS_TOPIC}'")
    print("-" * 60)

    for i in range(num_queries):
        category, query_text = random.choice(SAMPLE_QUERIES)
        request = {
            "request_id": str(uuid.uuid4()),
            "query": query_text,
            "category": category,
            "session_id": f"session_{random.randint(1000, 9999)}",
            "user_tier": random.choice(["free", "standard", "enterprise"]),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        producer.send(REQUESTS_TOPIC, key=request["request_id"], value=request)
        print(f"  [QUERY #{i+1:02d}] [{category}] {query_text[:60]}")
        time.sleep(random.uniform(0.3, 0.8))

    producer.flush()
    print(f"\n[PRODUCER] Done — published {num_queries} queries")


# ---------------------------------------------------------------------------
# Consumer / LLM Worker — enriches, calls LLM, publishes response
# ---------------------------------------------------------------------------

def run_llm_worker(producer: KafkaProducer, max_messages: int = 15):
    consumer = KafkaConsumer(
        REQUESTS_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=10000,  # stop after 10s of no messages
    )

    api_mode = "Groq API" if GROQ_API_KEY else "MOCK mode"
    print(f"\n[LLM WORKER] Started — consuming from '{REQUESTS_TOPIC}' ({api_mode})")
    print("-" * 60)

    processed = 0
    total_latency = 0.0

    for message in consumer:
        if processed >= max_messages:
            break

        request = message.value
        start = time.time()

        # Stage 1: retrieve context (simulates RAG)
        context_docs = retrieve_context(request["category"])

        # Stage 2: call LLM
        llm_response = call_groq(request["query"], context_docs)

        latency_ms = (time.time() - start) * 1000
        total_latency += latency_ms
        processed += 1

        # Stage 3: build enriched response
        response_event = {
            "request_id": request["request_id"],
            "session_id": request["session_id"],
            "query": request["query"],
            "category": request["category"],
            "user_tier": request["user_tier"],
            "context_docs_used": context_docs,
            "response": llm_response,
            "latency_ms": round(latency_ms, 1),
            "model": "llama-3.3-70b-versatile" if GROQ_API_KEY else "mock",
            "responded_at": datetime.now(timezone.utc).isoformat(),
        }

        producer.send(RESPONSES_TOPIC, key=request["request_id"], value=response_event)

        print(f"\n  [RESPONSE #{processed:02d}] request_id={request['request_id'][:8]}...")
        print(f"    Query    : {request['query'][:70]}")
        print(f"    Context  : {context_docs[0][:60]}...")
        print(f"    Response : {llm_response[:120]}...")
        print(f"    Latency  : {latency_ms:.0f}ms")

        # Stage 4: simulate user feedback (50% positive, 30% negative, 20% no feedback)
        feedback_roll = random.random()
        if feedback_roll < 0.5:
            feedback_type = "thumbs_up"
        elif feedback_roll < 0.8:
            feedback_type = "thumbs_down"
        else:
            feedback_type = None  # no feedback

        if feedback_type:
            feedback_event = {
                "request_id": request["request_id"],
                "feedback": feedback_type,
                "query": request["query"],
                "response": llm_response,
                "model": response_event["model"],
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            producer.send(FEEDBACK_TOPIC, key=request["request_id"], value=feedback_event)
            print(f"    Feedback : {feedback_type}")

    producer.flush()
    consumer.close()

    if processed > 0:
        print(f"\n[LLM WORKER] Done — processed={processed} avg_latency={total_latency/processed:.0f}ms")


# ---------------------------------------------------------------------------
# Feedback Analyzer — reads feedback-events and prints RLHF statistics
# ---------------------------------------------------------------------------

def run_feedback_analyzer():
    consumer = KafkaConsumer(
        FEEDBACK_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="feedback-analyzer",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=15000,
    )

    print(f"\n[FEEDBACK ANALYZER] Listening on '{FEEDBACK_TOPIC}'")
    print("-" * 60)

    thumbs_up = 0
    thumbs_down = 0
    bad_examples = []

    for message in consumer:
        fb = message.value
        if fb["feedback"] == "thumbs_up":
            thumbs_up += 1
        else:
            thumbs_down += 1
            bad_examples.append({
                "query": fb["query"],
                "response": fb["response"][:80],
            })
        print(f"  [{fb['feedback']:^11}] request={fb['request_id'][:8]}...")

    total = thumbs_up + thumbs_down
    if total > 0:
        print(f"\n  === RLHF Summary ===")
        print(f"  Total feedback       : {total}")
        print(f"  Positive (thumbs up) : {thumbs_up} ({thumbs_up/total*100:.0f}%)")
        print(f"  Negative (thumbs dn) : {thumbs_down} ({thumbs_down/total*100:.0f}%)")
        if bad_examples:
            print(f"\n  Negative examples (would go into fine-tuning dataset):")
            for ex in bad_examples[:3]:
                print(f"    Q: {ex['query'][:60]}")
                print(f"    A: {ex['response'][:60]}...")

    consumer.close()


# ---------------------------------------------------------------------------
# Main — orchestrates the full pipeline demo
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("  KAFKA + GenAI STREAMING PIPELINE DEMO")
    print(f"  Mode: {'Groq API (llama-3.3-70b-versatile)' if GROQ_API_KEY else 'MOCK (set GROQ_API_KEY for real LLM calls)'}")
    print("=" * 70)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks=1,
        linger_ms=5,
    )

    NUM_QUERIES = 10

    # Start feedback analyzer in background thread
    feedback_thread = threading.Thread(target=run_feedback_analyzer, daemon=True)
    feedback_thread.start()

    # Small delay to let consumer join the group
    time.sleep(1)

    # Start LLM worker in background thread
    worker_thread = threading.Thread(
        target=run_llm_worker, args=(producer, NUM_QUERIES), daemon=True
    )
    worker_thread.start()

    # Small delay so worker is ready before producer sends
    time.sleep(1)

    # Run producer in main thread
    run_query_producer(producer, num_queries=NUM_QUERIES)

    # Wait for worker and feedback analyzer to finish
    worker_thread.join(timeout=120)
    feedback_thread.join(timeout=30)

    producer.close()

    print("\n" + "=" * 70)
    print("  PIPELINE COMPLETE")
    print("  Topics used:")
    print(f"    {REQUESTS_TOPIC}  → raw customer queries")
    print(f"    {RESPONSES_TOPIC} → LLM-generated answers")
    print(f"    {FEEDBACK_TOPIC}  → user thumbs up/down (RLHF loop)")
    print("=" * 70)


if __name__ == "__main__":
    main()
