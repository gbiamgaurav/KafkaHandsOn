# Apache Kafka in GenAI & Machine Learning — Hands-On Guide

> A practical, end-to-end walkthrough of how Apache Kafka powers real-time ML pipelines and Generative AI systems — with live demo code and cloud deployment commands for **AWS**, **GCP**, and **Azure**.

---

## Table of Contents

1. [Why Kafka in AI/ML?](#1-why-kafka-in-aiml)
2. [What Happens at 1 Million Users Without Kafka?](#2-what-happens-at-1-million-users-without-kafka)
3. [Core Kafka Concepts (Quick Refresh)](#3-core-kafka-concepts-quick-refresh)
4. [Kafka in Generative AI — Use Cases](#4-kafka-in-generative-ai--use-cases)
   - [Multi-LLM API Fan-out & Rate Limiting](#46-multi-llm-api-fan-out--rate-limiting)
5. [Kafka in Machine Learning — Deep Dive](#5-kafka-in-machine-learning--deep-dive)
6. [Architecture: Real-Time Fraud Detection ML Pipeline](#6-architecture-real-time-fraud-detection-ml-pipeline)
7. [Architecture: GenAI Streaming Pipeline (RAG + LLM)](#7-architecture-genai-streaming-pipeline-rag--llm)
8. [Live Demo — Local Setup](#8-live-demo--local-setup)
   - [Step 3 — Simple Order Routing Demo (Start Here)](#step-3--simple-order-routing-demo-start-here)
   - [Step 4 — ML Fraud Detection Demo](#step-4--run-the-ml-fraud-detection-demo)
   - [Step 5 — GenAI Pipeline Demo](#step-5--run-the-genai-pipeline-demo)
9. [Cloud Deployment](#9-cloud-deployment)
   - [AWS — Amazon MSK](#91-aws--amazon-msk)
   - [GCP — Managed Service for Apache Kafka](#92-gcp--managed-service-for-apache-kafka)
   - [Azure — Event Hubs (Kafka-Compatible)](#93-azure--event-hubs-kafka-compatible)
10. [Custom Kafka Cluster Design for GenAI/ML](#10-custom-kafka-cluster-design-for-genaiml)
    - [Topic Taxonomy & Naming](#101-topic-taxonomy--naming)
    - [Partition Count Guidelines](#102-partition-count-guidelines)
    - [Full Cluster Config Reference](#103-full-cluster-config-reference)
11. [Demo Code Walkthrough](#11-demo-code-walkthrough)
12. [Key Takeaways](#12-key-takeaways)
13. [References & Further Reading](#13-references--further-reading)

---

## 1. Why Kafka in AI/ML?

Traditional ML systems are **batch-oriented** — you collect data, train, deploy, and wait for the next scheduled run. The world moves faster than that.

Kafka bridges the gap between **live business events** and **intelligent systems** by providing:

| Challenge                              | How Kafka Solves It                                               |
|----------------------------------------|-------------------------------------------------------------------|
| Data arrives continuously              | Persistent, ordered event log — no data loss                     |
| Multiple ML models need the same data  | Fan-out with consumer groups — one stream, many readers          |
| Model needs fresh context              | Sub-10ms end-to-end latency from event to inference              |
| Training data must reflect reality     | Continuous feedback loops: predictions → outcomes → retraining   |
| LLM needs real-time company knowledge  | Kafka feeds vector stores (Pinecone, Weaviate) in real-time      |
| Audit trail for AI decisions           | Kafka log is immutable — every input/output is replayable        |

> **OpenAI** uses Apache Kafka + Flink as the backbone for ChatGPT's real-time feedback loop — ingesting user signals, safety events, and model outputs at millions of events per second.

---

## 2. What Happens at 1 Million Users Without Kafka?

Imagine you've built an AI-powered app — a customer support bot, a fraud detector, a personalisation engine. It works great in testing. Then it goes viral.

### Without Kafka — Direct API calls

```
User 1 ──────────────────────────────► LLM API
User 2 ──────────────────────────────► LLM API
User 3 ──────────────────────────────► LLM API
  ...
User 1,000,000 ──────────────────────► LLM API  ← API rate limit hit. Requests dropped.
```

Every user hits your backend directly. Your backend hits the LLM API directly. Here's what happens:

| Problem | What you see |
|---------|-------------|
| **LLM API rate limit** | Groq/OpenAI allows ~500 req/min. At 1M users, 99.9% of requests get a 429 error and are silently dropped |
| **Your server crashes** | 1M simultaneous open connections overwhelm memory and threads — the process dies |
| **No retry logic** | A failed request is just gone. The user sees an error. You have no record of it |
| **Thundering herd** | If your server restarts, all 1M users retry at the same moment and immediately crash it again |
| **No audit trail** | You have no log of what was asked, what the LLM answered, or why it failed |

### With Kafka — Requests become events in a queue

```
User 1 ──►
User 2 ──►
User 3 ──►  [Kafka Topic: llm-requests]  ──► LLM Worker (controlled rate)  ──► LLM API
  ...                 │
User 1M ──►           └── messages sit here safely, nothing is dropped
```

Kafka acts as a **shock absorber** between your users and the LLM:

| What Kafka does | Why it matters |
|----------------|----------------|
| **Buffers all 1M requests** | No request is dropped — they queue up and get processed in order |
| **Controls throughput** | Your LLM worker reads at exactly the rate the API allows (e.g. 500/min) — no more 429s |
| **Survives crashes** | If the LLM worker crashes, it restarts and picks up exactly where it left off — messages are still in the topic |
| **Scales horizontally** | Spin up 10 LLM workers to process 10× faster — each gets a share of the queue automatically |
| **Full audit log** | Every request and response is persisted in Kafka — replay any window for debugging or retraining |

### The numbers in perspective

| Scenario | Without Kafka | With Kafka |
|----------|--------------|------------|
| 1M requests hit at once | Server crashes, ~999,500 dropped | All 1M queued safely, processed over time |
| LLM API rate limit hit | Requests silently fail | Worker slows down, nothing lost |
| Worker process crashes | In-flight requests lost forever | Worker restarts, continues from last offset |
| Need to reprocess for a bug fix | Impossible — data is gone | Replay the topic from any point in time |

> **In short:** without Kafka, your system is only as strong as its weakest moment. With Kafka, spikes become queues, crashes become pauses, and nothing is ever lost.

---

## 3. Core Kafka Concepts (Quick Refresh)

```
Producer ──► Topic (Partitioned Log) ──► Consumer Group
                │
                ├── Partition 0: [event1] [event2] [event3] ...
                ├── Partition 1: [event4] [event5] [event6] ...
                └── Partition 2: [event7] [event8] [event9] ...
```

| Concept          | Role in AI/ML                                                  |
|------------------|----------------------------------------------------------------|
| **Topic**        | Channel per data type: `raw-transactions`, `model-predictions` |
| **Partition**    | Parallelism unit — scale producers/consumers independently      |
| **Offset**       | Cursor in the log — replay any window for retraining           |
| **Consumer Group** | Multiple ML services reading the same stream independently   |
| **Kafka Connect**| Zero-code connectors to databases, S3, BigQuery, Pinecone      |
| **Kafka Streams / Flink** | In-stream feature engineering before inference        |

---

## 4. Kafka in Generative AI — Use Cases

### 4.1 Real-Time RAG (Retrieval-Augmented Generation)

RAG makes LLMs answer questions using *your* data. Without Kafka, the vector store goes stale.

```
[New Document Ingested]
        │
        ▼
   Kafka Topic: document-events
        │
        ▼
   Flink / Kafka Streams
   (chunk + embed)
        │
        ▼
   Vector Store (Pinecone / Weaviate)   ◄── always fresh
        │
        ▼
   LLM API (Claude / GPT-4)   ◄── retrieves up-to-date context
        │
        ▼
   User Response
```

**Why Kafka?** Documents can arrive from 50 sources simultaneously (CRM, emails, Slack, PDFs). Kafka decouples ingestion from embedding — the LLM always answers with current knowledge.

---

### 4.2 LLM Inference at Scale (Streaming Output)

```
User Query ──► Kafka: llm-requests ──► Consumer Pool (N workers)
                                              │
                                    Each worker calls LLM API
                                              │
                                    Kafka: llm-responses ──► WebSocket / SSE
```

**Why Kafka?** Absorbs burst traffic (e.g., 10,000 concurrent users). LLM workers autoscale based on consumer lag. No request is dropped.

---

### 4.3 AI Agent Coordination (Multi-Agent Systems)

```
Orchestrator Agent
      │
      ├── Publishes task to: kafka/agent-tasks
      │
      ├── Agent A (Search)   subscribes → produces results to: kafka/agent-results
      ├── Agent B (Code Gen) subscribes → produces results to: kafka/agent-results
      └── Agent C (Critic)   subscribes → produces results to: kafka/agent-results
                                               │
                                         Aggregator reads all results
                                               │
                                         Final Answer
```

**Why Kafka?** Agents are stateless microservices. Kafka is the shared memory and coordination bus. Failures are retryable — agents can replay missed tasks.

---

### 4.4 Continuous LLM Fine-Tuning / RLHF Feedback

```
[User gives thumbs down on AI response]
        │
        ▼
  Kafka: feedback-events
        │
        ├── Stream Processor: filter low-quality examples
        ├── Feature store: enrich with session context
        └── S3/GCS: sink for fine-tuning dataset accumulation
                │
                ▼
         Nightly fine-tune job picks up new labeled examples
```

**Why Kafka?** Human feedback arrives continuously, not in batches. Kafka durably captures every signal; the fine-tuning pipeline consumes at its own pace.

---

### 4.5 Real-Time Content Moderation

```
[User Message] ──► Kafka: raw-messages
                          │
                    Flink: windowed dedup + PII detection
                          │
                    Moderation Model (DistilBERT / GPT-4)
                          │
                    ├── SAFE    ──► Kafka: approved-messages  ──► App
                    └── UNSAFE  ──► Kafka: flagged-messages   ──► Human Review Queue
```

### 4.6 Multi-LLM API Fan-out & Rate Limiting

This is one of the most **practical** GenAI patterns and the one Kafka solves uniquely well.

#### The Problem Without Kafka

```
10,000 users send queries simultaneously
         │
         ▼
  Application calls Claude API directly
         │
  429 Too Many Requests ← rate limit hit
         │
  Queries dropped / users get errors
```

Directly calling an LLM API from your application creates a **tight coupling** between user traffic and provider capacity. Any spike → errors.

#### The Kafka Solution: Decoupled Fan-out Worker Pool

```
10,000 user queries arrive
         │
         ▼
  Kafka Topic: llm-requests  (partitions=20, retention=10min)
  ┌──────────────────────────────────────────────────────────┐
  │  Partition 0-4   → Worker Pool A (Claude claude-sonnet-4-6)     │
  │  Partition 5-9   → Worker Pool B (GPT-4o)               │
  │  Partition 10-14 → Worker Pool C (Gemini 1.5 Pro)       │
  │  Partition 15-19 → Worker Pool D (Claude Haiku fallback) │
  └──────────────────────────────────────────────────────────┘
         │
  Each worker respects provider rate limits independently
  (token bucket per worker, configurable TPM/RPM)
         │
         ▼
  Kafka Topic: llm-responses  ──► WebSocket ──► User
         │
         ▼
  Kafka Topic: llm-dead-letter  ──► Retry queue / alert
```

**What Kafka adds here:**
| Problem                     | Kafka's answer                                           |
|-----------------------------|----------------------------------------------------------|
| Provider rate limit hit      | Messages queue up — zero drops, processed when capacity frees |
| Primary provider is down     | Route partition assignment to fallback worker pool       |
| A/B test two models          | Two consumer groups on same topic, compare response quality |
| Log every input/output       | Immutable log — full audit trail for compliance          |
| Retry failed LLM calls       | Dead letter topic + scheduled retry consumer             |
| Scale LLM workers up/down    | Add/remove consumers — Kafka auto-rebalances partitions  |

#### Real-Time Architecture with Per-Provider Rate Limiting

```
                     ┌─────────────────────────────────────────┐
                     │         llm-requests (20 partitions)    │
                     └────────────┬────────────────────────────┘
                                  │
              ┌───────────────────┼───────────────────────────┐
              ▼                   ▼                           ▼
   ┌──────────────────┐  ┌──────────────────┐  ┌─────────────────────┐
   │  Claude Worker   │  │   GPT-4o Worker  │  │  Gemini Worker      │
   │  (partitions 0-6)│  │  (partitions 7-13│  │  (partitions 14-19) │
   │                  │  │                  │  │                     │
   │  Token bucket:   │  │  Token bucket:   │  │  Token bucket:      │
   │  100k TPM        │  │  80k TPM         │  │  120k TPM           │
   │  Rate: 500 RPM   │  │  Rate: 200 RPM   │  │  Rate: 300 RPM      │
   │                  │  │                  │  │                     │
   │  If 429 → sleep  │  │  If 429 → sleep  │  │  If 429 → sleep     │
   │  + exponential   │  │  + backoff       │  │  + backoff          │
   │    backoff       │  │                  │  │                     │
   └────────┬─────────┘  └────────┬─────────┘  └──────────┬──────────┘
            │                     │                        │
            └─────────────────────┼────────────────────────┘
                                  ▼
                     ┌─────────────────────────────┐
                     │  llm-responses (10 partitions│
                     │  keyed by request_id)        │
                     └─────────────────────────────┘
                                  │
                        ┌─────────┴──────────┐
                        ▼                    ▼
               Response Router          llm-dead-letter
               (WebSocket /             (failed after 3
                SSE stream)              retries → alert)
```

#### Priority Queue Pattern (VIP users skip the line)

```
# Three separate topics — consumers drain HIGH first
llm-requests-high    (enterprise tier, partitions=10)
llm-requests-medium  (standard tier,   partitions=6)
llm-requests-low     (free tier,       partitions=4)

# Consumer subscribes to all three with priority ordering:
consumer.subscribe(['llm-requests-high', 'llm-requests-medium', 'llm-requests-low'])
# Kafka consumer poll() processes high-priority partitions first
# when max.poll.records is set appropriately
```

#### A/B Testing Two LLM Models on Live Traffic

```bash
# Same topic, two consumer groups — each runs a different model
# Group A reads all partitions with llama3-8b
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group llm-workers-llama3 --describe

# Group B reads same partitions with mixtral
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group llm-workers-mixtral --describe

# Both groups get every message independently
# Compare response quality, latency, cost in the responses topic
```

#### Live Demo: [`llm_multi_api.py`](llm_multi_api.py)

Set your Groq API key in the file before running:
```python
# llm_multi_api.py — line 31
GROQ_API_KEY = "gsk_your_key_here"
```

```bash
python3 llm_multi_api.py
```

This demo shows:
1. A producer publishing 20 LLM requests with mixed priorities (high/medium/low)
2. Two worker pools consuming in parallel — `llama3-8b-8192` (pool-a, 30 RPM) and `mixtral-8x7b-32768` (pool-b, 60 RPM) via Groq API
3. Per-worker token-bucket rate limiter enforcing realistic API limits
4. Dead letter routing for requests that exceed retry budget
5. Live metrics: requests/sec, avg latency, provider distribution, queue depth

---

## 5. Kafka in Machine Learning — Deep Dive

### 5.1 The Three ML Stages and Where Kafka Fits

```
┌────────────────────────────────────────────────────────────────────────┐
│                         ML LIFECYCLE                                   │
│                                                                        │
│  TRAINING          SERVING              MONITORING                    │
│                                                                        │
│  Historical    ←── Kafka log ───►  Real-Time        ◄── Kafka ──►    │
│  batch data        (replay)         Inference             Model Drift  │
│                                     Pipeline              Detection    │
└────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Online Feature Engineering with Kafka Streams

Traditional batch pipelines compute features hours after events arrive. Kafka Streams computes them in **milliseconds**:

```
Raw Event:
  { user_id: "u42", amount: 8500, merchant: "luxury_goods", ts: now }

Kafka Streams Topology:
  ┌─ Count transactions per user in last 60s window
  ├─ Compute velocity: amount change vs. 30-day average
  ├─ Join with user risk profile from state store
  └─ Enrich with merchant category from lookup table

Feature Vector (output, <5ms later):
  { user_id: "u42", txn_count_60s: 4, velocity_ratio: 8.2,
    risk_tier: "medium", merchant_risk: "high" }
        │
        ▼
  Fraud Detection Model ──► score: 0.94 ──► BLOCK
```

### 5.3 Model Drift Detection via Kafka

```
Kafka: prediction-log  ──► Flink: compute PSI / KL-divergence
                                  (feature distributions over time)
                                        │
                               drift_score > threshold?
                                        │
                               YES ──► Kafka: retrain-trigger
                                        │
                               NO  ──► continue monitoring
```

---

## 6. Architecture: Real-Time Fraud Detection ML Pipeline

This is the demo you'll run locally in [Section 8](#8-live-demo--local-setup).

```
┌─────────────────────────────────────────────────────────────────┐
│                    FRAUD DETECTION PIPELINE                     │
│                                                                 │
│  [Payment Service]                                              │
│       │                                                         │
│       ▼                                                         │
│  Kafka Topic: raw-transactions                                  │
│       │                                                         │
│       ├──────────────────────────────────┐                      │
│       ▼                                  ▼                      │
│  [Feature Consumer]              [Audit Consumer]               │
│  Enriches with:                  Logs all events                │
│  - 60s tx count                  to data lake                   │
│  - velocity ratio                                               │
│  - merchant risk                                                │
│       │                                                         │
│       ▼                                                         │
│  Kafka Topic: enriched-transactions                             │
│       │                                                         │
│       ▼                                                         │
│  [ML Inference Consumer]                                        │
│  Scikit-learn Isolation Forest                                  │
│  (anomaly detection model)                                      │
│       │                                                         │
│       ├── score < 0.5 ──► Kafka: approved-transactions          │
│       └── score ≥ 0.5 ──► Kafka: flagged-transactions           │
│                                   │                             │
│                             [Alert Service]                     │
│                             Notifies fraud team                 │
└─────────────────────────────────────────────────────────────────┘
```

**Topics used:**
| Topic                    | Producers         | Consumers                        |
|--------------------------|-------------------|----------------------------------|
| `raw-transactions`       | Payment service   | Feature service, Audit service   |
| `enriched-transactions`  | Feature service   | ML inference service             |
| `flagged-transactions`   | ML inference      | Alert service, human review      |
| `model-feedback`         | Human review      | Model retraining job             |

---

## 7. Architecture: GenAI Streaming Pipeline (RAG + LLM)

```
┌──────────────────────────────────────────────────────────────────────┐
│                      GenAI REAL-TIME RAG PIPELINE                    │
│                                                                      │
│  Data Sources                                                        │
│  [CRM] [Emails] [PDFs] [DB changes via Debezium]                    │
│       │            │         │                                       │
│       └────────────┴─────────┘                                       │
│                    │                                                 │
│                    ▼                                                 │
│         Kafka Topic: document-ingestion                              │
│                    │                                                 │
│                    ▼                                                 │
│         Embedding Worker                                             │
│         (text-embedding-3-small / sentence-transformers)            │
│                    │                                                 │
│                    ▼                                                 │
│         Vector Store (Pinecone / Weaviate / pgvector)               │
│                    │                                                 │
│  User Query ──►  Retriever ──► Top-K docs ──► LLM Prompt           │
│                                                │                    │
│                                     Kafka Topic: llm-requests       │
│                                                │                    │
│                                     LLM Worker Pool                 │
│                                     (Claude / GPT-4 / Gemini)       │
│                                                │                    │
│                                     Kafka Topic: llm-responses      │
│                                                │                    │
│                                          User ◄─┘                   │
│                                                                      │
│         Feedback ──► Kafka: feedback-events ──► Fine-tune dataset   │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 8. Live Demo — Local Setup

### Prerequisites

- Docker & Docker Compose
- Python 3.10+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

```bash
uv pip install -r requirements.txt --python .venv/bin/python3
# or: pip install -r requirements.txt
```

### Step 1 — Start Kafka locally

```bash
docker compose up -d
```

This starts:
- **Kafka** (KRaft mode, no ZooKeeper) on `localhost:9092`
- **Kafka UI** at `http://localhost:8080` — visual topic browser

### Step 2 — Create Topics

```bash
# Create topics used by the demo scripts
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic raw-transactions \
  --partitions 3 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic flagged-transactions \
  --partitions 1 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic approved-transactions \
  --partitions 1 --replication-factor 1

# List all topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

### Step 3 — Simple Order Routing Demo (Start Here)

This is the easiest way to see Kafka in action. One producer sends orders, one processor reads them and routes them to different topics based on the amount.

```
simple_producer.py  →  [orders]  →  simple_processor.py  →  [large-orders]  (amount > $500)
                                                          →  [normal-orders] (amount ≤ $500)
```

**Create the topics first:**
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic orders --partitions 1 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic large-orders --partitions 1 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic normal-orders --partitions 1 --replication-factor 1
```

Open **3 terminals** (activate venv in each: `source .venv/bin/activate`):

**Terminal 1 — Start the processor first (it must be listening before orders arrive):**
```bash
python3 simple_processor.py
```

**Terminal 2 — Start the producer:**
```bash
python3 simple_producer.py
```

**Terminal 3 — Watch large orders in real time:**
```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic large-orders \
  --from-beginning
```

You will see Terminal 1 printing every routed order, and Terminal 3 showing only the large ones as they arrive.

> **Key concepts at work:** Producer writes to a topic → topic holds the event → consumer reads it and writes to another topic. That's the entire Kafka stream processing pattern.

---

### Step 4 — Run the ML Fraud Detection Demo

Open **3 terminals**:

**Terminal 1 — Start ML Consumer (inference engine):**
```bash
python3 consumer_ml.py
```

**Terminal 2 — Start Producer (simulate payment events):**
```bash
python3 producer.py
```

**Terminal 3 — Watch flagged transactions:**
```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic flagged-transactions \
  --from-beginning
```

### Step 5 — Run the GenAI Pipeline Demo

**What is this doing?**

Think of it as a live customer support system with three moving parts talking to each other through Kafka:

```
[Producer]  →  llm-requests  →  [LLM Worker]  →  llm-responses
                                      ↓
                                  Groq API
                                      ↓
                               feedback-events  →  [Feedback Analyzer]
```

1. **Producer** pretends to be a website sending customer questions (e.g. "How do I get a refund?") into Kafka — one message per question
2. **LLM Worker** picks up each question, pulls some relevant docs from a knowledge base (this is the RAG step), calls the Groq LLM with that context, and puts the answer back into Kafka
3. **Feedback Analyzer** watches for thumbs up / thumbs down signals and prints a summary — in a real system this data would feed back into model fine-tuning (RLHF)

The key point: **none of these three parts talk to each other directly**. They only read and write Kafka topics. This means you could swap out the LLM, scale the worker to 10 instances, or add a new consumer that logs to a database — all without touching the other parts.

**Setup:** add your Groq API key to the `.env` file in the project root:
```
GROQ_API_KEY=gsk_...
```

**Run:**
```bash
python3 genai_pipeline.py
```

### Step 6 — Monitor in Kafka UI

Open `http://localhost:8080` in your browser.

**For the GenAI pipeline, look at:**
- **Topics → `llm-responses` → Messages** — click any message to see the full JSON: question, context docs used, LLM answer, and latency
- **Topics → `feedback-events` → Messages** — see which answers got thumbs down (fine-tuning candidates)
- **Consumer Groups** — shows `genai-llm-workers` and how many messages it has processed

**For the order routing demo:**
- **Topics → `large-orders` → Messages** — all orders over $500 routed here in real time

Every time you re-run a script, new messages append to the topics — Kafka never overwrites, it keeps the full history.

### Teardown

```bash
docker compose down -v
```

---

## 9. Cloud Deployment

### 8.1 AWS — Amazon MSK

Amazon MSK is a fully managed Kafka service. Supports Kafka 4.1, serverless tier, and deep IAM integration.

#### Install AWS CLI & MSK prerequisites

```bash
# Install AWS CLI (macOS)
brew install awscli

# Configure credentials
aws configure
# AWS Access Key ID: <your-key>
# AWS Secret Access Key: <your-secret>
# Default region: us-east-1
# Output format: json
```

#### Create MSK Cluster (Provisioned)

```bash
# Create a VPC security group first (allow port 9092, 9094)
aws ec2 create-security-group \
  --group-name kafka-sg \
  --description "Kafka MSK security group" \
  --vpc-id vpc-xxxxxxxx

aws ec2 authorize-security-group-ingress \
  --group-id sg-xxxxxxxx \
  --protocol tcp \
  --port 9092 \
  --cidr 10.0.0.0/8

# Create the MSK cluster
aws kafka create-cluster \
  --cluster-name "genai-kafka-cluster" \
  --kafka-version "3.7.x" \
  --number-of-broker-nodes 3 \
  --broker-node-group-info '{
    "InstanceType": "kafka.m5.large",
    "ClientSubnets": ["subnet-aaa", "subnet-bbb", "subnet-ccc"],
    "SecurityGroups": ["sg-xxxxxxxx"],
    "StorageInfo": {
      "EbsStorageInfo": { "VolumeSize": 100 }
    }
  }' \
  --encryption-info '{
    "EncryptionInTransit": {
      "ClientBroker": "TLS",
      "InCluster": true
    }
  }' \
  --enhanced-monitoring "PER_TOPIC_PER_BROKER"
```

#### Create MSK Serverless Cluster (Recommended for GenAI workloads)

```bash
aws kafka create-cluster-v2 \
  --cluster-name "genai-kafka-serverless" \
  --serverless '{
    "VpcConfigs": [
      {
        "SubnetIds": ["subnet-aaa", "subnet-bbb"],
        "SecurityGroupIds": ["sg-xxxxxxxx"]
      }
    ],
    "ClientAuthentication": {
      "Sasl": { "Iam": { "Enabled": true } }
    }
  }'
```

#### Get Bootstrap Brokers

```bash
# Get cluster ARN
CLUSTER_ARN=$(aws kafka list-clusters \
  --query 'ClusterInfoList[?ClusterName==`genai-kafka-cluster`].ClusterArn' \
  --output text)

# Get bootstrap broker endpoints
aws kafka get-bootstrap-brokers \
  --cluster-arn $CLUSTER_ARN \
  --query 'BootstrapBrokerStringTls'
```

#### Create Topics via AWS CLI

```bash
# MSK doesn't have a direct CLI for topic creation — use kafka-topics.sh via a bastion
# From an EC2 instance in the same VPC:
kafka-topics.sh \
  --bootstrap-server <bootstrap-broker>:9094 \
  --command-config client.properties \
  --create --topic raw-transactions \
  --partitions 6 \
  --replication-factor 3
```

#### Connect Python App to MSK (IAM Auth)

```bash
# Install MSK IAM auth library
pip install aws-msk-iam-sasl-signer-python

# client.properties for kafka CLI tools
cat > client.properties << EOF
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
EOF
```

#### MSK + Lambda for Serverless ML Inference

```bash
# Create Lambda trigger from MSK topic (serverless ML inference)
aws lambda create-event-source-mapping \
  --function-name ml-inference-function \
  --event-source-arn $CLUSTER_ARN \
  --topics raw-transactions \
  --starting-position LATEST \
  --batch-size 100 \
  --maximum-batching-window-in-seconds 5
```

#### MSK + SageMaker (Real-Time ML)

```bash
# Create SageMaker endpoint for real-time inference
aws sagemaker create-endpoint \
  --endpoint-name fraud-detection-endpoint \
  --endpoint-config-name fraud-detection-config

# The MSK consumer calls this endpoint for each batch
aws sagemaker-runtime invoke-endpoint \
  --endpoint-name fraud-detection-endpoint \
  --content-type application/json \
  --body '{"instances": [{"amount": 8500, "velocity": 8.2}]}' \
  output.json
```

#### Monitor MSK Consumer Lag

```bash
# Monitor consumer group lag (critical for ML pipelines — detect if model is falling behind)
aws kafka describe-cluster \
  --cluster-arn $CLUSTER_ARN

# CloudWatch metric for consumer lag
aws cloudwatch get-metric-statistics \
  --namespace AWS/Kafka \
  --metric-name EstimatedMaxTimeLag \
  --dimensions Name=ClusterName,Value=genai-kafka-cluster \
             Name=ConsumerGroup,Value=ml-inference-group \
             Name=Topic,Value=raw-transactions \
  --start-time 2026-01-01T00:00:00Z \
  --end-time 2026-01-01T01:00:00Z \
  --period 60 \
  --statistics Maximum
```

---

### 8.2 GCP — Managed Service for Apache Kafka

Google Cloud's Managed Service for Apache Kafka (MSK equivalent on GCP) is GA since 2024. Native integration with BigQuery, Dataflow, and Vertex AI.

#### Install gcloud CLI

```bash
# macOS
brew install --cask google-cloud-sdk

# Authenticate
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
gcloud config set compute/region us-central1
```

#### Enable Required APIs

```bash
gcloud services enable \
  managedkafka.googleapis.com \
  aiplatform.googleapis.com \
  dataflow.googleapis.com \
  bigquery.googleapis.com
```

#### Create Kafka Cluster

```bash
# Create a Managed Kafka cluster
gcloud managed-kafka clusters create genai-kafka-cluster \
  --location=us-central1 \
  --cpu=3 \
  --memory=3GiB \
  --subnets=projects/YOUR_PROJECT_ID/regions/us-central1/subnetworks/default \
  --labels=env=production,usecase=genai

# Verify cluster is running
gcloud managed-kafka clusters describe genai-kafka-cluster \
  --location=us-central1
```

#### Create Topics

```bash
# Create topic for raw ML events
gcloud managed-kafka topics create raw-transactions \
  --cluster=genai-kafka-cluster \
  --location=us-central1 \
  --partitions=6 \
  --replication-factor=3 \
  --configs=retention.ms=604800000  # 7 days

# Create topic for enriched features
gcloud managed-kafka topics create enriched-transactions \
  --cluster=genai-kafka-cluster \
  --location=us-central1 \
  --partitions=6 \
  --replication-factor=3

# Create GenAI topics
gcloud managed-kafka topics create llm-requests \
  --cluster=genai-kafka-cluster \
  --location=us-central1 \
  --partitions=10 \
  --replication-factor=3

gcloud managed-kafka topics create document-ingestion \
  --cluster=genai-kafka-cluster \
  --location=us-central1 \
  --partitions=4 \
  --replication-factor=3

# List topics
gcloud managed-kafka topics list \
  --cluster=genai-kafka-cluster \
  --location=us-central1
```

#### Get Bootstrap Endpoint

```bash
# Get bootstrap servers for your application
gcloud managed-kafka clusters describe genai-kafka-cluster \
  --location=us-central1 \
  --format="value(bootstrapAddress)"
```

#### Kafka Connect: Sink to BigQuery (for ML training data)

```bash
# Create a Kafka Connect BigQuery sink connector
gcloud managed-kafka connect-clusters create bq-sink-connector \
  --kafka-cluster=genai-kafka-cluster \
  --location=us-central1 \
  --subnet=projects/YOUR_PROJECT_ID/regions/us-central1/subnetworks/default

# Deploy the BigQuery sink
cat > bigquery-sink.json << EOF
{
  "name": "bigquery-sink",
  "config": {
    "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
    "topics": "raw-transactions,enriched-transactions",
    "project": "YOUR_PROJECT_ID",
    "datasets": ".*=ml_training_data",
    "autoCreateTables": "true",
    "sanitizeTopics": "true"
  }
}
EOF
```

#### Dataflow Pipeline: Kafka → Vertex AI (Real-Time Inference)

```bash
# Deploy a Dataflow job that reads from Kafka and calls Vertex AI
gcloud dataflow flex-template run kafka-ml-pipeline \
  --project=YOUR_PROJECT_ID \
  --region=us-central1 \
  --template-file-gcs-location=gs://dataflow-templates-us-central1/latest/flex/Kafka_to_BigQuery \
  --parameters \
    bootstrapServers=$(gcloud managed-kafka clusters describe genai-kafka-cluster \
      --location=us-central1 --format="value(bootstrapAddress)"),\
    inputTopics=raw-transactions,\
    outputTableSpec=YOUR_PROJECT_ID:ml_training_data.predictions,\
    outputDeadletterTable=YOUR_PROJECT_ID:ml_training_data.dead_letter
```

#### Vertex AI Online Prediction (called from Kafka consumer)

```bash
# Deploy a scikit-learn model to Vertex AI
gcloud ai models upload \
  --region=us-central1 \
  --display-name=fraud-detection-model \
  --container-image-uri=us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-3:latest \
  --artifact-uri=gs://YOUR_BUCKET/model/

# Create endpoint
gcloud ai endpoints create \
  --region=us-central1 \
  --display-name=fraud-detection-endpoint

# Deploy model to endpoint
gcloud ai endpoints deploy-model ENDPOINT_ID \
  --region=us-central1 \
  --model=MODEL_ID \
  --display-name=fraud-detection \
  --machine-type=n1-standard-4 \
  --min-replica-count=1 \
  --max-replica-count=10
```

#### Pub/Sub Bridge (for hybrid architectures)

```bash
# Pub/Sub ↔ Managed Kafka bridge using Kafka Connect
# Source: Pub/Sub → Kafka
cat > pubsub-source.json << EOF
{
  "name": "pubsub-kafka-source",
  "config": {
    "connector.class": "com.google.pubsub.kafka.source.CloudPubSubSourceConnector",
    "kafka.topic": "raw-transactions",
    "cps.project": "YOUR_PROJECT_ID",
    "cps.subscription": "ml-events-sub"
  }
}
EOF
```

#### Monitor Cluster

```bash
# View consumer group lag
gcloud managed-kafka consumer-groups describe ml-inference-group \
  --cluster=genai-kafka-cluster \
  --location=us-central1

# List all consumer groups
gcloud managed-kafka consumer-groups list \
  --cluster=genai-kafka-cluster \
  --location=us-central1
```

---

### 8.3 Azure — Event Hubs (Kafka-Compatible)

Azure Event Hubs provides a fully managed Kafka endpoint. Your existing Kafka code works with **zero code changes** — just swap the broker address.

#### Install Azure CLI

```bash
# macOS
brew install azure-cli

# Login
az login
az account set --subscription "YOUR_SUBSCRIPTION_ID"
```

#### Create Resource Group and Event Hubs Namespace

```bash
# Create resource group
az group create \
  --name rg-genai-kafka \
  --location eastus

# Create Event Hubs namespace with Kafka enabled
# Standard tier minimum for Kafka support
az eventhubs namespace create \
  --resource-group rg-genai-kafka \
  --name genai-kafka-ns \
  --location eastus \
  --sku Standard \
  --enable-kafka true \
  --enable-auto-inflate true \
  --maximum-throughput-units 20 \
  --tags Environment=Production UseCase=GenAI
```

#### Create Event Hubs (= Kafka Topics)

```bash
# Each Event Hub = one Kafka topic
az eventhubs eventhub create \
  --resource-group rg-genai-kafka \
  --namespace-name genai-kafka-ns \
  --name raw-transactions \
  --partition-count 6 \
  --message-retention 7

az eventhubs eventhub create \
  --resource-group rg-genai-kafka \
  --namespace-name genai-kafka-ns \
  --name enriched-transactions \
  --partition-count 6 \
  --message-retention 7

az eventhubs eventhub create \
  --resource-group rg-genai-kafka \
  --namespace-name genai-kafka-ns \
  --name llm-requests \
  --partition-count 10 \
  --message-retention 3

az eventhubs eventhub create \
  --resource-group rg-genai-kafka \
  --namespace-name genai-kafka-ns \
  --name flagged-transactions \
  --partition-count 2 \
  --message-retention 7

# List all event hubs
az eventhubs eventhub list \
  --resource-group rg-genai-kafka \
  --namespace-name genai-kafka-ns \
  --output table
```

#### Get Connection String (= Kafka Bootstrap Server)

```bash
# Get namespace connection string
az eventhubs namespace authorization-rule keys list \
  --resource-group rg-genai-kafka \
  --namespace-name genai-kafka-ns \
  --name RootManageSharedAccessKey \
  --query primaryConnectionString \
  --output tsv

# Create a scoped policy (recommended for production)
az eventhubs namespace authorization-rule create \
  --resource-group rg-genai-kafka \
  --namespace-name genai-kafka-ns \
  --name ml-pipeline-policy \
  --rights Listen Send

# Kafka bootstrap server format:
# genai-kafka-ns.servicebus.windows.net:9093
```

#### Configure Kafka Client for Event Hubs

```bash
# Create kafka client config (SASL_SSL with OAuth token)
cat > eventhubs-kafka.properties << EOF
bootstrap.servers=genai-kafka-ns.servicebus.windows.net:9093
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="\$ConnectionString" \
  password="<your-connection-string>";
EOF

# Test with kafka console producer
kafka-console-producer.sh \
  --bootstrap-server genai-kafka-ns.servicebus.windows.net:9093 \
  --producer.config eventhubs-kafka.properties \
  --topic raw-transactions
```

#### Create Consumer Group

```bash
# Consumer groups = Event Hubs consumer groups
az eventhubs eventhub consumer-group create \
  --resource-group rg-genai-kafka \
  --namespace-name genai-kafka-ns \
  --eventhub-name raw-transactions \
  --name ml-inference-group

az eventhubs eventhub consumer-group create \
  --resource-group rg-genai-kafka \
  --namespace-name genai-kafka-ns \
  --eventhub-name raw-transactions \
  --name audit-group
```

#### Azure Event Hubs + Azure ML (Real-Time Scoring)

```bash
# Create Azure ML workspace
az ml workspace create \
  --resource-group rg-genai-kafka \
  --name genai-ml-workspace

# Deploy managed online endpoint for real-time scoring
az ml online-endpoint create \
  --resource-group rg-genai-kafka \
  --workspace-name genai-ml-workspace \
  --name fraud-detection-endpoint \
  --auth-mode key

# Deploy model to endpoint
az ml online-deployment create \
  --resource-group rg-genai-kafka \
  --workspace-name genai-ml-workspace \
  --endpoint-name fraud-detection-endpoint \
  --name blue \
  --model azureml:fraud-model:1 \
  --instance-type Standard_DS3_v2 \
  --instance-count 2
```

#### Azure Event Hubs + Azure OpenAI (GenAI Pipeline)

```bash
# Create Azure OpenAI resource
az cognitiveservices account create \
  --resource-group rg-genai-kafka \
  --name genai-openai-account \
  --kind OpenAI \
  --sku S0 \
  --location eastus

# Deploy GPT-4o model
az cognitiveservices account deployment create \
  --resource-group rg-genai-kafka \
  --name genai-openai-account \
  --deployment-name gpt-4o \
  --model-name gpt-4o \
  --model-version "2024-11-20" \
  --model-format OpenAI \
  --sku-capacity 10 \
  --sku-name Standard

# Get endpoint and key
az cognitiveservices account show \
  --resource-group rg-genai-kafka \
  --name genai-openai-account \
  --query properties.endpoint

az cognitiveservices account keys list \
  --resource-group rg-genai-kafka \
  --name genai-openai-account
```

#### Azure Functions: Kafka Trigger for Serverless ML

```bash
# Create Function App
az functionapp create \
  --resource-group rg-genai-kafka \
  --name ml-kafka-processor \
  --storage-account YOUR_STORAGE_ACCOUNT \
  --consumption-plan-location eastus \
  --runtime python \
  --runtime-version 3.11 \
  --functions-version 4

# Set Event Hubs connection string
az functionapp config appsettings set \
  --resource-group rg-genai-kafka \
  --name ml-kafka-processor \
  --settings \
    EventHubConnectionString="<connection-string>" \
    AZURE_OPENAI_ENDPOINT="https://genai-openai-account.openai.azure.com/" \
    AZURE_OPENAI_KEY="<key>"
```

#### Monitor Event Hubs Consumer Lag

```bash
# Check namespace metrics
az monitor metrics list \
  --resource /subscriptions/SUB_ID/resourceGroups/rg-genai-kafka/providers/Microsoft.EventHub/namespaces/genai-kafka-ns \
  --metric IncomingMessages,OutgoingMessages \
  --interval PT1M \
  --output table

# Get consumer group offset (lag)
az eventhubs eventhub consumer-group show \
  --resource-group rg-genai-kafka \
  --namespace-name genai-kafka-ns \
  --eventhub-name raw-transactions \
  --name ml-inference-group
```

#### Cleanup

```bash
az group delete --name rg-genai-kafka --yes --no-wait
```

---

## 10. Custom Kafka Cluster Design for GenAI/ML

This section answers: *"If I am building a GenAI/ML platform from scratch, what exactly do I put in my Kafka cluster?"*

### 10.1 Topic Taxonomy & Naming

Use a consistent naming convention so teams can discover topics, apply ACLs, and set retention policies by pattern.

**Recommended pattern:** `<domain>.<entity>.<event-type>`

```
genai.llm.requests           ← inbound user queries
genai.llm.responses          ← model outputs
genai.llm.dead-letter        ← failed after max retries
genai.feedback.events        ← thumbs up/down, ratings
genai.documents.ingested     ← new docs for RAG pipeline
genai.embeddings.ready       ← embedded vectors dispatched to vector store

ml.features.raw              ← raw event stream
ml.features.enriched         ← feature-engineered output
ml.predictions.output        ← model inference results
ml.predictions.ground-truth  ← actual outcomes (for retraining)
ml.drift.alerts              ← feature distribution shift events
ml.model.retrain-trigger     ← fires when drift exceeds threshold

ops.audit.all-events         ← compliance mirror of every topic (Kafka MirrorMaker)
ops.alerts.system            ← infrastructure alerts
```

---

### 10.2 Partition Count Guidelines

Partitions are the unit of parallelism. Getting this wrong is expensive to fix later.

#### Rules of thumb

| Factor | Guidance |
|--------|----------|
| **Target throughput** | `partitions = ceil(target_MB/s ÷ single_partition_MB/s)` — a single partition does ~10-50 MB/s on commodity hardware |
| **Consumer parallelism** | Partitions ≥ max consumers you'll ever run in one group |
| **LLM request topics** | Start at 2× your peak concurrent workers (scale up without data loss) |
| **Feedback / audit topics** | 2–4 partitions — low volume, ordering per user is fine |
| **Feature engineering topics** | Match your Flink/Kafka Streams parallelism (typically 12–24) |
| **Dead letter topics** | 1–2 partitions — low volume, processed manually or by retry scheduler |

#### Full Topic Design Table for a Mid-Scale GenAI Platform

| Topic | Partitions | Replication Factor | Retention | Key (partition by) | Notes |
|-------|-----------|-------------------|-----------|-------------------|-------|
| `genai.llm.requests-high` | 12 | 3 | 30 min | `session_id` | Enterprise tier — drain first |
| `genai.llm.requests-medium` | 8 | 3 | 1 hr | `session_id` | Standard tier |
| `genai.llm.requests-low` | 4 | 3 | 4 hr | `session_id` | Free tier — can lag |
| `genai.llm.responses` | 12 | 3 | 24 hr | `request_id` | Keyed for exact-once lookup |
| `genai.llm.dead-letter` | 2 | 3 | 7 days | `request_id` | Manual review + retry |
| `genai.feedback.events` | 4 | 3 | 90 days | `user_id` | RLHF dataset accumulation |
| `genai.documents.ingested` | 6 | 3 | 7 days | `doc_id` | Idempotent embedding re-run |
| `ml.features.raw` | 24 | 3 | 3 days | `entity_id` | High-volume event stream |
| `ml.features.enriched` | 24 | 3 | 1 day | `entity_id` | Matches raw partition count |
| `ml.predictions.output` | 12 | 3 | 30 days | `entity_id` | Long retention for drift calc |
| `ml.predictions.ground-truth` | 6 | 3 | 90 days | `entity_id` | Needed for model retraining |
| `ml.drift.alerts` | 2 | 3 | 7 days | `model_id` | Low volume, high importance |
| `ops.audit.all-events` | 6 | 3 | 365 days | - | Compacted, long retention |

---

### 10.3 Full Cluster Config Reference

#### Local Development Cluster (`docker-compose.yml`)

Already present in this repo — single-broker KRaft mode, no ZooKeeper, suitable for dev.

#### Production-Grade `server.properties` Reference

```properties
# ─── Broker Identity ───────────────────────────────────────────────────────
broker.id=1
node.id=1

# ─── KRaft (no ZooKeeper) ──────────────────────────────────────────────────
process.roles=broker,controller
controller.quorum.voters=1@broker1:9093,2@broker2:9093,3@broker3:9093
controller.listener.names=CONTROLLER

# ─── Listeners ─────────────────────────────────────────────────────────────
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=PLAINTEXT://broker1.internal:9092
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT

# ─── Storage ───────────────────────────────────────────────────────────────
log.dirs=/data/kafka
log.retention.hours=168                    # 7 days default retention
log.retention.bytes=107374182400           # 100 GB per partition cap
log.segment.bytes=1073741824               # 1 GB segments (faster log compaction)
log.cleanup.policy=delete                  # use 'compact' for changelog topics

# ─── Replication & Durability ──────────────────────────────────────────────
default.replication.factor=3
min.insync.replicas=2                      # writes require 2/3 brokers ACK
unclean.leader.election.enable=false       # never elect a lagging replica as leader

# ─── Performance Tuning for ML/GenAI Workloads ─────────────────────────────
num.partitions=6                           # default for auto-created topics
num.network.threads=8
num.io.threads=16
socket.send.buffer.bytes=1048576           # 1 MB
socket.receive.buffer.bytes=1048576
socket.request.max.bytes=104857600        # 100 MB max message size

# Large messages (e.g., embedding vectors, LLM prompts)
message.max.bytes=10485760                 # 10 MB per message
replica.fetch.max.bytes=10485760

# ─── Producer Defaults ─────────────────────────────────────────────────────
# These are server-side caps; producers also configure acks, linger, batch
compression.type=lz4                       # best balance: speed vs. size for ML data
# Use zstd for archival topics (audit, ground-truth) — better ratio, higher CPU

# ─── Consumer Coordination ─────────────────────────────────────────────────
group.initial.rebalance.delay.ms=3000      # wait 3s before rebalancing (absorbs rolling deploys)
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2

# ─── Topic Auto-Creation (disable in production — use IaC instead) ─────────
auto.create.topics.enable=false

# ─── Quotas (prevent one LLM worker from saturating brokers) ───────────────
# Set per-client via Admin API:
# quota.producer.default=50000000    # 50 MB/s per producer
# quota.consumer.default=50000000    # 50 MB/s per consumer group
```

#### Create All GenAI/ML Topics in One Script

```bash
#!/usr/bin/env bash
# create-topics.sh — run once after cluster is up
BS="--bootstrap-server localhost:9092"

create_topic() {
  local name=$1 partitions=$2 retention_ms=$3 cleanup=${4:-delete}
  kafka-topics.sh $BS \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor 3 \
    --config "retention.ms=$retention_ms" \
    --config "cleanup.policy=$cleanup" \
    --config "compression.type=lz4" \
    --config "min.insync.replicas=2"
  echo "  Created: $name (partitions=$partitions)"
}

echo "=== Creating GenAI topics ==="
create_topic genai.llm.requests-high     12   1800000    # 30 min
create_topic genai.llm.requests-medium    8   3600000    # 1 hr
create_topic genai.llm.requests-low       4  14400000    # 4 hr
create_topic genai.llm.responses         12  86400000    # 24 hr
create_topic genai.llm.dead-letter        2  604800000   # 7 days
create_topic genai.feedback.events        4  7776000000  # 90 days
create_topic genai.documents.ingested     6  604800000   # 7 days

echo "=== Creating ML topics ==="
create_topic ml.features.raw             24  259200000   # 3 days
create_topic ml.features.enriched        24  86400000    # 1 day
create_topic ml.predictions.output       12  2592000000  # 30 days
create_topic ml.predictions.ground-truth  6  7776000000  # 90 days
create_topic ml.drift.alerts              2  604800000   # 7 days
create_topic ml.model.retrain-trigger     1  604800000   # 7 days

echo "=== Creating Ops topics (compact + long retention) ==="
create_topic ops.audit.all-events         6  -1          compact  # forever

echo "=== All topics created ==="
kafka-topics.sh $BS --list
```

```bash
chmod +x create-topics.sh && ./create-topics.sh
```

#### Consumer Group Design

```bash
# Each logical service gets its own consumer group so it can track offsets independently
# Example consumer groups for a GenAI platform:

genai-llm-workers-claude       # reads genai.llm.requests-* (Claude worker pool)
genai-llm-workers-gpt4o        # reads same topics (A/B test, separate offsets)
genai-embedding-worker         # reads genai.documents.ingested
genai-response-router          # reads genai.llm.responses → pushes to WebSocket
genai-feedback-sink            # reads genai.feedback.events → writes to S3

ml-feature-engineer            # reads ml.features.raw
ml-inference-worker            # reads ml.features.enriched
ml-drift-monitor               # reads ml.predictions.output
ml-retraining-trigger          # reads ml.drift.alerts

audit-sink                     # reads ops.audit.all-events → writes to BigQuery/S3
```

#### Describe + Verify Topics

```bash
# Check partition count and replication
kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic genai.llm.requests-high

# Check all consumer group lags at once
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --all-groups

# Find topics with under-replicated partitions (cluster health)
kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --under-replicated-partitions

# Increase partitions on an existing topic (only increase, never decrease)
kafka-topics.sh --bootstrap-server localhost:9092 \
  --alter --topic genai.llm.requests-medium \
  --partitions 16
```

#### Three-Broker Production Docker Compose

```yaml
# docker-compose.prod.yml — 3-broker KRaft cluster
services:
  broker1:
    image: apache/kafka:3.7.1
    hostname: broker1
    container_name: broker1
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker1:9092
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@broker1:9093,2@broker2:9093,3@broker3:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_LOG_DIRS: /var/lib/kafka/data
    volumes:
      - broker1-data:/var/lib/kafka/data

  broker2:
    image: apache/kafka:3.7.1
    hostname: broker2
    container_name: broker2
    ports:
      - "9093:9092"
    environment:
      KAFKA_NODE_ID: 2
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker2:9092
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@broker1:9093,2@broker2:9093,3@broker3:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_LOG_DIRS: /var/lib/kafka/data
    volumes:
      - broker2-data:/var/lib/kafka/data

  broker3:
    image: apache/kafka:3.7.1
    hostname: broker3
    container_name: broker3
    ports:
      - "9094:9092"
    environment:
      KAFKA_NODE_ID: 3
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker3:9092
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@broker1:9093,2@broker2:9093,3@broker3:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_LOG_DIRS: /var/lib/kafka/data
    volumes:
      - broker3-data:/var/lib/kafka/data

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: production
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: broker1:9092,broker2:9092,broker3:9092
    depends_on: [broker1, broker2, broker3]

volumes:
  broker1-data:
  broker2-data:
  broker3-data:
```

```bash
# Start 3-broker cluster
docker compose -f docker-compose.prod.yml up -d

# Wait for brokers to elect controller, then create topics
sleep 15 && ./create-topics.sh
```

---

## 11. Demo Code Walkthrough

### [`llm_multi_api.py`](llm_multi_api.py)

Demonstrates the full multi-LLM fan-out pattern from Section 3.6:
1. Publishes requests to three priority topics (`high` / `medium` / `low`)
2. Two worker pools consume in parallel — `llama3-pool-a` (`llama3-8b-8192`, 30 RPM) and `mixtral-pool-b` (`mixtral-8x7b-32768`, 60 RPM), both via Groq API
3. Per-worker token-bucket enforces configurable RPM limits
4. Requests that exceed retry budget are routed to `genai.llm.dead-letter`
5. Live dashboard prints: queue depth per tier, provider distribution, avg latency

Requires a Groq API key set as `GROQ_API_KEY` on line 31 of the file.

### [`producer.py`](producer.py)

Simulates a payment processing service publishing transaction events to `raw-transactions`. Each message contains:
- Transaction metadata (amount, merchant, user ID)
- Timestamp for latency measurement
- Deliberately injected anomalies (10% of events) to trigger fraud detection

#### What `producer.py` is actually doing (step by step)

```
Your Python script
      │
      │  creates a KafkaProducer connected to localhost:9092
      │
      ▼
  Picks a random user (user_001 to user_020)
      │
      ├── 90% chance → normal transaction  (grocery, airline, pharmacy...)
      └── 10% chance → anomalous transaction (crypto_exchange, luxury_goods, $8k–$25k)
      │
      ▼
  Serialises the event to JSON and sends it to topic: raw-transactions
      │
      │  Kafka decides which partition using the key (user_id)
      │  → same user always goes to same partition = ordering guaranteed per user
      │
      ├── user_018 → always Partition 1
      ├── user_003 → always Partition 0
      └── user_012 → always Partition 2
      │
      ▼
  Every 10 messages → producer.flush()
  (forces all buffered messages to actually reach the broker)
      │
      ▼
  Sleeps 0.3–1.0 seconds → simulates realistic payment traffic rate
```

#### Reading the output

```
[normal ] user=user_018 amount=$    451.54 merchant=airline
```
- `normal` = regular transaction, amount close to merchant's average — will likely get `approved` by the ML consumer

```
[ANOMALY] user=user_007 amount=$  14200.00 merchant=crypto_exchange
```
- Deliberately injected spike — high amount + high-risk merchant — should get `FLAGGED` with a score near 1.0

```
  [OK]    raw-transactions[1] offset=42
```
- Kafka confirmed the message landed in **partition 1** at **offset 42**
- Offset is permanent — you can replay from offset 42 any time in the future

#### Why the producer and consumer are decoupled

The producer never calls the consumer directly. Kafka is the middleman. You could stop the consumer, let 100 messages queue up, restart it, and it processes all 100 from where it left off — this is what `auto_offset_reset="latest"` and consumer group offsets give you.

### [`consumer_ml.py`](consumer_ml.py)

Implements the full ML inference pipeline:
1. Consumes from `raw-transactions`
2. Computes real-time features (velocity, z-score vs. user baseline)
3. Runs Isolation Forest anomaly detection
4. Publishes scored results to `flagged-transactions` or `approved-transactions`
5. Prints end-to-end latency per message

#### What `consumer_ml.py` is actually doing (step by step)

```
Kafka broker delivers next message from raw-transactions
      │
      ▼
  Parse JSON event  →  extract user_id, amount, merchant, country
      │
      ▼
  OnlineFeatureStore.record(user_id, amount, timestamp)
  Updates two in-memory structures per user:
    ├── rolling deque  → transactions in last 60 seconds
    └── history list   → last 100 transaction amounts (for baseline)
      │
      ▼
  Build 6-feature vector:
    [0] log(amount)              — scale-normalised transaction size
    [1] velocity_count           — how many txns in last 60s
    [2] log(velocity_amount)     — total spend in last 60s
    [3] z_score                  — how unusual this amount vs. user history
    [4] merchant_risk_score      — low=0.1 / medium=0.4 / high=0.9
    [5] country_risk_score       — US=0.0, RU=0.8, NG=0.8 ...
      │
      ▼
  IsolationForest.decision_function(features)
  Normalised to anomaly score in [0.0 → 1.0]
    score < 0.5  →  publish to approved-transactions  ✓
    score ≥ 0.5  →  publish to flagged-transactions   ⚠
      │
      ▼
  Print result + end-to-end latency in milliseconds
      │
      ▼
  Every 20 messages → print rolling stats:
    flagged rate (%), average latency (ms)
```

### [`genai_pipeline.py`](genai_pipeline.py)

Demonstrates a streaming GenAI pipeline:
1. Publishes customer support queries to `llm-requests`
2. Consumer enriches each query with mock context (simulates RAG retrieval)
3. Calls Groq API (`llama-3.3-70b-versatile`) for response generation
4. Publishes responses to `llm-responses`
5. Collects feedback events to `feedback-events` (RLHF loop)

---

## 12. Key Takeaways

| Scenario                        | Kafka's Role                                      | Without Kafka                          |
|---------------------------------|---------------------------------------------------|----------------------------------------|
| Real-time fraud detection       | Sub-second feature engineering + model serving   | Batch jobs, 15-min delay               |
| LLM RAG with live data          | Keeps vector store current as docs arrive        | Stale knowledge base                   |
| Multi-agent AI coordination     | Durable task bus between agents                  | Direct HTTP calls, no retry/replay     |
| RLHF feedback loop              | Durable capture of every user signal             | Lost feedback, slower fine-tuning      |
| Model drift detection           | Continuous feature distribution monitoring       | Manual periodic checks                 |
| Cloud-native ML inference       | Absorbs burst, decouples producers from models   | Direct API calls overload model server |

---

## 13. References & Further Reading

- [How OpenAI uses Apache Kafka and Flink for GenAI Data Pipelines](https://www.kai-waehner.de/blog/2025/06/09/how-openai-uses-apache-kafka-and-flink-for-genai/)
- [Real-Time Model Inference with Apache Kafka and Flink for Predictive AI and GenAI](https://www.kai-waehner.de/blog/2024/10/01/real-time-model-inference-with-apache-kafka-and-flink-for-predictive-ai-and-genai/)
- [Online Model Training and Model Drift with Apache Kafka and Flink](https://www.kai-waehner.de/blog/2025/02/23/online-model-training-and-model-drift-in-machine-learning-with-apache-kafka-and-flink/)
- [The Rise of Kappa Architecture in the Era of Agentic AI](https://www.kai-waehner.de/blog/2025/07/08/the-rise-of-kappa-architecture-in-the-era-of-agentic-ai-and-data-streaming/)
- [Using Apache Kafka in AI projects — Instaclustr](https://www.instaclustr.com/education/apache-kafka/using-apache-kafka-in-ai-projects-benefits-use-cases-and-best-practices/)
- [Real-Time ML Pipelines — Conduktor](https://www.conduktor.io/glossary/real-time-ml-pipelines)
- [Add Your First ML Model to a Streaming Pipeline — Confluent](https://www.confluent.io/blog/first-ml-function-streaming/)
- [Google Cloud Managed Service for Apache Kafka Docs](https://docs.cloud.google.com/managed-service-for-apache-kafka/docs/release-notes)
- [Amazon MSK Developer Guide](https://docs.aws.amazon.com/msk/latest/developerguide/)
- [Apache Kafka Protocol Support in Azure Event Hubs](https://learn.microsoft.com/en-us/azure/event-hubs/azure-event-hubs-apache-kafka-overview)
- [Kafka-ML Framework Analysis](https://taogang.medium.com/an-analysis-of-kafka-ml-a-framework-for-real-time-machine-learning-pipelines-1f2e28e213ea)
- [Streaming Machine Learning Inference with Kafka and TensorFlow Serving](https://dev.co/machine-learning-inference-with-kafka-and-tensorflow)
