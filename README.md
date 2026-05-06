# AI-Augmented Clinical Alerting System

A real-time medical monitoring pipeline that ingests high-frequency patient vitals and uses a local AI agent to filter alert fatigue — distinguishing sensor noise from genuine clinical emergencies.

## Architecture

```
Bedside Monitor (Producer)
        │
        ▼ vitals-raw (Kafka)
Sentinel Filter (Redis Lua)
        │  threshold crossed?
        ▼ triage-investigation (Kafka)
Clinical Agent (Ollama)  ← up to 4 concurrent inferences
        │
        ├──► MongoDB (all alerts archived)
        └──► Redis Pub/Sub ──► Nursing Dashboard (WebSocket)
```

| Layer | Technology | Role |
|---|---|---|
| Ingestion | Redpanda (Kafka) | 5-partition vitals stream at 50ms intervals, 10 patients |
| Speed filter | Redis (Lua script) | Atomic sliding-window threshold check |
| AI triage | Ollama `llama3.2:1b` | Scores alert 1–10, suppresses false alarms |
| Storage | MongoDB | Full audit trail of every triage decision |
| API | FastAPI | REST history + live WebSocket push |

## Prerequisites

- Docker Desktop (≥ 6 GB memory allocated)

## Setup

```bash
# 1. Clone and enter the project
git clone <repo-url>
cd Proj

# 2. Copy environment file
cp .env.example .env

# 3. Pull the AI model (one-time, ~800 MB)
#    Do this before starting the stack so the agent doesn't time out waiting.
docker compose run --rm ollama ollama pull llama3.2:1b
```

## Running

**Production mode** — starts the full stack, no simulated data:

```bash
docker compose up --build
```

**Demo mode** — same as above but also starts the vitals simulator (5 fake patients at 100ms intervals):

```bash
docker compose --profile demo up --build
```

**Start the simulator on an already-running stack:**

```bash
docker compose run --rm producer
```

| URL | Purpose |
|---|---|
| http://localhost:8000 | Nursing dashboard (clinical) |
| http://localhost:3000 | Grafana (engineering — login: admin / admin) |
| http://localhost:9090 | Prometheus UI |

Useful commands:

```bash
docker compose logs -f agent        # tail the AI triage worker
docker compose up --build -d        # run everything in the background
docker compose down                 # stop and remove containers
```

## API

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/alerts/{patient_id}` | Recent alert history for a patient |
| `GET` | `/metrics` | False-alarm suppression rate (last 24 h) |
| `WS` | `/ws/alerts` | Live push for critical alerts (Score > 7) |
| `GET` | `/admin/baselines` | List all patient baseline thresholds (from MongoDB) |
| `PUT` | `/admin/baselines/{patient_id}` | Update a patient's baseline; propagates to Redis immediately |

Patient baseline fields: `hr_min`, `hr_max`, `spo2_min`, `bp_min`, `bp_max`. Changes take effect on the next vitals reading — no restart required.

## How it works

**Sentinel filter** — every vitals reading is passed to a Redis Lua script that atomically:
1. Checks the Signal Quality Index (SQI < 0.6 → sensor artifact, discarded immediately)
2. Stores the reading in a per-patient sorted set (10-minute sliding window)
3. Compares against the patient's baseline thresholds
4. Produces a triage event to Kafka only if a threshold is crossed

**Clinical agent** — for each triage event the agent:
1. Fetches the full 10-minute vitals window from Redis
2. Sends the context to Ollama with a structured clinical prompt
3. Parses a `criticality_score` (1–10) and `reasoning` from the response
4. Writes the result to MongoDB; publishes to Redis Pub/Sub if Score > 7

Both `vitals-raw` and `triage-investigation` have 5 partitions. Kafka distributes partitions across consumer group members automatically, so scaling the agent is a single command:

```bash
docker compose up --scale agent=3
```

Up to `agent_thread_workers` (default: 4) Ollama inferences run concurrently per agent instance, via a sliding-window offset strategy: Kafka offsets are committed in receipt order, so a crash always replays from the correct position.

**Dead Letter Queue** — if a triage event fails processing, it is retried in-place up to `max_retries` times (default: 3) without requiring a restart. After all retries are exhausted the message is published to `triage-dead-letter` with full error context and its offset is committed, unblocking the rest of the queue. Failed messages can be inspected with:

```bash
docker exec -it proj-redpanda-1 rpk topic consume triage-dead-letter
```

**Scoring guide**

| Score | Meaning |
|---|---|
| 1–4 | Likely artifact or minor variation — alert suppressed |
| 5–7 | Monitor closely, not immediately life-threatening |
| 8–10 | Genuine emergency — pushed live to the dashboard |

## Observability — Grafana

The Grafana dashboard at http://localhost:3000 (admin / admin) auto-provisions on startup with 4 time-series panels and 6 stat panels:

| Metric | PromQL |
|---|---|
| Vitals/sec | `sum(rate(vitals_ingested_total[1m]))` |
| Triage events/sec | `sum(rate(triage_dispatched_total[1m]))` |
| AI inference p95 | `histogram_quantile(0.95, ...)` |
| Suppression rate | `sum(alerts_total{outcome="suppressed"}) / sum(alerts_total)` |
| DLQ messages | `sum(dlq_messages_total)` |
| Active WS connections | `ws_connections_active` |

Each Python service exposes Prometheus metrics on port `9100`. Prometheus scrapes all three (sentinel, agent, api) every 10 seconds.

## Logs

Every service emits single-line JSON logs. Each log line carries a `ctx` object with domain fields:

```json
{"ts": "2026-05-06T14:23:01Z", "level": "INFO", "service": "sentinel", "msg": "triage_dispatched",
 "ctx": {"trace_id": "a3f1c...", "patient_id": "P001", "heart_rate": 134.2, "latency_ms": 1.83}}

{"ts": "2026-05-06T14:23:09Z", "level": "INFO", "service": "agent",    "msg": "assessment_complete",
 "ctx": {"trace_id": "a3f1c...", "patient_id": "P001", "score": 9, "ai_latency_ms": 7420, "total_latency_ms": 7589}}
```

The `trace_id` is generated at the sentinel and propagates through to MongoDB, so a single alert can be traced end-to-end across all service logs.

Key log events per service:

| Service | Event | Notable fields |
|---|---|---|
| `producer` | `started` | `patients`, `interval_ms` |
| `sentinel` | `triage_dispatched` | `trace_id`, `patient_id`, `latency_ms` |
| `agent` | `triage_received` | `trace_id`, `patient_id` |
| `agent` | `assessment_complete` | `score`, `ai_latency_ms`, `total_latency_ms` |
| `agent` | `critical_alert_published` | `trace_id`, `score` |
| `agent` | `retrying` | `partition`, `offset`, `attempt`, `error` |
| `agent` | `sending_to_dlq` | `partition`, `offset`, `attempt`, `error` |
| `api` | `ws_broadcast_error` | `error` |

## Project structure

```
├── producer/
│   └── vitals_generator.py   # Simulated bedside monitor
├── sentinel/
│   ├── redis_client.py       # Sliding window + Lua runner
│   ├── threshold_check.lua   # Atomic multi-vital check
│   └── worker.py             # Kafka consumer → Redis → triage producer
├── agent/
│   ├── clinical_agent.py     # Ollama prompt + response parsing
│   └── worker.py             # Triage consumer → concurrent AI → MongoDB
├── api/
│   ├── main.py               # FastAPI app (REST + WebSocket)
│   ├── mongo_client.py       # Alert persistence and metrics
│   ├── websocket.py          # WebSocket connection manager
│   └── static/index.html     # Nursing dashboard UI
├── models/
│   └── schemas.py            # Pydantic models (includes trace_id)
├── logger.py                 # Shared JSON formatter used by all services
├── config.py                 # Centralised settings (env-backed)
├── Dockerfile                # Single image used by all Python services
├── docker-compose.yml        # Full stack: infrastructure + Python services
└── requirements.txt
```
