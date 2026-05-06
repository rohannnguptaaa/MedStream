"""
Shared Prometheus metrics.

Each service imports only the counters/histograms it updates.
All services expose metrics on port 9100 via start_http_server().
"""

from prometheus_client import Counter, Gauge, Histogram

vitals_ingested_total = Counter(
    "vitals_ingested_total",
    "Total vitals readings processed by the sentinel",
    ["patient_id"],
)

triage_dispatched_total = Counter(
    "triage_dispatched_total",
    "Total triage events dispatched to the investigation topic",
)

ai_inference_duration_seconds = Histogram(
    "ai_inference_duration_seconds",
    "Ollama inference duration per triage event",
    buckets=[1, 2, 5, 10, 20, 30, 60, 120],
)

alerts_total = Counter(
    "alerts_total",
    "Total alerts processed by the AI agent",
    ["outcome"],  # "suppressed" or "critical"
)

dlq_messages_total = Counter(
    "dlq_messages_total",
    "Messages forwarded to the dead letter queue after exhausting retries",
)

ws_connections_active = Gauge(
    "ws_connections_active",
    "Active WebSocket connections to the nursing dashboard",
)
