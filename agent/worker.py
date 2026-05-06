"""
Agent Worker
------------
Consumes triage events from `triage-investigation`, runs the clinical AI
analysis in a thread pool, writes every result to MongoDB, and publishes
critical alerts (score > 7) to the Redis `critical_alerts` pub/sub channel.

Concurrency — sliding window:
  A deque of (msg, future) pairs is maintained in receipt order. New messages
  are submitted to the thread pool while the window has capacity. Completed
  futures are drained from the front: offsets are committed in order so a
  crash never skips or duplicates a message.

Retry + Dead Letter Queue:
  When a future raises an exception, it is retried in-place (the failed future
  is replaced with a fresh submission at the same deque position). After
  max_retries consecutive failures the message is published to
  `triage-dead-letter` with full error context and its offset is committed,
  unblocking the rest of the queue. The retry loop does not require a restart
  and concurrent processing of other messages continues throughout.
"""

import json
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, Producer
from prometheus_client import start_http_server

from agent.clinical_agent import ClinicalAgent
from api.mongo_client import MongoAlertClient
from config import settings
from logger import get_logger
from metrics import ai_inference_duration_seconds, alerts_total, dlq_messages_total
from models.schemas import AlertRecord, TriageEvent
from sentinel.redis_client import SentinelClient

log = get_logger("agent")


class AgentWorker:
    def __init__(self) -> None:
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                "group.id": "clinical-agent-group",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self.dlq_producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                "acks": "all",
                "enable.idempotence": True,
            }
        )
        self.executor = ThreadPoolExecutor(max_workers=settings.agent_thread_workers)
        self.agent    = ClinicalAgent()
        self.mongo    = MongoAlertClient()
        self.redis    = SentinelClient()

        # Tracks consecutive failure counts per message, keyed by
        # "{partition}-{offset}". Cleaned up on success or DLQ dispatch.
        self._retry_counts: dict[str, int] = {}

    def _msg_key(self, msg) -> str:
        return f"{msg.partition()}-{msg.offset()}"

    def _send_to_dlq(self, msg, error: str, attempt: int) -> None:
        payload = json.dumps({
            "original_topic":     msg.topic(),
            "original_partition": msg.partition(),
            "original_offset":    msg.offset(),
            "attempt":            attempt,
            "error":              error,
            "failed_at":          datetime.now(timezone.utc).isoformat(),
            "original_payload":   msg.value().decode(),
        })
        self.dlq_producer.produce(
            topic=settings.kafka_dlq_topic,
            key=msg.key(),
            value=payload,
        )
        self.dlq_producer.flush()

    def _process(self, raw: str, received_at: float) -> None:
        """Runs inside the thread pool — one Ollama call per thread."""
        event = TriageEvent(**json.loads(raw))

        log.info("triage_received", extra={"ctx": {
            "trace_id":   event.trace_id,
            "patient_id": event.patient_id,
        }})

        vitals_window = self.redis.get_vitals_window(event.patient_id)

        ai_start   = time.perf_counter()
        assessment = self.agent.analyze(event, vitals_window)
        ai_elapsed = time.perf_counter() - ai_start
        ai_ms      = round(ai_elapsed * 1000)
        ai_inference_duration_seconds.observe(ai_elapsed)

        record = AlertRecord(
            patient_id=event.patient_id,
            trigger_timestamp=event.trigger_timestamp,
            trigger_vitals=event.trigger_vitals,
            vitals_window=vitals_window,
            assessment=assessment,
            trace_id=event.trace_id,
        )
        self.mongo.save_alert(record)

        total_ms = round((time.perf_counter() - received_at) * 1000)
        log.info("assessment_complete", extra={"ctx": {
            "trace_id":         event.trace_id,
            "patient_id":       event.patient_id,
            "score":            assessment.criticality_score,
            "suppressed":       assessment.is_suppressed,
            "ai_latency_ms":    ai_ms,
            "total_latency_ms": total_ms,
            "reasoning":        assessment.reasoning,
        }})

        outcome = "suppressed" if assessment.is_suppressed else "critical"
        alerts_total.labels(outcome=outcome).inc()

        if assessment.criticality_score > 7:
            self.redis.publish_critical_alert(record.model_dump_json())
            log.info("critical_alert_published", extra={"ctx": {
                "trace_id":   event.trace_id,
                "patient_id": event.patient_id,
                "score":      assessment.criticality_score,
            }})

    def run(self) -> None:
        start_http_server(9100)
        self.consumer.subscribe([settings.kafka_triage_topic])
        log.info("started", extra={"ctx": {
            "concurrency": settings.agent_thread_workers,
            "max_retries": settings.max_retries,
        }})

        pending: deque = deque()  # (kafka_msg, future) in receipt order

        try:
            while True:
                # ── Consume ──────────────────────────────────────────────────
                if len(pending) < settings.agent_thread_workers:
                    msg = self.consumer.poll(timeout=0.05)
                    if msg is not None:
                        if msg.error():
                            if msg.error().code() != KafkaError._PARTITION_EOF:
                                log.error("kafka_consumer_error",
                                          extra={"ctx": {"error": str(msg.error())}})
                        else:
                            future = self.executor.submit(
                                self._process,
                                msg.value().decode(),
                                time.perf_counter(),
                            )
                            pending.append((msg, future))

                # ── Drain ────────────────────────────────────────────────────
                while pending:
                    oldest_msg, oldest_future = pending[0]
                    if not oldest_future.done():
                        break

                    key = self._msg_key(oldest_msg)
                    exc = oldest_future.exception()

                    if exc:
                        attempt = self._retry_counts.get(key, 0) + 1
                        self._retry_counts[key] = attempt

                        if attempt >= settings.max_retries:
                            # Poison pill — move to DLQ and commit
                            log.error("sending_to_dlq", extra={"ctx": {
                                "partition": oldest_msg.partition(),
                                "offset":    oldest_msg.offset(),
                                "attempt":   attempt,
                                "error":     str(exc),
                            }})
                            self._send_to_dlq(oldest_msg, str(exc), attempt)
                            dlq_messages_total.inc()
                            del self._retry_counts[key]
                            pending.popleft()
                            self.consumer.commit(message=oldest_msg, asynchronous=False)
                        else:
                            # Retry in-place: replace the failed future at the
                            # front of the deque, keeping queue ordering intact.
                            log.warning("retrying", extra={"ctx": {
                                "partition": oldest_msg.partition(),
                                "offset":    oldest_msg.offset(),
                                "attempt":   attempt,
                                "error":     str(exc),
                            }})
                            new_future = self.executor.submit(
                                self._process,
                                oldest_msg.value().decode(),
                                time.perf_counter(),
                            )
                            pending[0] = (oldest_msg, new_future)
                            break  # wait for the retry to complete

                    else:
                        # Success — clean up retry state and commit
                        self._retry_counts.pop(key, None)
                        pending.popleft()
                        self.consumer.commit(message=oldest_msg, asynchronous=False)

        except KeyboardInterrupt:
            log.info("shutting_down")
        finally:
            self.executor.shutdown(wait=True)
            self.dlq_producer.flush()
            self.consumer.close()


if __name__ == "__main__":
    AgentWorker().run()
