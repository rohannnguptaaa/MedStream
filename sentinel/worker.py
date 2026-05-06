"""
Sentinel Worker
--------------
Consumes every vitals reading from `vitals-raw`, runs the atomic Redis Lua
check, and forwards threshold-crossing events to `triage-investigation`.
"""

import json
import time

from confluent_kafka import Consumer, KafkaError, Producer
from prometheus_client import start_http_server

from config import settings
from logger import get_logger
from metrics import triage_dispatched_total, vitals_ingested_total
from models.schemas import TriageEvent, VitalsReading
from sentinel.redis_client import SentinelClient

log = get_logger("sentinel")


class SentinelWorker:
    def __init__(self) -> None:
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                "group.id": "sentinel-group",
                "auto.offset.reset": "latest",
                "enable.auto.commit": False,
            }
        )
        self.producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                "acks": "all",
                "enable.idempotence": True,
            }
        )
        self.redis = SentinelClient()

    def run(self) -> None:
        start_http_server(9100)
        self.consumer.subscribe([settings.kafka_vitals_topic])
        log.info("started")

        try:
            while True:
                msg = self.consumer.poll(timeout=0.1)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error("kafka_consumer_error", extra={"ctx": {"error": str(msg.error())}})
                    continue

                t0 = time.perf_counter()
                try:
                    vitals = VitalsReading(**json.loads(msg.value().decode()))
                    vitals_ingested_total.labels(patient_id=vitals.patient_id).inc()

                    if self.redis.check_and_store(vitals):
                        event = TriageEvent(
                            patient_id=vitals.patient_id,
                            trigger_timestamp=vitals.timestamp,
                            trigger_vitals=vitals,
                        )
                        self.producer.produce(
                            topic=settings.kafka_triage_topic,
                            key=vitals.patient_id,
                            value=event.model_dump_json(),
                        )
                        self.producer.poll(0)
                        triage_dispatched_total.inc()
                        log.info("triage_dispatched", extra={"ctx": {
                            "trace_id":   event.trace_id,
                            "patient_id": vitals.patient_id,
                            "heart_rate": vitals.heart_rate,
                            "spo2":       vitals.spo2,
                            "bp":         vitals.bp_systolic,
                            "latency_ms": round((time.perf_counter() - t0) * 1000, 2),
                        }})

                    self.consumer.commit(message=msg, asynchronous=False)

                except Exception as exc:
                    log.exception("processing_error", extra={"ctx": {"error": str(exc)}})

        except KeyboardInterrupt:
            log.info("shutting_down")
        finally:
            self.producer.flush()
            self.consumer.close()


if __name__ == "__main__":
    SentinelWorker().run()
