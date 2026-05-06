"""
Vitals Generator
----------------
Simulates five bedside monitors emitting patient vitals at 100 ms intervals.
Each patient has a stable baseline; the generator randomly injects:
  - sensor artifacts  (5 % chance)  — low SQI, normal values
  - clinical events   (8 % chance)  — good SQI, abnormal values
"""

import json
import random
import time

from confluent_kafka import Producer

from config import settings
from logger import get_logger

log = get_logger("producer")

_PATIENTS: dict[str, dict] = {
    "P001": {"hr_base": 72,  "spo2_base": 98.0, "bp_base": 120},
    "P002": {"hr_base": 80,  "spo2_base": 97.0, "bp_base": 130},
    "P003": {"hr_base": 65,  "spo2_base": 99.0, "bp_base": 115},
    "P004": {"hr_base": 88,  "spo2_base": 96.0, "bp_base": 125},
    "P005": {"hr_base": 75,  "spo2_base": 98.0, "bp_base": 118},
    "P006": {"hr_base": 70,  "spo2_base": 97.5, "bp_base": 122},
    "P007": {"hr_base": 82,  "spo2_base": 98.0, "bp_base": 135},
    "P008": {"hr_base": 68,  "spo2_base": 99.0, "bp_base": 110},
    "P009": {"hr_base": 90,  "spo2_base": 95.0, "bp_base": 140},
    "P010": {"hr_base": 77,  "spo2_base": 98.5, "bp_base": 126},
}


def _generate(patient_id: str, base: dict) -> dict:
    is_artifact = random.random() < 0.05
    is_event    = random.random() < 0.08

    if is_artifact:
        hr   = base["hr_base"]   + random.gauss(0, 2)
        spo2 = base["spo2_base"] + random.gauss(0, 0.3)
        bp   = base["bp_base"]   + random.gauss(0, 3)
        sqi  = random.uniform(0.10, 0.45)
    elif is_event:
        hr   = base["hr_base"]   + random.uniform(35, 55)
        spo2 = base["spo2_base"] - random.uniform(5,  12)
        bp   = base["bp_base"]   + random.uniform(20, 40)
        sqi  = random.uniform(0.75, 1.0)
    else:
        hr   = base["hr_base"]   + random.gauss(0, 3)
        spo2 = base["spo2_base"] + random.gauss(0, 0.5)
        bp   = base["bp_base"]   + random.gauss(0, 5)
        sqi  = random.uniform(0.80, 1.0)

    return {
        "patient_id":           patient_id,
        "timestamp":            time.time(),
        "heart_rate":           round(hr, 1),
        "spo2":                 round(min(100.0, max(70.0, spo2)), 1),
        "bp_systolic":          round(bp, 1),
        "signal_quality_index": round(sqi, 3),
    }


def run() -> None:
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "acks": "all",
            "enable.idempotence": True,
        }
    )

    log.info("started", extra={"ctx": {"patients": list(_PATIENTS.keys()), "interval_ms": 50}})
    try:
        while True:
            for patient_id, base in _PATIENTS.items():
                vitals = _generate(patient_id, base)
                producer.produce(
                    topic=settings.kafka_vitals_topic,
                    key=patient_id,
                    value=json.dumps(vitals),
                )
            producer.poll(0)
            time.sleep(0.05)
    except KeyboardInterrupt:
        log.info("shutting_down")
    finally:
        producer.flush()


if __name__ == "__main__":
    run()
