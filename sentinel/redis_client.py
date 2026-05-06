import json
from pathlib import Path

import pymongo
import redis

from config import settings
from models.schemas import VitalsReading

# Defaults written to MongoDB on first startup if no baselines exist.
# After that, MongoDB is the source of truth — edit via PUT /admin/baselines/{id}.
_DEFAULT_BASELINES: dict[str, dict] = {
    "P001":  {"hr_min": 50, "hr_max": 100, "spo2_min": 94, "bp_min": 80, "bp_max": 140},
    "P002":  {"hr_min": 55, "hr_max": 105, "spo2_min": 93, "bp_min": 85, "bp_max": 145},
    "P003":  {"hr_min": 45, "hr_max": 95,  "spo2_min": 95, "bp_min": 75, "bp_max": 135},
    "P004":  {"hr_min": 60, "hr_max": 110, "spo2_min": 92, "bp_min": 80, "bp_max": 140},
    "P005":  {"hr_min": 50, "hr_max": 100, "spo2_min": 94, "bp_min": 80, "bp_max": 138},
    "P006":  {"hr_min": 48, "hr_max": 98,  "spo2_min": 94, "bp_min": 80, "bp_max": 138},
    "P007":  {"hr_min": 55, "hr_max": 108, "spo2_min": 93, "bp_min": 85, "bp_max": 148},
    "P008":  {"hr_min": 45, "hr_max": 95,  "spo2_min": 95, "bp_min": 70, "bp_max": 130},
    "P009":  {"hr_min": 62, "hr_max": 115, "spo2_min": 91, "bp_min": 88, "bp_max": 155},
    "P010":  {"hr_min": 52, "hr_max": 102, "spo2_min": 94, "bp_min": 80, "bp_max": 140},
}

_BASELINE_FIELDS = ("hr_min", "hr_max", "spo2_min", "bp_min", "bp_max")


class SentinelClient:
    def __init__(self) -> None:
        self.r = redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            decode_responses=True,
        )
        self._baselines = (
            pymongo.MongoClient(settings.mongo_uri)[settings.mongo_db]["baselines"]
        )
        lua = (Path(__file__).parent / "threshold_check.lua").read_text()
        self._script = self.r.register_script(lua)
        self._seed_baselines()

    def _seed_baselines(self) -> None:
        """
        Ensures defaults exist in MongoDB, then loads all baselines into Redis.
        On every restart, Redis is re-synced from MongoDB so any edits made
        via the admin API survive container restarts.
        """
        for patient_id, baseline in _DEFAULT_BASELINES.items():
            self._baselines.update_one(
                {"patient_id": patient_id},
                {"$setOnInsert": {"patient_id": patient_id, **baseline}},
                upsert=True,
            )

        for doc in self._baselines.find():
            self._load_into_redis(doc["patient_id"], doc)

    def _load_into_redis(self, patient_id: str, doc: dict) -> None:
        self.r.hset(
            f"baseline:{patient_id}",
            mapping={f: str(doc[f]) for f in _BASELINE_FIELDS if f in doc},
        )

    def check_and_store(self, vitals: VitalsReading) -> bool:
        """Atomically stores the reading and returns True if triage is needed."""
        result = self._script(
            keys=[f"vitals:{vitals.patient_id}", f"baseline:{vitals.patient_id}"],
            args=[
                vitals.timestamp,
                vitals.model_dump_json(),
                vitals.heart_rate,
                vitals.spo2,
                vitals.bp_systolic,
                vitals.signal_quality_index,
            ],
        )
        return result == 1

    def get_vitals_window(self, patient_id: str) -> list[dict]:
        """Returns the full 10-minute vitals window for a patient."""
        raw = self.r.zrange(f"vitals:{patient_id}", 0, -1)
        return [json.loads(v) for v in raw]

    def publish_critical_alert(self, payload: str) -> None:
        self.r.publish("critical_alerts", payload)
