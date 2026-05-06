"""
MongoDB client (synchronous)
Used by: agent worker (writes) and FastAPI endpoints (reads).
"""

from datetime import datetime, timedelta

import pymongo

from config import settings
from models.schemas import AlertRecord


class MongoAlertClient:
    def __init__(self) -> None:
        client = pymongo.MongoClient(settings.mongo_uri)
        db = client[settings.mongo_db]
        self.collection = db[settings.mongo_collection]
        self.baselines  = db["baselines"]

        # Compound index covers patient history queries and the suppression metric
        self.collection.create_index(
            [("patient_id", pymongo.ASCENDING), ("created_at", pymongo.DESCENDING)]
        )
        self.collection.create_index([("assessment.criticality_score", pymongo.ASCENDING)])

    def save_alert(self, record: AlertRecord) -> None:
        doc = record.model_dump()
        # model_dump() returns datetime objects which MongoDB accepts natively
        self.collection.insert_one(doc)

    def get_patient_alerts(self, patient_id: str, limit: int = 20) -> list[dict]:
        cursor = (
            self.collection.find({"patient_id": patient_id}, {"_id": 0})
            .sort("created_at", pymongo.DESCENDING)
            .limit(limit)
        )
        results = []
        for doc in cursor:
            if isinstance(doc.get("created_at"), datetime):
                doc["created_at"] = doc["created_at"].isoformat()
            results.append(doc)
        return results

    def get_all_baselines(self) -> list[dict]:
        return list(self.baselines.find({}, {"_id": 0}))

    def upsert_baseline(self, patient_id: str, data: dict) -> None:
        self.baselines.update_one(
            {"patient_id": patient_id},
            {"$set": {"patient_id": patient_id, **data, "updated_at": datetime.utcnow()}},
            upsert=True,
        )

    def get_suppression_metrics(self) -> dict:
        since = datetime.utcnow() - timedelta(hours=24)
        total = self.collection.count_documents({"created_at": {"$gte": since}})
        suppressed = self.collection.count_documents(
            {"created_at": {"$gte": since}, "assessment.is_suppressed": True}
        )
        critical = total - suppressed
        return {
            "window": "last_24h",
            "total_alerts":      total,
            "suppressed_alerts": suppressed,
            "critical_alerts":   critical,
            "suppression_rate":  round(suppressed / total, 3) if total else 0.0,
        }
