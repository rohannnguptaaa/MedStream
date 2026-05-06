from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, Field


class VitalsReading(BaseModel):
    patient_id: str
    timestamp: float  # unix seconds
    heart_rate: float
    spo2: float
    bp_systolic: float
    signal_quality_index: float  # 0.0–1.0


class TriageEvent(BaseModel):
    patient_id: str
    trigger_timestamp: float
    trigger_vitals: VitalsReading
    # Generated at the sentinel when the event is first created.
    # Flows through to the agent and MongoDB so one reading is traceable
    # across all three log streams.
    trace_id: str = Field(default_factory=lambda: uuid.uuid4().hex)


class ClinicalAssessment(BaseModel):
    criticality_score: int = Field(ge=1, le=10)
    reasoning: str
    is_suppressed: bool  # True when score <= 7


class AlertRecord(BaseModel):
    patient_id: str
    trigger_timestamp: float
    trigger_vitals: VitalsReading
    vitals_window: list[dict]
    assessment: ClinicalAssessment
    trace_id: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
