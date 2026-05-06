"""
Clinical Agent
--------------
Sends patient context to a local Ollama model and parses a structured
ClinicalAssessment (criticality score + reasoning) from the response.

Design note: we skip the full LangChain ReAct loop because we always call
the same single tool (Redis window fetch) before the LLM, so a straight
chain is simpler and less fragile with smaller local models.
"""

import json
import re

import ollama

from config import settings
from models.schemas import ClinicalAssessment, TriageEvent

_SYSTEM_PROMPT = """\
You are a clinical triage AI for an ICU monitoring system.
Analyze the patient's vital signs and decide whether the alert is a genuine
medical emergency or a false alarm (sensor artifact / transient reading).

Respond ONLY with valid JSON — no extra text, no markdown:
{"criticality_score": <integer 1-10>, "reasoning": "<one or two sentences>"}

Scoring guide:
  1-4  Likely artifact or minor variation — suppress the alert
  5-7  Worth monitoring, not immediately life-threatening
  8-10 Genuine emergency requiring immediate clinical intervention

Analysis principles:
- Multiple vitals abnormal simultaneously → more likely genuine
- Single vital spike with all others stable → likely artifact or exertion
- Sustained trend across several readings  → more concerning than a one-off
- Readings with low SQI in the window      → factor in measurement unreliability\
"""


def _format_window(window: list[dict]):
    if not window:
        yield "  (no historical data)"
        return
    # Cap at 20 readings to keep the prompt manageable
    for v in window[-20:]:
        yield (
            f"  HR={v.get('heart_rate'):>5}  "
            f"SpO2={v.get('spo2'):>5}%  "
            f"BP={v.get('bp_systolic'):>5} mmHg  "
            f"SQI={v.get('signal_quality_index'):.2f}"
        )


def _parse_assessment(text: str) -> dict:
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise ValueError(f"No JSON object found in LLM output: {text!r}")
    return json.loads(match.group())


class ClinicalAgent:
    def __init__(self) -> None:
        self._client = ollama.Client(host=settings.ollama_host)

    def analyze(self, event: TriageEvent, vitals_window: list[dict]) -> ClinicalAssessment:
        tv = event.trigger_vitals
        window_lines = "\n".join(_format_window(vitals_window))

        user_msg = (
            f"Patient: {event.patient_id}\n"
            f"Triggering reading:\n"
            f"  HR={tv.heart_rate}  SpO2={tv.spo2}%  "
            f"BP={tv.bp_systolic} mmHg  SQI={tv.signal_quality_index:.2f}\n\n"
            f"Last 10 minutes of vitals (oldest → newest):\n"
            f"{window_lines}\n\n"
            f"Is this a medical emergency or a false alarm?"
        )

        response = self._client.chat(
            model=settings.ollama_model,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user",   "content": user_msg},
            ],
            format="json",
        )

        # ollama package returns a plain dict, not an object
        content = response["message"]["content"]
        raw = _parse_assessment(content)
        score = max(1, min(10, int(raw["criticality_score"])))

        return ClinicalAssessment(
            criticality_score=score,
            reasoning=str(raw["reasoning"]),
            is_suppressed=score <= 7,
        )
