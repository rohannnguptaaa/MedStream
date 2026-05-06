"""
Shared JSON logger.

Usage:
    from logger import get_logger
    log = get_logger("sentinel")
    log.info("triage_dispatched", extra={"ctx": {"patient_id": "P001", "trace_id": "...", "latency_ms": 2.1}})

Every log line is a single JSON object:
    {"ts": "...", "level": "INFO", "service": "sentinel", "msg": "triage_dispatched", "ctx": {...}}

The "ctx" key carries all domain fields so they never clash with standard
LogRecord attributes and are trivially queryable in any log aggregator.
"""

import json
import logging
import sys
from datetime import datetime, timezone


class _JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out: dict = {
            "ts":      datetime.now(timezone.utc).isoformat(),
            "level":   record.levelname,
            "service": record.name,
            "msg":     record.getMessage(),
        }
        ctx = getattr(record, "ctx", None)
        if ctx:
            out["ctx"] = ctx
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return json.dumps(out)


def get_logger(service: str) -> logging.Logger:
    logger = logging.getLogger(service)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JSONFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger
