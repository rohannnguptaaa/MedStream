"""
Nursing Station API
-------------------
Endpoints:
  GET  /alerts/{patient_id}  — recent alert history from MongoDB
  GET  /metrics              — false-alarm suppression rate (last 24 h)
  WS   /ws/alerts            — real-time push for Score > 7 alerts

WebSocket delivery mechanism:
  The agent worker publishes critical alerts to Redis pub/sub channel
  `critical_alerts`. A background asyncio task subscribes to that channel
  and fans out to all connected WebSocket clients via ConnectionManager.
  This avoids polling and requires no extra infrastructure beyond Redis,
  which is already present.
"""

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path

import redis
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import start_http_server
from pydantic import BaseModel

from api.mongo_client import MongoAlertClient
from api.websocket import manager
from config import settings
from logger import get_logger

log = get_logger("api")

_STATIC     = Path(__file__).parent / "static"
_mongo      = MongoAlertClient()
_redis_sync = redis.Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True)


class BaselineUpdate(BaseModel):
    hr_min:   float
    hr_max:   float
    spo2_min: float
    bp_min:   float
    bp_max:   float


async def _subscribe_and_broadcast() -> None:
    """Background task: relay Redis pub/sub messages to WebSocket clients."""
    r      = aioredis.Redis(host=settings.redis_host, port=settings.redis_port)
    pubsub = r.pubsub()
    await pubsub.subscribe("critical_alerts")

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue
        try:
            data = json.loads(message["data"])
            await manager.broadcast(data)
        except Exception as exc:
            log.error("ws_broadcast_error", extra={"ctx": {"error": str(exc)}})


@asynccontextmanager
async def lifespan(app: FastAPI):
    start_http_server(9100)
    log.info("started")
    task = asyncio.create_task(_subscribe_and_broadcast())
    yield
    task.cancel()
    log.info("shutting_down")


app = FastAPI(title="Clinical Alerting System — Nursing Station", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=_STATIC), name="static")


@app.get("/", include_in_schema=False)
def dashboard() -> FileResponse:
    return FileResponse(_STATIC / "index.html")


@app.get("/alerts/{patient_id}")
def get_patient_alerts(patient_id: str, limit: int = 20) -> list[dict]:
    return _mongo.get_patient_alerts(patient_id, limit)


@app.get("/metrics")
def get_metrics() -> dict:
    return _mongo.get_suppression_metrics()


@app.get("/admin/baselines")
def get_baselines() -> list[dict]:
    return _mongo.get_all_baselines()


@app.put("/admin/baselines/{patient_id}")
def update_baseline(patient_id: str, update: BaselineUpdate) -> dict:
    data = update.model_dump()
    _mongo.upsert_baseline(patient_id, data)
    _redis_sync.hset(f"baseline:{patient_id}", mapping={k: str(v) for k, v in data.items()})
    log.info("baseline_updated", extra={"ctx": {"patient_id": patient_id}})
    return {"status": "updated", "patient_id": patient_id}


@app.websocket("/ws/alerts")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
