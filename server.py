"""
FastAPI Server — Real-Time Digital Twin Anomaly Detection Dashboard.
Expanded with service lifecycle API, health endpoints, and log access.
IOE413 Streaming & Data Analytics | Mini Project Team 10
"""

import asyncio
import json
import logging
import time
import sys
import os
from pathlib import Path
from collections import deque
from typing import Any, Dict, Optional

base_dir = Path(__file__).resolve().parent
if str(base_dir) not in sys.path:
    sys.path.insert(0, str(base_dir))

from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from config.settings import STATIC_DIR, TEMPLATES_DIR, DATA_DIR, HOST, PORT, KAFKA_ENABLED
from streaming.stream_engine import RealTimeStreamingEngine
from streaming.kafka_producer import IIoTKafkaProducer

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
logger = logging.getLogger("DigitalTwinServer")

# ------------------------------------------------------------------
# Log buffer for in-app log viewer (bounded, thread-safe-enough for single async loop)
# ------------------------------------------------------------------
_LOG_BUFFER: deque = deque(maxlen=200)

class _BufferHandler(logging.Handler):
    def emit(self, record: logging.LogRecord):
        _LOG_BUFFER.append({
            "ts": time.strftime("%H:%M:%S", time.localtime(record.created)),
            "level": record.levelname,
            "name": record.name,
            "msg": self.format(record),
        })

_buf_handler = _BufferHandler()
_buf_handler.setLevel(logging.DEBUG)
logging.getLogger().addHandler(_buf_handler)

# ------------------------------------------------------------------
# Stream engine (the actual real-time processing component)
# ------------------------------------------------------------------
stream_engine = RealTimeStreamingEngine(interval_sec=0.1)
_kafka_producer: Optional[IIoTKafkaProducer] = None
_kafka_connected = False
_streaming_task: Optional[asyncio.Task] = None
_engine_start_time: float = 0.0
_engine_log: deque = deque(maxlen=50)


# ------------------------------------------------------------------
# Request/Response models
# ------------------------------------------------------------------
class FaultInjectionRequest(BaseModel):
    fault_code: int = Field(..., ge=0, le=5, description="Fault class 0-5")
    intensity: float = Field(1.0, ge=0.1, le=1.0, description="Severity 0.1-1.0")


class ServiceActionResponse(BaseModel):
    service_id: str
    action: str
    status: str
    message: str


# ------------------------------------------------------------------
# Application lifespan
# ------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _streaming_task, _engine_start_time, _kafka_producer, _kafka_connected
    _engine_start_time = time.time()
    _engine_log.append(f"[{time.strftime('%H:%M:%S')}] Streaming engine started")
    _streaming_task = asyncio.create_task(stream_engine.run_loop())
    if KAFKA_ENABLED:
        _kafka_producer = IIoTKafkaProducer()
        _kafka_connected = _kafka_producer.is_connected()
        if not _kafka_connected:
            logger.warning(
                "[Server] Kafka is enabled but unavailable; using the in-process pipeline."
            )
    logger.warning(f"[Server] Digital Twin streaming started on http://{HOST}:{PORT}")
    yield
    # --- Shutdown ---
    stream_engine.stop()
    if _kafka_producer:
        _kafka_producer.stop()
    if _streaming_task and not _streaming_task.done():
        _streaming_task.cancel()
        try:
            await asyncio.wait_for(_streaming_task, timeout=3.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
    logger.warning("[Server] Streaming engine stopped cleanly.")


app = FastAPI(
    title="IOE413 Real-Time Digital Twin Anomaly Detection",
    description="Digital Twin and Two-Stage ML Ensemble for Streaming Anomaly Detection (Ranpariya & Sharma 2025)",
    version="2.0.0",
    lifespan=lifespan,
)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ------------------------------------------------------------------
# Dashboard
# ------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def get_dashboard():
    """Serves the main dashboard."""
    index_file = TEMPLATES_DIR / "index.html"
    return HTMLResponse(content=index_file.read_text(encoding="utf-8"))


# ------------------------------------------------------------------
# WebSocket stream
# ------------------------------------------------------------------
@app.websocket("/ws/stream")
async def websocket_stream_endpoint(websocket: WebSocket):
    """Real-time bi-directional telemetry stream."""
    await websocket.accept()
    # Send last 30 packets so charts populate immediately
    for p in list(stream_engine.recent_buffer)[-30:]:
        try:
            await websocket.send_json(p)
        except Exception:
            return

    stream_engine.register_client(websocket)
    try:
        while True:
            # Keep connection alive; actual data pushed from run_loop
            await asyncio.sleep(0.5)
    except (WebSocketDisconnect, asyncio.CancelledError):
        pass
    except Exception as exc:
        logger.warning(f"[WS] Unexpected error: {exc}")
    finally:
        stream_engine.unregister_client(websocket)


# ------------------------------------------------------------------
# Health & Status
# ------------------------------------------------------------------
@app.get("/api/health")
async def api_health():
    """Dashboard/API health and application readiness."""
    return {
        "status": "healthy",
        "service": "digital-twin-dashboard",
        "uptime_sec": round(time.time() - _engine_start_time, 1),
        "stream_running": stream_engine.is_running,
        "packets_processed": stream_engine.packet_counter,
        "kafka_enabled": KAFKA_ENABLED,
        "kafka_consumer_error": str(stream_engine.kafka_consumer.error) if stream_engine.kafka_consumer and stream_engine.kafka_consumer.error else None,
        "active_ws_clients": len(stream_engine.subscribers),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


@app.get("/api/status")
async def get_system_status():
    """Current live snapshot — used for HTTP polling fallback."""
    return {
        "asset_id": stream_engine.digital_twin.asset_id,
        "packet_counter": stream_engine.packet_counter,
        "health_index": stream_engine.digital_twin.health_index,
        "active_subscribers": len(stream_engine.subscribers),
        "stream_running": stream_engine.is_running,
        "latest_packet": stream_engine.last_packet,
    }


# ------------------------------------------------------------------
# Service Registry
# ------------------------------------------------------------------
def _build_services_response() -> Dict[str, Any]:
    """Returns the service registry with real observed states."""
    stream_state = "healthy" if stream_engine.is_running else "stopped"
    kafka_info = stream_engine.get_kafka_info()
    kafka_state = "healthy" if kafka_info["active"] else "stopped"
    kafka_desc = f"KRaft message broker (topic: {kafka_info['topic']})" if kafka_info["active"] else "Message broker (offline — using in-process pipeline)"
    kafka_note = f"Active ({kafka_info['bootstrap_servers']}) | Published: {kafka_info['messages_produced']} | Consumed: {kafka_info['messages_consumed']}" if kafka_info["active"] else "Start Kafka on 127.0.0.1:9092 to enable broker mode"

    return {
        "services": [
            {
                "id": "stream-engine",
                "name": "Real-Time Stream Engine",
                "description": "Continuous telemetry simulator + digital twin + ML inference",
                "state": stream_state,
                "controllable": True,
                "uptime_sec": round(time.time() - _engine_start_time, 1) if stream_engine.is_running else 0,
                "packets": stream_engine.packet_counter,
                "ws_clients": len(stream_engine.subscribers),
            },
            {
                "id": "kafka-broker",
                "name": "Apache Kafka Broker",
                "description": kafka_desc,
                "state": kafka_state,
                "controllable": True,
                "bootstrap_servers": kafka_info["bootstrap_servers"],
                "topic": kafka_info["topic"],
                "messages_produced": kafka_info["messages_produced"],
                "messages_consumed": kafka_info["messages_consumed"],
                "note": kafka_note,
            },
            {
                "id": "spark-job",
                "name": "Spark Structured Streaming Job",
                "description": "Windowed analytics job (sliding window equivalent active in stream engine)",
                "state": "healthy" if stream_engine.is_running else "stopped",
                "controllable": False,
                "note": "5-second window, 1-second slide rolling analytics active",
            },
        ]
    }


@app.get("/api/services")
async def get_services():
    """Service registry with actual observed states."""
    return _build_services_response()


@app.get("/api/kafka/status")
async def get_kafka_status():
    """Returns detailed Kafka status, topics, and counters."""
    return stream_engine.get_kafka_info()


@app.post("/api/services/{service_id}/start")
async def start_service(service_id: str):
    global _streaming_task, _engine_start_time
    if service_id == "stream-engine":
        if stream_engine.is_running:
            return {"service_id": service_id, "action": "start", "status": "already_running", "message": "Stream engine is already running"}
        _engine_start_time = time.time()
        _streaming_task = asyncio.create_task(stream_engine.run_loop())
        _engine_log.append(f"[{time.strftime('%H:%M:%S')}] Stream engine started via API")
        return {"service_id": service_id, "action": "start", "status": "started", "message": "Stream engine started"}
    elif service_id == "kafka-broker":
        try:
            from streaming.kafka_producer import IIoTKafkaProducer
            from streaming.kafka_consumer import IIoTKafkaConsumer
            from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_TELEMETRY
            stream_engine.kafka_producer = IIoTKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC_TELEMETRY)
            stream_engine.kafka_consumer = IIoTKafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC_TELEMETRY)
            stream_engine.kafka_active = bool(stream_engine.kafka_producer.producer is not None)
            st = "started" if stream_engine.kafka_active else "unreachable"
            return {"service_id": service_id, "action": "start", "status": st, "message": f"Kafka broker connection status: {st}"}
        except Exception as e:
            return {"service_id": service_id, "action": "start", "status": "error", "message": str(e)}
    raise HTTPException(status_code=404, detail=f"Unknown service '{service_id}' or not controllable")


@app.post("/api/services/{service_id}/stop")
async def stop_service(service_id: str):
    global _streaming_task
    if service_id == "stream-engine":
        if not stream_engine.is_running:
            return {"service_id": service_id, "action": "stop", "status": "already_stopped", "message": "Stream engine is already stopped"}
        stream_engine.stop()
        if _streaming_task and not _streaming_task.done():
            _streaming_task.cancel()
            try:
                await asyncio.wait_for(_streaming_task, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        _engine_log.append(f"[{time.strftime('%H:%M:%S')}] Stream engine stopped via API")
        return {"service_id": service_id, "action": "stop", "status": "stopped", "message": "Stream engine stopped"}
    elif service_id == "kafka-broker":
        if stream_engine.kafka_producer:
            stream_engine.kafka_producer.stop()
        if stream_engine.kafka_consumer:
            stream_engine.kafka_consumer.stop()
        stream_engine.kafka_active = False
        return {"service_id": service_id, "action": "stop", "status": "stopped", "message": "Kafka streaming disconnected"}
    raise HTTPException(status_code=404, detail=f"Unknown service '{service_id}' or not controllable")


@app.post("/api/services/{service_id}/restart")
async def restart_service(service_id: str):
    global _streaming_task, _engine_start_time
    if service_id == "stream-engine":
        # Stop
        stream_engine.stop()
        if _streaming_task and not _streaming_task.done():
            _streaming_task.cancel()
            try:
                await asyncio.wait_for(_streaming_task, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        await asyncio.sleep(0.2)
        # Restart
        _engine_start_time = time.time()
        _streaming_task = asyncio.create_task(stream_engine.run_loop())
        _engine_log.append(f"[{time.strftime('%H:%M:%S')}] Stream engine restarted via API")
        return {"service_id": service_id, "action": "restart", "status": "restarted", "message": "Stream engine restarted"}
    elif service_id == "kafka-broker":
        if stream_engine.kafka_producer:
            stream_engine.kafka_producer.stop()
        if stream_engine.kafka_consumer:
            stream_engine.kafka_consumer.stop()
        await asyncio.sleep(0.2)
        from streaming.kafka_producer import IIoTKafkaProducer
        from streaming.kafka_consumer import IIoTKafkaConsumer
        from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_TELEMETRY
        stream_engine.kafka_producer = IIoTKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC_TELEMETRY)
        stream_engine.kafka_consumer = IIoTKafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC_TELEMETRY)
        stream_engine.kafka_active = bool(stream_engine.kafka_producer.producer is not None)
        st = "restarted" if stream_engine.kafka_active else "unreachable"
        return {"service_id": service_id, "action": "restart", "status": st, "message": f"Kafka broker connection status: {st}"}

    raise HTTPException(status_code=404, detail=f"Unknown service '{service_id}' or not controllable")


@app.get("/api/services/{service_id}/logs")
async def get_service_logs(service_id: str, tail: int = 30):
    """Return last N sanitized log lines for a service."""
    tail = max(1, min(tail, 100))
    if service_id == "stream-engine":
        lines = list(_engine_log)[-tail:]
        return {"service_id": service_id, "lines": lines, "total": len(_engine_log)}
    if service_id == "app":
        logs = list(_LOG_BUFFER)[-tail:]
        return {"service_id": service_id, "lines": [f"[{l['ts']}] {l['level']} {l['msg']}" for l in logs], "total": len(_LOG_BUFFER)}
    raise HTTPException(status_code=404, detail=f"Unknown service '{service_id}'")


# ------------------------------------------------------------------
# Telemetry
# ------------------------------------------------------------------
@app.get("/api/telemetry/latest")
async def get_latest_telemetry():
    """Latest real telemetry snapshot."""
    return stream_engine.last_packet or {"status": "no_data", "message": "Stream not yet started or no packets received"}


@app.get("/api/telemetry/alerts")
async def get_recent_alerts():
    """Recent anomaly alerts from the alert buffer."""
    return {"alerts": list(stream_engine.recent_alerts)}


# ------------------------------------------------------------------
# Fault control
# ------------------------------------------------------------------
@app.post("/api/fault/inject")
async def inject_fault_endpoint(req: FaultInjectionRequest):
    """Injects a specific fault scenario into the live stream."""
    stream_engine.inject_fault(req.fault_code, req.intensity)
    _engine_log.append(f"[{time.strftime('%H:%M:%S')}] Fault injected: code={req.fault_code} intensity={req.intensity:.2f}")
    return {
        "status": "success",
        "fault_code": req.fault_code,
        "intensity": req.intensity,
        "message": f"Injected fault {req.fault_code} at {req.intensity * 100:.0f}% intensity",
    }


@app.post("/api/fault/reset")
async def reset_fault_endpoint():
    """Resets the simulator to normal operation."""
    stream_engine.reset_fault()
    _engine_log.append(f"[{time.strftime('%H:%M:%S')}] Fault reset — normal operation restored")
    return {"status": "success", "message": "Restored normal operating conditions"}


# ------------------------------------------------------------------
# Benchmark / evaluation
# ------------------------------------------------------------------
@app.get("/api/benchmark")
async def get_benchmark_results():
    """Returns offline model benchmark metrics."""
    benchmark_file = DATA_DIR / "model_benchmark_results.json"
    if benchmark_file.exists():
        with open(benchmark_file, "r") as f:
            data = json.load(f)
        data["_provenance"] = "offline-benchmark"
        data["_note"] = "These are offline evaluation metrics from the synthetic dataset. Not live measurements."
        return data
    return {
        "status": "pending",
        "message": "Run models/train_models.py to generate benchmarks",
        "_provenance": "offline-benchmark",
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host=HOST, port=PORT, reload=False)
