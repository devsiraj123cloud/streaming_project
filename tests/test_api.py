"""
Integration tests for FastAPI REST and Service Lifecycle endpoints.
"""

import pytest
from fastapi.testclient import TestClient
from server import app

client = TestClient(app)

def test_api_health():
    res = client.get("/api/health")
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "healthy"
    assert data["service"] == "digital-twin-dashboard"
    assert "uptime_sec" in data

def test_api_status():
    res = client.get("/api/status")
    assert res.status_code == 200
    data = res.json()
    assert "asset_id" in data
    assert "health_index" in data

def test_api_services():
    res = client.get("/api/services")
    assert res.status_code == 200
    data = res.json()
    assert "services" in data
    ids = [s["id"] for s in data["services"]]
    assert "stream-engine" in ids
    assert "kafka-broker" in ids
    assert "spark-job" in ids

def test_api_service_lifecycle():
    # Stop
    res_stop = client.post("/api/services/stream-engine/stop")
    assert res_stop.status_code == 200
    assert res_stop.json()["status"] in ["stopped", "already_stopped"]

    # Start
    res_start = client.post("/api/services/stream-engine/start")
    assert res_start.status_code == 200
    assert res_start.json()["status"] in ["started", "already_running"]

    # Restart
    res_restart = client.post("/api/services/stream-engine/restart")
    assert res_restart.status_code == 200
    assert res_restart.json()["status"] == "restarted"

def test_api_service_logs():
    res = client.get("/api/services/stream-engine/logs?tail=10")
    assert res.status_code == 200
    assert "lines" in res.json()

    res_app = client.get("/api/services/app/logs?tail=10")
    assert res_app.status_code == 200
    assert "lines" in res_app.json()

def test_api_telemetry():
    res_latest = client.get("/api/telemetry/latest")
    assert res_latest.status_code == 200

    res_alerts = client.get("/api/telemetry/alerts")
    assert res_alerts.status_code == 200
    assert "alerts" in res_alerts.json()

def test_api_fault_injection():
    # Inject fault 2
    res = client.post("/api/fault/inject", json={"fault_code": 2, "intensity": 0.85})
    assert res.status_code == 200
    assert res.json()["status"] == "success"
    assert res.json()["fault_code"] == 2

    # Reset fault
    res_reset = client.post("/api/fault/reset")
    assert res_reset.status_code == 200
    assert res_reset.json()["status"] == "success"

def test_api_fault_injection_validation():
    # Invalid code
    res = client.post("/api/fault/inject", json={"fault_code": 99, "intensity": 0.5})
    assert res.status_code == 422

def test_api_benchmark():
    res = client.get("/api/benchmark")
    assert res.status_code == 200
    data = res.json()
    assert "_provenance" in data
    assert data["_provenance"] == "offline-benchmark"

def test_dashboard_html():
    res = client.get("/")
    assert res.status_code == 200
    assert "text/html" in res.headers["content-type"]
    assert "IOE413" in res.text or "Digital Twin" in res.text

def test_api_kafka_status():
    res = client.get("/api/kafka/status")
    assert res.status_code == 200
    data = res.json()
    assert "active" in data
    assert "topic" in data
    assert "bootstrap_servers" in data

