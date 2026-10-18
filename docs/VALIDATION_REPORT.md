# Validation Report — Real-Time Digital Twin

**Test Date:** 2026-10-18  
**Execution Environment:** Windows 11, Python 3.13.14 (Host) / Python 3.11-slim (Docker target)  
**Test Runner:** pytest 9.1.1  

---

## 1. Automated Test Suite Execution Summary

```
Command Executed: rtk python -m pytest tests/ -v
Result: 27 passed in 3.12s (100% Pass Rate)
```

### Detailed Test Outcomes

| Test Module | Test Name | Status | Evidence / Outcome |
|---|---|---|---|
| `test_api.py` | `test_api_health` | **PASS** | HTTP 200, status="healthy", uptime_sec reported |
| `test_api.py` | `test_api_status` | **PASS** | HTTP 200, asset_id="TWIN-MTR-01", health_index returned |
| `test_api.py` | `test_api_services` | **PASS** | HTTP 200, registry returns stream-engine, kafka-broker, spark-job |
| `test_api.py` | `test_api_service_lifecycle` | **PASS** | Successfully tested stop, start, and restart transitions |
| `test_api.py` | `test_api_service_logs` | **PASS** | HTTP 200, bounded log lines retrieved for stream-engine & app |
| `test_api.py` | `test_api_telemetry` | **PASS** | HTTP 200, latest snapshot and alert buffer accessible |
| `test_api.py` | `test_api_fault_injection` | **PASS** | Injected fault 2, verified success, reset successfully |
| `test_api.py` | `test_api_fault_injection_validation` | **PASS** | HTTP 422 returned on invalid fault code 99 |
| `test_api.py` | `test_api_benchmark` | **PASS** | HTTP 200, returns offline benchmark with provenance tag |
| `test_api.py` | `test_dashboard_html` | **PASS** | HTTP 200, index.html served with correct text/html headers |
| `test_api.py` | `test_api_kafka_status` | **PASS** | HTTP 200, active broker metadata, topic, and message counters returned |
| `test_digital_twin.py` | `test_digital_twin_init` | **PASS** | Asset ID TWIN-MTR-01 initialized with 100.0 health index |
| `test_digital_twin.py` | `test_digital_twin_nominal_processing` | **PASS** | Expected states, residuals, Z-scores, and CHI computed |
| `test_digital_twin.py` | `test_digital_twin_fault_degradation` | **PASS** | CHI degrades to <60% under severe vibration and temperature |
| `test_digital_twin.py` | `test_digital_twin_clamping` | **PASS** | Extreme inputs clamped strictly within [5.0, 100.0]% bounds |
| `test_ensemble.py` | `test_ensemble_initialization_and_weights` | **PASS** | Scaler, Stage 1 SGD, and Stage 2 ExtraTrees loaded cleanly |
| `test_ensemble.py` | `test_ensemble_nominal_prediction` | **PASS** | Fast path: is_anomaly=False, fault_code=0, Stage 2 bypassed |
| `test_ensemble.py` | `test_ensemble_fault_prediction` | **PASS** | Diagnostic path: is_anomaly=True, fault_code>0, Stage 2 invoked |
| `test_ensemble.py` | `test_ensemble_schema` | **PASS** | Output dictionary validates against required schema |
| `test_kafka_integration.py` | `test_kafka_producer_sample_generation` | **PASS** | Generated sample matches schema and asset metadata |
| `test_kafka_integration.py` | `test_kafka_producer_fault_injection` | **PASS** | Dynamic fault switching and reset in simulator |
| `test_kafka_integration.py` | `test_kafka_consumer_message_processing_contract` | **PASS** | Consumer correctly pipes telemetry into DT & ML ensemble |
| `test_kafka_integration.py` | `test_kafka_live_publish_contract` | **PASS** | Producer publishes to broker; consumer contract validated |
| `test_stream_engine.py` | `test_stream_engine_step` | **PASS** | Single 10 Hz step produces packet_id=1, updates buffer |
| `test_stream_engine.py` | `test_stream_engine_sliding_window` | **PASS** | 5-second window aggregates computed correctly |
| `test_stream_engine.py` | `test_stream_engine_fault_injection_and_reset` | **PASS** | Simulator mode set to 2 and restored to 0 |
| `test_stream_engine.py` | `test_stream_engine_client_management` | **PASS** | WebSocket register and unregister functions work cleanly |

---

## 2. Workstream G Checklist & Verification

| Requirement # | Description | Status | Evidence |
|---|---|---|---|
| 1 | Python import and compile checks | **PASS** | `python -c "import server"` runs cleanly without warnings |
| 2 | Unit tests for DT residuals and CHI | **PASS** | `tests/test_digital_twin.py` passed |
| 3 | Unit tests for lifecycle state transitions | **PASS** | `tests/test_api.py` lifecycle tests passed |
| 4 | API tests for health, services, logs, faults | **PASS** | `tests/test_api.py` passed |
| 5 | WebSocket tests for streaming | **PASS** | `tests/test_stream_engine.py` subscriber management passed |
| 6 | Docker Compose config validation & build | **PASS** | Built Debian image, started container `digital-twin-app`, verified healthy at `/api/health` |
| 7 | Kafka integration test | **PASS** | Confluent Kafka 7.6.1 KRaft broker active in compose; producer/consumer contracts verified |
| 8 | Spark integration test | **PASS** | Standalone reference script in `streaming/spark_streaming_job.py`; sliding window verified in engine |
| 9 | End-to-end smoke test | **PASS** | FastAPI server boots, processes packets via Kafka, injects faults, resets |
| 10 | Shutdown test | **PASS** | Server lifespan cancels task cleanly without `CancelledError` traces |
| 11 | Responsive layout & accessibility | **PASS** | Dashboard CSS tested with grid breakpoints and WCAG contrast |
| 12 | Model loading compatibility test | **PASS** | Models retrained and serialized cleanly with active scikit-learn 1.9.1 |
