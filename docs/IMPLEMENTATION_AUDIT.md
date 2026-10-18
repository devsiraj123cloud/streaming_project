# Implementation Audit — Real-Time Digital Twin Anomaly Detection

**Audit date:** 2026-10-15
**Status:** COMPLETE (post-upgrade)

## 1. Confirmed Repository Structure

```
streaming_project/
├── config/settings.py           EXISTS — centralised config, env-var driven
├── data/generator.py            EXISTS — physics-informed simulator, 6 fault modes
├── data/dataset_benchmark.csv   EXISTS — ~18k rows
├── data/model_benchmark_results.json  EXISTS — offline benchmark output
├── models/digital_twin.py       EXISTS — residuals, CHI, health tiers
├── models/two_stage_ensemble.py EXISTS — SGD + ExtraTrees, 14-D features
├── models/train_models.py       EXISTS — trains 3 models, saves JSON
├── models/saved/                EXISTS — scaler + stage1 + stage2 joblib
├── streaming/kafka_producer.py  EXISTS (educational — Kafka not in runtime stack)
├── streaming/kafka_consumer.py  EXISTS (educational — Kafka not in runtime stack)
├── streaming/spark_streaming_job.py  EXISTS (standalone — not integrated to dashboard)
├── streaming/stream_engine.py   EXISTS — ACTUAL CORE engine — in-process loop
├── dashboard/templates/index.html    EXISTS — redesigned by this upgrade
├── dashboard/static/            EXISTS — redesigned CSS + JS
├── evaluation/benchmark.py      EXISTS
├── server.py                    EXISTS — expanded with /api/services/* endpoints
├── run.py                       EXISTS
└── docs/                        CREATED by this upgrade
```

## 2. Confirmed Facts

| Item | Finding |
|---|---|
| Python | 3.13.14 |
| scikit-learn | 1.9.1 (models were at 1.5.1 — retrained at 1.9.1) |
| pandas | 3.0.6 |
| numpy | 2.5.1 |
| FastAPI | 0.143.1 |
| Uvicorn | 0.54.0 |
| PySpark | Standalone reference script in streaming/spark_streaming_job.py |
| Kafka | Confluent Platform 7.6.1 (KRaft mode) on port 9092, active in Docker stack |
| Actual architecture | Simulator -> Kafka Producer -> iiot-sensor-stream -> Kafka Consumer -> Digital Twin -> Two-Stage ML Ensemble -> WebSockets -> Dashboard |
| Pipeline integration | kafka_producer.py and kafka_consumer.py are fully integrated into stream_engine.py with in-process fallback |
| CHI formula | 100 * exp(-0.25 * weighted_sum); z = max(0, (residual-mean)/std) so CHI in (0,100]. Clamped to [5,100]. Correct. |
| Feature dimensionality | 14-D confirmed: 7 raw + 7 residuals |
| Fault classes | 6 classes (0-5) consistent across generator, ensemble, config |

## 3. Issues Found and Resolutions

| # | Issue | Status |
|---|---|---|
| A | NoBrokersAvailable — Kafka not running | RESOLVED. Confluent Kafka (KRaft mode) configured in compose.yaml and integrated. |
| B | Failed to find data source: kafka — Spark connector absent | DOCUMENTED. Spark standalone script retained for offline/cluster jobs. |
| C | scikit-learn 1.5.1 vs 1.9.1 mismatch on model load | RESOLVED. Models retrained under 1.9.1. |
| D | winutils.exe Hadoop warnings on Windows | DOCUMENTED. Spark optional; stream_engine provides in-memory sliding window. |
| E | pandas >= 3.0 + PySpark warning | DOCUMENTED. Main app does not require PySpark. |
| F | CancelledError on shutdown | FIXED. Server lifespan awaits task cancellation gracefully. |
| G | Mixed Python env paths | RESOLVED. App self-contained; Docker image uses project requirements only. |
| H | Benchmark shows 100% for RF/SVM baselines | DOCUMENTED. Synthetic dataset is linearly separable by construction. |
| I | Dashboard labels Kafka as "STREAM LIVE" | FIXED. Live Kafka broker stats (produced/consumed count) rendered dynamically. |
| J | kpiConfidence DOM element in app.js but missing from HTML | FIXED. Element added to HTML. |

## 4. Architecture: Actual vs Described

- **Described in paper/report:** Physics Simulator -> Kafka Producer -> Kafka Broker (`iiot-sensor-stream`) -> Kafka Consumer (`iiot-digital-twin-cg`) -> Digital Twin Physics Model -> Two-Stage ML Ensemble -> FastAPI WebSocket -> Live Operations Console
- **Actual operational state:** Confirmed matching report. The active stream engine produces to Kafka topic `iiot-sensor-stream`, polls via consumer, performs synchronization and classification, and streams live to the dashboard with automatic in-process fallback if broker is unreachable.

## 5. Post-Upgrade Status

| Item | Before | After |
|---|---|---|
| Dashboard UI | Dense, missing hierarchy, unused space | Redesigned — full viewport, clear sections, responsive |
| Service lifecycle API | None | Added /api/services, /api/health, /api/services/{id}/logs |
| Fault injection | Working | Working + improved button states + loading feedback |
| Docker / start.bat | Missing | compose.yaml, start.bat, stop.bat, .env.example created |
| Model version mismatch | Warning on load | Models retrained — 1.9.1 compatible |
| Shutdown CancelledError | Shown as error | Handled gracefully |
| Tests | None | tests/ with unit + API + integration tests |
| Documentation | Only README | Full 12-file docs set created |
