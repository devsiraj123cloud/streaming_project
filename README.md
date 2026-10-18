# Real-Time Digital Twin for Streaming-Based Anomaly Detection

**IOE413: Streaming & Data Analytics — Mini Project**
**Team 10:** Mohammed Sirajudheen (2023BCS0132), Rohit Pratap Singh (2023BCS0138), Amit Kumar (2023BCS0210), and Kartik P (2023BCD0051)

This project implements a real-time Industrial Internet of Things (IIoT) monitoring pipeline for an induction motor and centrifugal pump. A physics-informed sensor simulator produces telemetry, a Digital Twin calculates expected values and residuals, and a two-stage machine-learning ensemble detects and classifies faults. Results are exposed through a FastAPI REST/WebSocket service and displayed in a browser dashboard.

The implementation is an academic prototype using synthetic telemetry. Benchmark values are offline results from the included synthetic dataset and must not be interpreted as measurements from a physical production asset.

## 1. Complete Pipeline

```text
IIoT Sensor Simulator
        |
        | 10 Hz telemetry
        v
Kafka topic: iiot-sensor-stream  -----------+
        |                                    |
        v                                    v
Kafka Consumer                         Spark Structured Streaming
        |                              5-second window / 1-second slide
        v
Digital Twin
  - expected physical state
  - residuals and z-scores
  - Composite Health Index (CHI)
        |
        v
Two-Stage ML Ensemble
  Stage 1: Normal vs anomaly screening
  Stage 2: Six-class fault diagnosis
        |
        +--> REST API
        +--> WebSocket: /ws/stream
        +--> anomaly alert buffer
        v
Operations Dashboard
```

### Processing sequence

1. [`data/generator.py`](data/generator.py) generates vibration, temperature, RPM, current, pressure, and acoustic readings.
2. The simulator applies one of six states: normal, bearing fault, stator overheating, rotor imbalance, cavitation, or sensor drift.
3. In Docker, telemetry is published to Kafka topic `iiot-sensor-stream`.
4. The Kafka consumer sends each record to [`models/digital_twin.py`](models/digital_twin.py).
5. The Digital Twin estimates the healthy state, computes residuals, normalizes them, and calculates CHI.
6. [`models/two_stage_ensemble.py`](models/two_stage_ensemble.py) uses 14 features: seven raw sensor values and seven Digital Twin residuals.
7. Stage 1 screens for an anomaly. Stage 2 runs only when Stage 1 flags an anomaly and assigns fault class, confidence, severity, and latency.
8. [`streaming/stream_engine.py`](streaming/stream_engine.py) maintains the rolling buffer, alerts, and five-second in-process window metrics.
9. [`server.py`](server.py) exposes REST endpoints and broadcasts packets to dashboard clients over WebSocket.
10. [`dashboard/`](dashboard/) renders the live KPI cards, charts, Digital Twin visualizer, fault controls, service controls, logs, alerts, and benchmark table.
11. [`streaming/spark_streaming_job.py`](streaming/spark_streaming_job.py) provides the Docker Spark Structured Streaming window aggregation over Kafka.

## 2. Repository Contents

| Path | Purpose |
|---|---|
| `server.py` | FastAPI application, REST API, WebSocket endpoint, lifecycle controls |
| `run.py` | Local one-command runner; verifies models and launches Uvicorn |
| `data/generator.py` | Physics-informed synthetic IIoT telemetry and benchmark dataset generator |
| `models/digital_twin.py` | Expected-state model, residuals, z-scores, and CHI |
| `models/two_stage_ensemble.py` | Stage 1 anomaly screen and Stage 2 fault classifier |
| `models/train_models.py` | Dataset preparation, model training, and offline benchmark generation |
| `models/saved/` | Serialized scaler and trained model artifacts |
| `streaming/stream_engine.py` | Real-time orchestration and WebSocket broadcast |
| `streaming/kafka_producer.py` | Kafka telemetry producer with local fallback support |
| `streaming/kafka_consumer.py` | Kafka consumer and inference integration |
| `streaming/spark_streaming_job.py` | Spark Structured Streaming reference job |
| `dashboard/` | HTML, CSS, and JavaScript operations dashboard |
| `tests/` | API, Digital Twin, model, streaming, and Kafka contract tests |
| `Dockerfile` | Reproducible application image |
| `compose.yaml` | App, Kafka KRaft, and Spark services |
| `start.bat` / `stop.bat` | Windows Docker startup and shutdown launchers |
| `requirements.txt` | Python dependency requirements |

## 3. Tools and Versions Used

The following versions were used for the validated submission:

| Tool or library | Version used |
|---|---|
| Operating system | Windows 11 |
| Python host runtime | 3.13.14 |
| Python Docker runtime | 3.11-slim |
| FastAPI | 0.146.0 |
| Uvicorn | 0.54.0 |
| Pydantic | 2.14.1 |
| NumPy | 2.5.4 |
| Pandas | 3.0.6 |
| SciPy | 1.18.1 |
| scikit-learn | 1.9.1 |
| joblib | 1.6.0 |
| kafka-python-ng | 2.2.3 |
| pytest | 9.1.1 |
| Docker Compose | Docker Compose v2 (Docker Desktop) |
| Kafka image | `confluentinc/cp-kafka:7.6.1` |
| Spark image | `apache/spark:3.5.3` |
| Spark Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3` |
| Browser client | Modern browser with WebSocket and Canvas support |

The Python manifest intentionally specifies compatible minimum versions in [`requirements.txt`](requirements.txt). To record the exact environment used on another machine:

```powershell
python --version
python -m pip freeze
docker --version
docker compose version
```

## 4. Requirements

### Local Python execution

- Windows, macOS, or Linux
- Python 3.11 or newer recommended
- `pip`
- Approximately 1 GB of free disk space for Python packages and model artifacts

### Docker execution

- Docker Desktop with Docker Compose v2
- At least 4 GB of memory allocated to Docker Desktop
- Internet access during the first build to download images and the Spark Kafka connector

## 5. Setup and Execution

### Option A — Windows Docker launcher

This is the recommended full-pipeline execution path.

1. Start Docker Desktop.
2. Open the project folder:

   ```powershell
   cd "C:\Users\siraj\Desktop\New folder (11)\streaming_project"
   ```

3. Double-click [`start.bat`](start.bat), or run:

   ```powershell
   docker compose up -d --build
   ```

4. Open the dashboard:

   ```text
   http://localhost:8000
   ```

5. Check service status:

   ```powershell
   docker compose ps
   Invoke-RestMethod http://localhost:8000/api/health
   ```

6. View application and Spark logs:

   ```powershell
   docker compose logs -f app spark
   ```

7. Stop the complete stack with [`stop.bat`](stop.bat), or:

   ```powershell
   docker compose down
   ```

The Compose stack starts:

- `digital-twin-app`: FastAPI dashboard and streaming application on port `8000`
- `digital-twin-kafka`: single-node Kafka in KRaft mode
- `digital-twin-spark`: Spark Structured Streaming job

### Option B — Local Python execution

This mode runs the dashboard and in-process streaming engine without requiring Kafka or Spark.

```powershell
cd "C:\Users\siraj\Desktop\New folder (11)\streaming_project"
python -m pip install -r requirements.txt
python run.py
```

Open `http://127.0.0.1:8000`. Stop with `Ctrl+C`.

Models are loaded from `models/saved/`. If the model files are missing, `run.py` automatically runs the training pipeline before starting the server.

### Optional environment configuration

Copy [`.env.example`](.env.example) and set values in the shell or deployment environment. Important variables include:

| Variable | Default | Description |
|---|---|---|
| `HOST` | `127.0.0.1` | Web server bind address |
| `PORT` | `8000` | Web server port |
| `KAFKA_ENABLED` | `false` | Enables Kafka mode when set to `true` |
| `KAFKA_BOOTSTRAP_SERVERS` | `127.0.0.1:9092` | Kafka broker address |
| `WS_HEARTBEAT_INTERVAL` | `1.0` | Dashboard heartbeat interval |

Local Kafka example:

```powershell
$env:KAFKA_ENABLED="true"
$env:KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:9092"
python run.py
```

If Kafka is unavailable, the application uses the in-process streaming path rather than stopping the dashboard.

## 6. Dashboard and API

The dashboard provides:

- Live Composite Health Index, condition, latency, and packet counters
- Digital Twin visualizer and telemetry charts
- Bearing, overheating, rotor imbalance, cavitation, sensor drift, and reset controls
- Stage 1 and Stage 2 inference details
- Start, stop, and restart controls for the stream engine
- Recent application logs and anomaly alerts
- Offline benchmark comparison with provenance information

Useful endpoints:

| Method | Endpoint | Purpose |
|---|---|---|
| `GET` | `/api/health` | Readiness, runtime, packet, and Kafka state |
| `GET` | `/api/status` | Current CHI and latest packet |
| `GET` | `/api/telemetry/latest` | Latest processed telemetry packet |
| `GET` | `/api/telemetry/alerts` | Recent anomaly alerts |
| `GET` | `/api/services` | Service registry and observed states |
| `GET` | `/api/benchmark` | Offline benchmark metrics |
| `POST` | `/api/fault/inject` | Inject fault code `0`–`5` with intensity `0.1`–`1.0` |
| `POST` | `/api/fault/reset` | Return simulator to normal operation |
| `WS` | `/ws/stream` | Receive live processed packets |

Example fault injection:

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8000/api/fault/inject" `
  -ContentType "application/json" `
  -Body '{"fault_code":1,"intensity":0.85}'
```

Reset the simulator:

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8000/api/fault/reset"
```

## 7. Model Training and Benchmarking

To explicitly regenerate the synthetic dataset, train the Digital Twin ensemble, and write benchmark results:

```powershell
python -c "from models.train_models import run_training_pipeline; run_training_pipeline()"
```

This creates or updates:

- `data/dataset_benchmark.csv`
- `data/model_benchmark_results.json`
- `models/saved/scaler.joblib`
- `models/saved/stage1_model.joblib`
- `models/saved/stage2_model.joblib`

The proposed model uses 14 dimensions: seven raw sensor features plus seven Digital Twin residuals. Stage 1 is a binary anomaly screen; Stage 2 is a six-class diagnostic classifier.

## 8. Testing and Validation

Run the full automated suite:

```powershell
python -m pytest tests/ -v
```

The validated suite contains **28 passing tests** covering:

- Digital Twin initialization, residuals, CHI degradation, and bounds
- Two-stage ensemble loading, normal inference, fault inference, and schema
- Streaming engine packets, window metrics, fault injection, and clients
- Kafka producer/consumer contracts and fallback behavior
- FastAPI health, status, lifecycle, logs, telemetry, faults, benchmark, and dashboard routes

Additional checks:

```powershell
python -m compileall -q config data models streaming tests server.py run.py
docker compose config --quiet
```

For the latest validation details, see [`docs/VALIDATION_REPORT.md`](docs/VALIDATION_REPORT.md).

## 9. Submission Checklist

The submitted project should include:

- Complete Python source code under `config/`, `data/`, `models/`, `streaming/`, and the project root
- Complete dashboard source under `dashboard/`
- Automated tests under `tests/`
- Trained model artifacts under `models/saved/`
- `requirements.txt`
- `Dockerfile`, `compose.yaml`, `start.bat`, and `stop.bat`
- This `README.md` containing pipeline, setup, execution, tools, and versions
- Supporting technical documentation under `docs/`

Do not submit private credentials, local `.env` files, Docker secrets, or machine-specific caches.

## 10. Documentation

| Document | Description |
|---|---|
| [`PROJECT_MASTER_GUIDE.md`](docs/PROJECT_MASTER_GUIDE.md) | Full event lifecycle, formulas, glossary, and developer recipes |
| [`PROJECT_OVERVIEW.md`](docs/PROJECT_OVERVIEW.md) | Problem statement, academic context, and project scope |
| [`ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Topology, event flow, ports, schemas, and service boundaries |
| [`COMPONENTS_DEEP_DIVE.md`](docs/COMPONENTS_DEEP_DIVE.md) | Module responsibilities and error paths |
| [`SETUP_DOCKER_AND_RUNBOOK.md`](docs/SETUP_DOCKER_AND_RUNBOOK.md) | Docker setup, volumes, environment, and operations runbook |
| [`DASHBOARD_AND_OPERATIONS.md`](docs/DASHBOARD_AND_OPERATIONS.md) | Dashboard controls and fault-injection workflows |
| [`TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md) | Known runtime issues and resolutions |
| [`MODELS_AND_METRICS.md`](docs/MODELS_AND_METRICS.md) | Feature space, residuals, CHI, and benchmark methodology |
| [`VALIDATION_REPORT.md`](docs/VALIDATION_REPORT.md) | Test execution and validation evidence |

## 11. Academic Reference

A. Ranpariya and S. Sharma, “A digital twin solution for fault detection in time-critical IIoT applications,” *Journal of Simulation*, vol. 19, no. 4, pp. 441–454, 2025. DOI: [10.1080/17477778.2025.2453725](https://doi.org/10.1080/17477778.2025.2453725).
