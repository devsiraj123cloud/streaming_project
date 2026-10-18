# Setup, Docker & Runbook — Real-Time Digital Twin

This runbook provides complete operational procedures for running the digital twin console locally and inside Docker containers.

---

## 1. Quick Start: Windows Double-Click (No-CLI)

### Prerequisites
- Windows 10/11 64-bit
- Docker Desktop installed and running ([Download Docker Desktop](https://www.docker.com/products/docker-desktop/))

### Starting the Application
1. Open Docker Desktop and verify the whale icon in the taskbar is steady (not animating).
2. Double-click `start.bat` in the project root directory.
3. The script will:
   - Verify Docker daemon connectivity.
   - Build the container image using `Dockerfile` (pins Python 3.11-slim and trains model weights if needed).
   - Launch `digital-twin-app`, `digital-twin-kafka`, and `digital-twin-spark` via Docker Compose.
   - Poll `http://localhost:8000/api/health` until ready.
   - Automatically open `http://localhost:8000` in your default browser.

### Stopping the Application
1. Double-click `stop.bat` in the project root directory.
2. The script runs `docker compose down` and cleanly terminates the containers without deleting benchmark data.

---

## 2. Host Python Execution (Direct Mode)

If Docker is not installed or you prefer running directly on your host Python environment:

### Prerequisites
- Python 3.10+ (tested on Python 3.11 and Python 3.13)
- Install requirements:
  ```powershell
  pip install -r requirements.txt
  ```

### Launch
```powershell
python run.py
```
This runs the master launcher, checks pre-trained weights, starts Uvicorn on `127.0.0.1:8000`, and opens the browser.

---

## 3. Configuration & Environment Variables

Copy `.env.example` to `.env` to customize settings:

```ini
# Server Network Configuration
HOST=127.0.0.1
PORT=8000

# Streaming & Heartbeat
WS_HEARTBEAT_INTERVAL=1.0

# Kafka bootstrap server (Docker Compose service DNS)
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
```

In Docker, `HOST` is automatically set to `0.0.0.0` inside `compose.yaml`, while the port mapping exposes `127.0.0.1:8000:8000` on the host to prevent unauthorized LAN access.

---

## 4. Persistent Storage & Data Reset

- **Docker Named Volume:** `digital-twin-data` (mounted at `/app/data`).
  - Persists generated benchmark data and results if the training pipeline creates them.
- **Safe Data Reset:**
  If you wish to wipe persisted benchmark data and retrain models from scratch:
  ```powershell
  docker compose down -v
  ```
  This deletes the named volume. On next start, the models will be freshly trained.

---

## 5. Kafka and Spark (Default Docker Stack)

Start the full distributed stack:

```powershell
docker compose up -d
```

This starts:
- `digital-twin-app` on port 8000
- `digital-twin-kafka` (Confluent cp-kafka 7.6.1 in KRaft mode) on port 9092. The app publishes telemetry to `iiot-sensor-stream`.
- `digital-twin-spark` (Spark Structured Streaming 3.5.3) consumes that topic and outputs 5-second sliding window aggregates. Inspect with `docker compose logs -f spark`.

---

## 6. CLI Troubleshooting & Health Checks

| Check | Command | Expected Output |
|---|---|---|
| Container Status | `docker compose ps` | `app`, `kafka`, and `spark` running |
| Application Health | `curl http://localhost:8000/api/health` | `{"status": "healthy", ...}` |
| Service Registry | `curl http://localhost:8000/api/services` | Service states list |
| Live Container Logs | `docker compose logs -f app spark` | Application events and Spark window aggregates |
