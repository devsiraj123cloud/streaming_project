"""
Configuration Settings for IOE413 Real-Time Digital Twin Streaming Project.
Team No - 10 | Amit Kumar (2023BCS0210), Kartik P (2023BCD0051), Rohit Pratap Singh (2023BCS0138), Mohammed Sirajudheen (2023BCS0132)
Baseline Paper: Ranpariya & Sharma (2025), Journal of Simulation
"""

import os
from pathlib import Path

# Base Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
MODELS_DIR = BASE_DIR / "models" / "saved"
STATIC_DIR = BASE_DIR / "dashboard" / "static"
TEMPLATES_DIR = BASE_DIR / "dashboard" / "templates"

for directory in [DATA_DIR, MODELS_DIR, STATIC_DIR, TEMPLATES_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
KAFKA_TOPIC_TELEMETRY = "iiot-sensor-stream"
KAFKA_TOPIC_ALERTS = "iiot-anomaly-alerts"
KAFKA_TOPIC_TWIN_STATE = "iiot-digital-twin-state"

# Streaming & Analytics Configuration
STREAM_BATCH_INTERVAL_SEC = 0.1  # 100ms emission interval (10 Hz stream)
WINDOW_DURATION_SEC = 5          # 5-second sliding window
SLIDE_DURATION_SEC = 1           # 1-second slide interval
MAX_STREAM_BUFFER_SIZE = 1000

# Server & Dashboard Configuration
HOST = os.getenv("HOST", "127.0.0.1")
PORT = int(os.getenv("PORT", "8000"))
WS_HEARTBEAT_INTERVAL = float(os.getenv("WS_HEARTBEAT_INTERVAL", "1.0"))

# Digital Twin Parameters (Induction Motor / Pump Asset ID: TWIN-MTR-01)
ASSET_METADATA = {
    "asset_id": "TWIN-MTR-01",
    "name": "Industrial Induction Motor & Centrifugal Pump Unit",
    "rated_power_kw": 15.0,
    "rated_rpm": 1480,
    "rated_current_a": 28.5,
    "normal_temp_c": 62.0,
    "max_allowed_temp_c": 95.0,
    "max_allowed_vibration_rms": 4.5,
    "baseline_accuracy_target": 0.9971,  # 99.71% from Ranpariya & Sharma 2025
    "target_latency_ms": 4.8            # 4.8 ms average estimation delay
}

# Fault Categories
FAULT_CLASSES = {
    0: "Normal Operation",
    1: "Bearing Inner/Outer Race Fault",
    2: "Stator Winding Overheating",
    3: "Dynamic Rotor Imbalance",
    4: "Hydraulic Cavitation / Loss of Pressure",
    5: "Sensor Drift / Calibration Anomaly"
}
