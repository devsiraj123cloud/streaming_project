# System Architecture — Real-Time Digital Twin

This document details the architectural topology, dataflow, network bindings, and component boundaries of the IOE413 Real-Time Digital Twin Streaming Project.

**Academic Team:** Team 12  
- **Mohammed Sirajudheen** (2023BCS0132) — Lead System Architecture  
- **Rohit Pratap Singh** (2023BCS0138) — Streaming Engine & Kafka Ingestion  
- **Amit Kumar** (2023BCS0210) — Digital Twin Physics Model & Residuals  
- **Kartik P** (2023BCD0051) — ML Ensemble & Benchmarks  

**Baseline Research Paper:**  
A. Ranpariya and S. Sharma, *"A digital twin solution for fault detection in time-critical IIoT applications,"* *Journal of Simulation*, vol. 19, no. 4, pp. 441–454, 2025. [DOI: 10.1080/17477778.2025.2453725](https://doi.org/10.1080/17477778.2025.2453725)

---

## 1. System Architecture Diagram

```mermaid
flowchart TD
    subgraph EdgeSimulation ["Edge Simulation Layer"]
        SIM["IIoTSensorStreamSimulator\n(10 Hz / 100 ms Interval)\n6 Fault Modes (0-5)"]
        FI["Fault Injection Controller\n(REST POST /api/fault/inject)"]
        FI -->|Set Mode / Intensity| SIM
    end

    subgraph KafkaIngestion ["Apache Kafka Event Broker (KRaft Mode)"]
        PROD["IIoTKafkaProducer\n(JSON Serializer, Key: asset_id)"]
        TOPIC["Kafka Topic: iiot-sensor-stream\n(Partition 0, Strict FIFO Ordering)"]
        CONS["IIoTKafkaConsumer\n(Group: iiot-digital-twin-cg)"]
        
        SIM -->|Raw Telemetry 7-D| PROD
        PROD -->|Publish 10 Hz| TOPIC
        TOPIC -->|Poll Records| CONS
    end

    subgraph CoreEngine ["Stream Engine & Analytics Core"]
        CONS -->|Ingested Telemetry| STEP["Engine Step Loop (with local fallback)"]
        
        subgraph DigitalTwin ["Digital Twin Physics Model"]
            DT["IndustrialDigitalTwin (TWIN-MTR-01)\nVirtual Physics Calculations\nResiduals: R(t) = |X_actual - X_exp|\nComposite Health Index (CHI)"]
        end
        
        STEP -->|Process Telemetry| DT
        DT -->|7-D Residuals + CHI| STEP
        
        subgraph MLEnsemble ["Two-Stage Machine Learning Ensemble"]
            S1["Stage 1: SGD Fast Binary Filter\n(Sub-millisecond screening < 1.5 ms)"]
            S2["Stage 2: ExtraTrees Diagnostic Classifier\n(Multiclass fault diagnosis ~ 3.1 ms)"]
            S1 -->|Anomaly Flagged (P >= 0.45)| S2
        end
        
        STEP -->|14-D Concatenated Vector| MLEnsemble
        MLEnsemble -->|Diagnostic Prediction & Latency| STEP
        
        WIN["Sliding Window Aggregator\n(5-second window, 1-second slide)"]
        STEP --> WIN
    end

    subgraph APIServer ["FastAPI Server (Port 8000)"]
        SRV["FastAPI Lifespan Manager"]
        REST["REST Endpoints\n/api/health, /api/services, /api/kafka/status\n/api/telemetry/*, /api/fault/*, /api/benchmark"]
        WS["WebSocket Telemetry Hub\n/ws/stream (100-packet ring buffer)"]
        SRV --- REST
        SRV --- WS
    end

    STEP -->|Unified Telemetry Packet| WS

    subgraph ClientUI ["Browser Operations Console (60 FPS)"]
        DASH["HTML5 / Canvas Dashboard"]
        KPI["Row 1: KPI Metrics Strip (CHI, Delay, Fault)"]
        TWIN["Row 2: 2D Dynamic Motor/Pump Canvas + 4 Charts"]
        DIAG["Row 3: Inference Breakdown + Fault Injection Deck"]
        CTRL["Row 4: Pipeline Flow + Kafka Status & Controls"]
        FEED["Row 5: Audit Feed + Benchmark Matrix"]
        
        DASH --- KPI
        DASH --- TWIN
        DASH --- DIAG
        DASH --- CTRL
        DASH --- FEED
    end

    WS -->|JSON Broadcast 10 Hz| DASH
    DASH -->|REST Actions| REST

    subgraph DistributedBatchReference ["Spark Structured Streaming Reference"]
        SPARK["PySpark Structured Streaming Job\n(streaming/spark_streaming_job.py)\nWindowed Kafka Analytics Reference"]
        TOPIC -.->|ReadStream| SPARK
    end
```

---

## 2. Event Lifecycle & Dataflow

Every 100 ms (10 Hz), the decoupled streaming pipeline executes the following 8-phase sequence:

1. **Telemetry Generation (Edge Simulator):** `IIoTSensorStreamSimulator.generate_sample()` computes 7 physical sensor channels (vibration RMS, vibration peak, temperature, RPM, phase current, hydraulic pressure, acoustic emissions) governed by first-principles induction motor slip equations and active fault modulation.
2. **Kafka Producer Serialization:** `IIoTKafkaProducer` serializes the reading into JSON keyed by `asset_id` (`TWIN-MTR-01`) and publishes the event to Apache Kafka topic `iiot-sensor-stream`.
3. **Broker Transit & Ordering:** Apache Kafka (KRaft consensus mode) commits the record to partition 0 with monotonic offset numbering, guaranteeing FIFO temporal ordering.
4. **Kafka Consumer Polling:** `IIoTKafkaConsumer` (belonging to consumer group `iiot-digital-twin-cg`) polls the message from the topic with zero lag. *(If the broker is unreachable, the engine automatically falls back to in-process memory transit without packet loss).*
5. **Digital Twin Synchronization:** `IndustrialDigitalTwin.process_telemetry()` computes expected physical state from coupled electrical and thermal models:
   - Slip factor $s(t)$ and mechanical load $\lambda(t) \in [0.4, 1.0]$.
   - First-order thermal dissipation $\hat{T}_{\text{ss}}(t) = T_{\text{ambient}} + 35.0 \cdot (I/I_{\text{rated}})^{1.8}$ with $\tau = 35\text{ s}$.
   - ISO 10816 Class II vibration baseline velocity $\hat{V}_{\text{rms}}(t) = 1.5 + 0.6 \lambda(t)$.
6. **Residual Extraction & Composite Health Index (CHI):**
   - 7-D physical residual vector: $R_i(t) = |X_i(t) - \hat{X}_i(t)|$.
   - Z-score normalization against calibrated healthy baseline: $z_i(t) = \max\left(0, \frac{R_i(t) - \mu_i}{\sigma_i}\right)$.
   - Weighted exponential decay: $\text{CHI}_{\text{raw}}(t) = 100 \times \exp(-0.25 \sum w_i z_i)$.
   - EWMA smoothing: $\text{CHI}(t) = 0.85 \cdot \text{CHI}(t-1) + 0.15 \cdot \text{CHI}_{\text{raw}}(t)$, clamped to $[5.0, 100.0]\%$.
7. **Two-Stage ML Inference (Ranpariya & Sharma 2025):**
   - Concatenates 7 raw sensors + 7 physical residuals into a standardized 14-D feature vector $\mathbf{X}_{14D}$.
   - **Stage 1 (SGD Binary Screener):** Evaluated with modified Huber loss. If $P(\text{anomaly}) < 0.45$, Stage 2 is bypassed (**Fast Path**, $< 1.5$ ms delay).
   - **Stage 2 (ExtraTrees Classifier):** Invoked conditionally when Stage 1 flags an anomaly ($P \ge 0.45$). Evaluates 60 decision trees across fault classes 1–5 (**Diagnostic Path**, $\sim 4.8$ ms delay).
8. **Window Aggregation & WebSocket Broadcast:** Updates 5-second sliding window aggregates (1-second slide), appends to recent buffer (`maxlen=100`), logs timestamped anomalies, and pushes JSON to all browser clients via `/ws/stream`.

---

## 3. Network Topology & Ports

| Component | Container Name | Host Binding | Port | Protocol | Purpose |
|---|---|---|---|---|---|
| Digital Twin Application | `digital-twin-app` | `127.0.0.1` (`0.0.0.0` in Docker) | `8000` | HTTP / WebSocket | Main web console, REST endpoints, telemetry WebSocket |
| Apache Kafka Broker | `digital-twin-kafka` | `127.0.0.1` (Host) / `kafka` (Docker net) | `9092` / `29092` | PLAINTEXT | High-throughput telemetry event ingestion broker |
| Kafka KRaft Controller | `digital-twin-kafka` | Internal Docker network | `9093` | PLAINTEXT | KRaft consensus metadata quorum (Zookeeper-less) |

---

## 4. Telemetry Event Schema

Every packet emitted on `/ws/stream` conforms to the unified 14-D streaming schema:

```json
{
  "packet_id": 1420,
  "timestamp": 1776203100.125,
  "kafka_enabled": true,
  "kafka_metadata": {
    "topic": "iiot-sensor-stream",
    "partition": 0,
    "offset": 1420,
    "timestamp": 1776203100125
  },
  "raw_telemetry": {
    "timestamp": 1776203100.125,
    "asset_id": "TWIN-MTR-01",
    "vibration_rms": 1.842,
    "vibration_peak": 2.763,
    "temperature": 58.4,
    "rpm": 1478.2,
    "current_amp": 24.1,
    "pressure_bar": 4.195,
    "acoustic_db": 67.8,
    "fault_label": 0,
    "fault_name": "Normal Operation"
  },
  "digital_twin": {
    "asset_id": "TWIN-MTR-01",
    "timestamp": 1776203100.125,
    "expected_state": {
      "vibration_rms": 1.85,
      "vibration_peak": 2.867,
      "temperature": 58.1,
      "rpm": 1478.5,
      "current_amp": 24.05,
      "pressure_bar": 4.2,
      "acoustic_db": 67.5
    },
    "residuals": {
      "vibration_rms": 0.008,
      "vibration_peak": 0.104,
      "temperature": 0.3,
      "rpm": 0.3,
      "current_amp": 0.05,
      "pressure_bar": 0.005,
      "acoustic_db": 0.3
    },
    "z_scores": {
      "vibration_rms": 0.0,
      "vibration_peak": 0.04,
      "temperature": 0.0,
      "rpm": 0.0,
      "current_amp": 0.0,
      "pressure_bar": 0.0,
      "acoustic_db": 0.0
    },
    "composite_health_index": 98.4,
    "health_status": "Optimal / Healthy",
    "health_color": "#10b981",
    "sync_latency_ms": 1.2
  },
  "prediction": {
    "is_anomaly": false,
    "fault_code": 0,
    "fault_name": "Normal Operation",
    "confidence_percent": 99.8,
    "stage1": {
      "flagged_anomaly": false,
      "anomaly_probability": 0.002,
      "delay_ms": 0.85
    },
    "stage2": {
      "invoked": false,
      "delay_ms": 0.0,
      "probabilities": { "Normal Operation": 1.0 }
    },
    "estimation_delay_ms": 1.55,
    "severity_level": "NORMAL"
  },
  "window_aggregates": {
    "window_size_samples": 50,
    "window_duration_sec": 5.0,
    "rolling_avg_vibration": 1.86,
    "rolling_max_vibration": 2.12,
    "rolling_avg_temp": 58.2,
    "rolling_avg_current": 24.1,
    "rolling_avg_delay_ms": 1.58
  }
}
```

