# Components Deep Dive — Real-Time Digital Twin

This document provides a module-by-module breakdown of each source component, detailing its responsibilities, interfaces, dependencies, error handling, and lifecycle.

---

## 1. Configuration & Orchestration

### `config/settings.py`
- **Purpose:** Centralized configuration provider with environment variable overrides.
- **Key Variables:**
  - `HOST`: Server interface binding (defaults to `127.0.0.1`, overridable to `0.0.0.0` for Docker).
  - `PORT`: HTTP/WebSocket listening port (defaults to `8000`).
  - `STREAM_BATCH_INTERVAL_SEC`: Real-time emission interval (0.1 s = 10 Hz).
  - `WINDOW_DURATION_SEC` / `SLIDE_DURATION_SEC`: Spark-equivalent window analytics parameters (5 s window / 1 s slide).
  - `ASSET_METADATA`: Physical asset specs for `TWIN-MTR-01` (15 kW, 1480 RPM, 28.5 A rated, 95°C max temp).
  - `FAULT_CLASSES`: Integer to fault name dictionary (0: Normal, 1: Bearing Defect, 2: Stator Overheating, 3: Rotor Imbalance, 4: Cavitation, 5: Sensor Drift).

---

## 2. Telemetry Generation & Physics Simulation

### `data/generator.py`
- **Class:** `IIoTSensorStreamSimulator`
- **Responsibility:** High-frequency synthetic IIoT telemetry generator embedding realistic multi-physics correlations.
- **Physics Equations:**
  - **Motor Slip & RPM:** $\text{RPM} = 1480 - (\text{Load} \times 15) + \mathcal{N}(0, 1.2)$.
  - **Thermal Transfer:** $T(t) = T(t-1) + 0.02 \times (T_{\text{target}} - T(t-1)) + \mathcal{N}(0, 0.08)$, where $T_{\text{target}} = T_{\text{amb}} + 35 \times (I / 28.5)^{1.8}$.
  - **Structural Vibration:** Baseline $1.6 + 0.5 \times \text{Load} + \mathcal{N}(0, 0.08)$ mm/s RMS (ISO 10816 Class II).
- **Fault Signatures:**
  - Mode 1 (Bearing Defect): High vibration peaks ($3.5 \times$ normal), elevated acoustic dB ($+18$ dB).
  - Mode 2 (Stator Overheating): Exponential thermal rise to 95–115°C, current surge to 38 A.
  - Mode 3 (Rotor Imbalance): 1X rotational vibration resonance ($6.2$ mm/s RMS).
  - Mode 4 (Cavitation): Pressure oscillations dropping to $1.8$ bar with high-frequency acoustic hiss ($88$ dB).
  - Mode 5 (Sensor Drift): Cumulative linear bias in temperature and acoustic transducers.
- **Methods:**
  - `generate_sample(timestamp)`: Produces a 7-channel telemetry dictionary.
  - `set_fault_mode(fault_code, intensity)`: Modulates active failure parameters.
  - `reset_to_normal()`: Clears active fault states and restores baseline dynamics.

---

## 3. Mathematical Digital Twin & Residual Modeling

### `models/digital_twin.py`
- **Class:** `IndustrialDigitalTwin`
- **Responsibility:** Virtual twin tracking expected nominal dynamics and computing residual Z-scores and asset health.
- **State Representation:** Virtual steady-state temperature, expected current, expected RPM, expected pressure.
- **Residual Z-Score Formula:**
  $$z_i(t) = \max\left(0, \frac{|X_{i}(t) - \hat{X}_{i}(t)| - \mu_i}{\sigma_i}\right)$$
- **Composite Health Index (CHI):**
  $$\text{CHI}_{\text{raw}} = 100 \times \exp\left(-0.25 \sum_{i=1}^{7} w_i \cdot z_i(t)\right)$$
  Smoothed with exponential filter: $\text{CHI}(t) = 0.85 \times \text{CHI}(t-1) + 0.15 \times \text{CHI}_{\text{raw}}$, clamped to $[5.0, 100.0]\%$.
- **Health Tiers:**
  - Optimal / Healthy ($\ge 85.0\%$, emerald `#10b981`)
  - Degradation Warning ($60.0\% - 84.9\%$, amber `#f59e0b`)
  - Critical Fault Imminent ($< 60.0\%$, red `#ef4444`)

---

## 4. Machine Learning Inference

### `models/two_stage_ensemble.py`
- **Class:** `TwoStageFaultDetectionEnsemble`
- **Responsibility:** Replicates Ranpariya & Sharma (2025) two-stage edge inference architecture.
- **Feature Vector:** 14 dimensions (7 raw sensor channels + 7 digital twin residual channels).
- **Stage 1 (Fast Binary Screener):**
  - Algorithm: `SGDClassifier(loss='modified_huber', penalty='l2')`.
  - Latency: $< 1.5$ ms.
  - Decision: If $P(\text{anomaly}) < 0.45$, returns Normal immediately without invoking Stage 2.
- **Stage 2 (Diagnostic Fault Classifier):**
  - Algorithm: `ExtraTreesClassifier(n_estimators=60, max_depth=16)`.
  - Latency: $\sim 3.2$ ms.
  - Total Estimation Delay: Fast path $\sim 1.5$ ms, diagnostic path $\sim 4.8$ ms.
- **Methods:**
  - `predict_stream_packet(sensor_reading, residuals)`: Executes inference and returns latency metrics and fault diagnosis.
  - `train(X_train, y_train)`: Fits StandardScaler, Stage 1 SGD, and Stage 2 ExtraTrees.
  - `save_models()` / `load_models()`: Joblib serialization to `models/saved/`.

### `models/train_models.py`
- **Responsibility:** Generates 18,000 synthetic benchmark samples when needed, trains Proposed Two-Stage Ensemble vs Baseline A (Random Forest, 7-D raw) and Baseline B (SVM RBF, 7-D raw), and exports comparative metrics to the generated `data/model_benchmark_results.json`.

---

## 5. Streaming Engine & Server

### `streaming/stream_engine.py`
- **Class:** `RealTimeStreamingEngine`
- **Responsibility:** Core in-process asynchronous streaming coordinator.
- **Attributes:**
  - `recent_buffer`: Bounded deque (`maxlen=100`) of recent unified packets.
  - `recent_alerts`: Bounded deque (`maxlen=20`) of deduped anomaly events.
  - `subscribers`: Active WebSocket connections set.
  - `packet_counter`: Monotonically increasing packet counter.
- **Methods:**
  - `step()`: Executes one sensor generation, digital twin update, ML prediction, and window aggregation cycle.
  - `run_loop()`: Asynchronous `while self.is_running:` loop running at 10 Hz with WebSocket broadcast.
  - `inject_fault(code, intensity)` / `reset_fault()`: Fault control interface.
  - `register_client(ws)` / `unregister_client(ws)`: WebSocket client management.

### `server.py`
- **Framework:** FastAPI with Uvicorn ASGI server.
- **Key Endpoints:**
  - `GET /`: Serves industrial operations console HTML.
  - `WEBSOCKET /ws/stream`: 10 Hz bi-directional telemetry broadcast.
  - `GET /api/health`: Application uptime, engine status, and client count.
  - `GET /api/status`: Current snapshot fallback for HTTP polling.
  - `GET /api/services`: Service registry with real observed state (`stream-engine`, `kafka-broker`, `spark-job`).
  - `POST /api/services/{id}/start|stop|restart`: Lifecycle controls with concurrency protection.
  - `GET /api/services/{id}/logs?tail=N`: In-memory log buffer retrieval.
  - `GET /api/telemetry/latest`: Latest raw and processed packet.
  - `GET /api/telemetry/alerts`: Recent alerts deque.
  - `POST /api/fault/inject`: Fault injection endpoint.
  - `POST /api/fault/reset`: Normal operation restoration endpoint.
  - `GET /api/benchmark`: Serves offline benchmark metrics tagged with provenance metadata.

---

## 6. Frontend Operations Console

- **`dashboard/templates/index.html`:** Semantic HTML5 5-row layout with WCAG AA compliance, high-contrast badges, and descriptive IDs.
- **`dashboard/static/css/dashboard.css`:** Cyberpunk industrial design system with CSS custom property tokens, dark mode palette, responsive grid, and zero external CSS dependencies.
- **`dashboard/static/js/app.js`:** Telemetry controller managing WebSocket lifecycle (exponential backoff reconnect), REST service controls, fault button dispatches, and benchmark table rendering.
- **`dashboard/static/js/charts.js`:** Native HTML5 Canvas charts rendering Vibration, Temperature, Current, and Health Index at 60 fps with auto-scaling axes, safety thresholds, and legends.
- **`dashboard/static/js/twin_visualizer.js`:** Canvas-based 2D digital twin visualizer rendering induction motor, drive shaft, and centrifugal pump with real-time rotational speed, thermal color glow, and fault warning pulse rings.
