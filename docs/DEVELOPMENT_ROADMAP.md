# Development Roadmap — Real-Time Digital Twin

This roadmap tracks implemented milestones, current architectural boundaries, and prioritized future enhancements.

---

## 1. Implemented Milestones (Phase 1 & 2 Complete)

- [x] **Physics-Informed Digital Twin:** Nominal state physics, 7-channel residual computation, and clamped Composite Health Index.
- [x] **Ranpariya & Sharma (2025) Two-Stage ML Ensemble:** Fast-path Stage 1 SGD screener + fine-grained Stage 2 ExtraTrees classifier.
- [x] **Asynchronous In-Process Streaming Engine:** 10 Hz telemetry loop with rolling window aggregates.
- [x] **Industrial Operations Console:** High-tech 5-row responsive dashboard, native Canvas charts, dynamic 2D visualizer, and fault controls.
- [x] **Service Lifecycle API:** REST endpoints for start/stop/restart, bounded log buffer, and clean cancellation.
- [x] **Dockerized Packaging & Windows Launchers:** Self-contained Dockerfile, Compose stack with loopback binding, and `start.bat` / `stop.bat`.
- [x] **Automated Test Suite:** Unit and integration tests covering Digital Twin, ML Ensemble, Streaming Engine, and API contracts (27/27 passing).
- [x] **Technical Documentation Set:** Comprehensive 12-file operational and academic documentation.

---

## 2. Prioritized Technical Backlog

| Priority | Feature / Improvement | Target Component | Acceptance Criteria |
|---|---|---|---|
| **P1** | **Physical Hardware Ingestion Connector** | Ingestion Engine | MQTT / Modbus TCP adapter allowing real PLC / accelerometer ingestion. |
| **P2** | **Dynamic Sliding Window Adjustment** | Streaming Engine | Runtime API endpoint allowing users to modify window duration (1–30s) from UI. |
| **P3** | **Automated Multi-Run Benchmark Suite** | Evaluation | CLI flag to run N-fold cross validation with noise jitter injection. |
| **P4** | **Remaining Useful Life (RUL) Prognostics** | Digital Twin Model | Exponential degradation extrapolation predicting hours until failure. |
| **P5** | **Multi-Asset Cluster Topology** | Backend / Dashboard | Support monitoring multiple induction motors (`TWIN-MTR-01` .. `04`) simultaneously. |

---

## 3. Academic & Research Extensions

1. **Experimental Benchmarking with Real Testbed Data:**
   - Integrate CWRU (Case Western Reserve University) Bearing Data or IMS (Intelligent Maintenance Systems) run-to-failure vibration datasets.
   - Benchmark domain adaptation transferability of the Two-Stage Ensemble on real mechanical degradation signatures.
2. **Physics-Informed Neural Networks (PINNs):**
   - Explore replacing empirical heat transfer ODEs with a PINN surrogate model running via ONNX Runtime at the edge.
