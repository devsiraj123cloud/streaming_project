# Master Implementation Prompt — Real-Time Digital Twin Anomaly Detection

> **How to use:** Open the actual project repository in your coding agent (Cursor, Claude Code, Codex, Copilot Agent, or another repo-aware agent), then paste this entire prompt. The agent must edit the repository, run checks, and document what it actually changed. Do not run this against a blank folder: it needs the existing source code, models, dashboard, and configuration.

## Your role and mission

Act as a senior full-stack engineer, data-streaming engineer, ML engineer, DevOps engineer, UI/UX designer, and technical writer. Upgrade the existing **Real-Time Digital Twin for Streaming-Based Anomaly Detection** mini-project into a coherent, runnable, demonstrable application.

Perform the work in one end-to-end pass. Do not stop after giving a plan or code snippets. Inspect the repository, make changes, test the result, and generate the documentation listed below. If something cannot be implemented or verified in the current environment, state exactly why and leave a reproducible verification step rather than claiming success.

### Project context supplied by the owner

The supplied project outline describes the following intended technology stack and behaviour:

- FastAPI HTTP/WebSocket server and a browser dashboard.
- An industrial induction motor / centrifugal pump digital-twin simulation (`TWIN-MTR-01`).
- Synthetic multi-channel IIoT telemetry, reportedly generated at 10 Hz.
- Apache Kafka broker and topic `iiot-sensor-stream`.
- PySpark Structured Streaming with a 5-second window and 1-second slide, if supported by the actual implementation.
- A digital twin that compares measured/simulated sensor values with virtual expected values and computes residuals and a Composite Health Index (CHI).
- A two-stage model: a quick binary screening stage and a fault-classification stage.
- Fault scenarios described in the project notes: normal operation, bearing defect, stator overheating, rotor imbalance, hydraulic cavitation/pressure loss, and sensor drift.
- Existing modules are described as `config/`, `data/`, `models/`, `streaming/`, `dashboard/`, `evaluation/`, `server.py`, and `run.py`. Verify this structure; do not assume every listed file exists or behaves as described.

The project notes report 99.71% accuracy and 4.78 ms average latency. Treat these as **reported claims, not verified live measurements** until the actual evaluation code and dataset reproduce them. Distinguish published-paper metrics, offline benchmark results, and current live telemetry throughout the UI and documentation.

## Non-negotiable working rules

1. **Audit before editing.** Inspect the actual file tree, Git status, dependency files, Docker setup, APIs, frontend, model artefacts, training code, Spark code, Kafka settings, and existing tests. Record the real architecture and runtime versions before making decisions.
2. **Protect the working tree.** Do not delete user data, CSV datasets, model files, results, or uncommitted work. Do not use destructive Git commands. Do not overwrite unrelated changes. Preserve the existing stack where reasonable instead of rewriting everything for novelty.
3. **Do the implementation.** Make the code, UI, Docker, configuration, tests, and docs changes in the repo. Do not return only advice, pseudo-code, or a checklist.
4. **Keep the system honest.** Never manufacture telemetry, model scores, latency, health, connection status, or benchmark results and display them as real. Simulated data must be clearly labelled as simulation. Show unavailable data as unavailable, not as a plausible-looking number.
5. **Follow the source of truth.** Existing code and measured test results take precedence over old README claims. Where source files contradict the supplied project outline, investigate and document the discrepancy. Do not silently claim paper replication if it has not been demonstrated.
6. **Avoid broad exception suppression.** Handle normal user stops, WebSocket disconnects, and cancellation cleanly. Log genuine unexpected faults with enough context to debug them.
7. **Keep versions coherent.** Pin and document compatible Python, Java, PySpark, Scala/Kafka connector, pandas, scikit-learn, and model-artifact versions. Do not fix one warning by indiscriminately upgrading every package.
8. **Make it maintainable.** Prefer small, explicit modules, type-checked request/response schemas, configuration via environment variables, useful logging, lifecycle ownership, health/readiness checks, and repeatable tests.
9. **Security matters even for a student demo.** Do not expose Docker Engine control to the browser. Do not mount the Docker socket into the public-facing dashboard/API container. Do not add shell-command endpoints that accept arbitrary user input. Keep any privileged local control-plane isolated and restricted to an allowlist of known services/actions.
10. **No false completion claims.** At the end, list exact files changed, commands/checks actually run, outcomes, and items that could not be tested. If Docker, network downloads, or a broker are unavailable in the environment, say so.

# Workstream A — Audit and establish a baseline

1. Inspect the complete repository and identify the actual entry points, module boundaries, frontend rendering approach, WebSocket protocol, Kafka topic/schema, Spark version, model loading/training code, settings, and existing tests.
2. Record the current Git status before editing. Do not reset or clean the user's work.
3. Inspect dependencies and versions. Identify the Python version used to train model artefacts if metadata or logs establish it. Determine the installed PySpark and its matching Kafka source connector coordinates (Spark version plus Scala binary version).
4. Check whether the architecture has two independent processing paths (for example, `kafka_consumer.py` running twin/ML inference while `spark_streaming_job.py` independently reads the same topic). Decide how they are meant to work together. Avoid duplicate alerts, duplicated inference, consumer-group conflicts, or a dashboard that mixes values from different pipelines without provenance. Preserve alternate paths only if they have a clearly documented purpose.
5. Create a short audit note in `docs/IMPLEMENTATION_AUDIT.md`, including confirmed facts, contradictions, missing files, current blockers, and a prioritized remediation plan. Then continue implementation; the audit is not the final deliverable.

# Workstream B — Redesign the dashboard UI

The current screenshot is a compact, dense, cyberpunk-style grid positioned toward the right of a wide viewport, with too much empty space, very small labels, and many similarly weighted panels. Replace it with a polished **industrial operations/observability console**, not merely a recoloured version of the existing layout.

## Design direction

- Create a clear hierarchy: asset identity and runtime status; primary operational KPIs; live trends; digital-twin state; detected anomalies; controls; pipeline status; and audit/history.
- Use the full available viewport intelligently. Remove the giant unused left area, prevent microscopic typography, and make the layout work from laptop to large monitor and smaller screens.
- Use a restrained palette: near-black/graphite surfaces, readable neutral text, and carefully reserved semantic accents (green = healthy, amber = degraded/warning, red = active fault, blue/cyan = data/selection). Do not use neon on every border or glow on every card.
- Use a consistent spacing scale, readable typography, aligned cards, subtle borders, useful empty states, clear loading states, and responsive chart grids. Keep contrast and keyboard accessibility high.
- Build a real application shell with a compact header, asset selector/identity, clear global state, and sensible navigation or section anchors. Avoid decorative UI that has no functional meaning.
- Give the main asset/digital twin a meaningful visual area. It may be a refined canvas/SVG illustration or the existing renderer if it is functional. The asset should react to valid state data, not random animation that implies a fault.
- Charts must have labels, units, readable axes/tooltips, a visible time range, and sensible sample retention. Display gaps/stale status when streaming stops. Avoid misleading animation or chart values that do not originate from the real pipeline.
- Provide useful live alert history with timestamp, fault class, severity, source, and measured/estimated delay when genuinely available. Include an explicit empty state when no alert has arrived.
- Keep research comparison data visually and semantically separate from live metrics. Label offline benchmarks with the dataset/run/provenance when known.
- Add clear user feedback for start/stop, fault injection, reset-to-normal, errors, and reconnection. Disable actions while requests are pending; prevent double submissions; surface backend errors in accessible messages.
- No fake activity. If the broker or pipeline is down, show that it is down and offer a relevant start/retry action.
- Prefer the existing frontend technology and static asset pattern unless there is a concrete maintenance or functionality reason to change it. Do not add an entire frontend framework solely for styling.

## Required dashboard sections

1. **Global status strip:** dashboard/API, Kafka, producer, inference/twin processor, Spark job, and WebSocket/stream state. Distinguish service health from data freshness.
2. **Primary KPIs:** CHI, current asset condition, active anomaly count, stream rate, end-to-end or inference latency, processed samples. Use measured values and labels; unavailable values must show `N/A`/`Not measured`.
3. **Digital twin asset panel:** asset identity and relevant operating values (RPM, temperature, vibration, current, pressure, acoustic data only if these fields exist). Visually distinguish simulated/virtual estimates from observed stream values.
4. **Telemetry chart grid:** sensor trends, expected-vs-observed/residual trends, and health trend if the backend emits the corresponding fields.
5. **Fault injection deck:** only the scenarios supported by the backend; show active scenario and severity; include reset/normal action. A severity slider must change the injected signal if and only if that is implemented and tested.
6. **Pipeline view:** clear event/data flow and per-stage states/timings. Show waiting/healthy/degraded/stopped/failed states without implying a stage works when it is unavailable.
7. **Anomaly/audit feed:** real events with stable IDs and time ordering; empty and error states; sensible retention or pagination.
8. **Benchmark/evaluation section:** reported paper metrics versus reproduced benchmark values, with provenance and clear labels.
9. **Operator controls:** start/stop/restart/retry for each supported component, start/stop stream, start/stop fault injection, and reset to normal. Explain any control that cannot be performed because its controlling service is down.

## UI engineering acceptance criteria

- Test the actual dashboard in a browser or, if browser tooling is unavailable, state that clearly and run the nearest available checks.
- Test wide and narrow layouts; no horizontal overflow at normal laptop width.
- No important text that requires zooming to read. Buttons have accessible names, visible focus, disabled/loading state, and semantic HTML.
- All controls are wired to real backend actions and reflect the returned state. Remove or clearly disable dead/demo-only controls.
- Reconnecting the WebSocket must not create duplicate timers, duplicate event handlers, or duplicated chart history.

# Workstream C — Make every subsystem operable from the UI

The owner should not need to open separate terminals to launch the producer, consumer, Spark job, or other project-managed services during normal use.

## Define service lifecycle explicitly

Create a single authoritative service registry that names each actual, independently manageable component. Start with these candidates and keep only those confirmed in the source:

- Kafka broker
- telemetry producer / simulator
- Kafka consumer + Digital Twin/model inference worker
- Spark Structured Streaming job
- FastAPI dashboard/API (the control plane; this normally must remain alive while using the UI)
- optional model training/evaluation jobs (not continuously running services)

For each service expose a state machine such as `stopped`, `starting`, `healthy`, `degraded`, `stopping`, `failed`, `unknown`. Derive the state from real process/container health and logs—not from a button click alone. Add a last transition timestamp, sanitized recent logs, health-check detail, and a human-readable error when a service fails.

## API and safety expectations

Use the existing API conventions when sound. Otherwise, implement typed and documented endpoints equivalent to:

- `GET /api/health` — dashboard/API health and application readiness.
- `GET /api/services` — service registry, actual state, health, and recent status.
- `POST /api/services/{service_id}/start`
- `POST /api/services/{service_id}/stop`
- `POST /api/services/{service_id}/restart`
- `GET /api/services/{service_id}/logs?tail=N` — bounded, sanitized recent logs.
- `GET /api/telemetry/latest` — latest real telemetry/status snapshot.
- `POST /api/faults/{fault_id}/activate` and `/deactivate`, or a typed scenario endpoint matching the current backend.
- `POST /api/faults/reset` — restore normal simulation through the same command path used by the UI.

These are target shapes, not an instruction to break working existing API contracts. Inspect the actual application and adjust paths/payloads consistently. Validate all identifiers, ranges, and request payloads. Use a fixed allowlist of services and actions. Never interpolate arbitrary UI text into shell commands.

## Process/container lifecycle

- The browser must call only the application API; it must never contact Kafka or Docker Engine directly.
- Prefer a dedicated internal controller with a strict allowlist if individual Compose containers have to be started/stopped. Do not mount `/var/run/docker.sock` in the public-facing dashboard/API container. Keep Docker control off the host-facing network and document the local-development trust boundary. Do not expose a general-purpose command execution endpoint.
- Where feasible, run producer/consumer/Spark as separately supervised Compose services with explicit health checks. Make the lifecycle controller request only start/stop/restart for named services that exist in Compose. Deny unknown service IDs and arbitrary Docker API paths.
- If a safe container-level controller cannot be implemented in the current repository, keep the essential dashboard/control plane alive and implement lifecycle control for the processes it safely owns. Explain which infrastructure must stay up and why.
- It is technically impossible for a browser to use an app's UI to restart the same sole server process after that server has been stopped. Therefore, keep the UI/control-plane alive during component stop operations and provide a double-click launcher for initial stack startup. Do not create a “stop absolutely everything” button that destroys its own control plane without explaining the consequence.
- `Stop All Processing` should stop data-generation and processing workers but leave the dashboard/control-plane available. A separate shutdown option, if implemented, must confirm that the dashboard will become unreachable and explain how to restart via the launcher.
- Add timeouts, cancellation, idempotency, and concurrency protection around lifecycle requests. A second Start click must not create another process. On API restart, reconcile actual processes/containers instead of trusting stale in-memory state.
- Keep a bounded per-service log buffer and an explicit way to inspect errors from the UI.
- If a WebSocket disconnects, reconnect with exponential backoff, show `reconnecting`/`stale`, and stop the retry loop after the page/application is disposed. Avoid silent loops that hide permanent failure.

# Workstream D — Dockerize the complete project

The intended routine should be:

1. User opens Docker Desktop (or the launcher offers to open it if possible).
2. User double-clicks `start.bat` on Windows (or uses a documented equivalent on Linux/macOS).
3. The stack builds/starts with Docker Compose, waits for readiness, then opens the dashboard URL in the default browser.
4. The user starts/stops supported pipeline components, injects faults, views logs, and resets the scenario from the UI.
5. User can stop processing from the UI and stop the full stack through a documented double-click launcher if desired.

Create or update, as appropriate:

- `Dockerfile`/Dockerfiles for the actual services.
- `compose.yaml` (or `docker-compose.yml`, not two competing definitions).
- `.dockerignore` and `.env.example`.
- pinned dependency files / lock strategy.
- Windows `start.bat` and `stop.bat`; make them quote paths correctly, report actionable errors, wait for Docker daemon/readiness, and avoid deleting volumes. Add Linux/macOS scripts only if useful.
- a persistent named volume only for data that must survive container restarts (e.g. generated event/result data if the code uses it). Do not put secrets or transient Python environments in volumes.
- health checks, restart policy where appropriate, service dependency/readiness strategy, internal service DNS, and graceful termination.

## Docker requirements

- One documented command or double-click launcher must build and start the stack. Do not require manually running three Python scripts in separate terminals.
- Use Compose service DNS inside containers. `localhost:9092` inside an API/worker container refers to that container, not to Kafka; configure internal clients with the Kafka Compose hostname and port. Configure host access separately only if needed.
- Ensure the topic `iiot-sensor-stream` is created automatically or that the producer handles topic readiness robustly. Use retry/backoff during broker startup; `depends_on` alone is not a readiness guarantee unless paired with health checks and healthy dependencies.
- Add a Spark Kafka connector compatible with the *exact* PySpark/Spark version and Scala binary version chosen in the image. Do not add an arbitrary connector version. Confirm the correct Maven coordinates using the resolved dependency versions and test that `format("kafka")` can load.
- Use a Linux container runtime for Spark where practical to avoid Windows-only `winutils.exe` issues. If using separate worker images, share a base configuration and keep Java/Python versions consistent.
- Keep pandas at a version supported by the selected PySpark release; the current log warns that pandas 3.x may not be supported. Pin a compatible version or deliberately upgrade/test the Spark stack—do not ignore the warning.
- The provided model-artifact warnings state that serialized scikit-learn estimators were written with version 1.5.1 but loaded with 1.9.1. Either pin inference and training to the artifact's compatible version (likely `scikit-learn==1.5.1`, subject to project/runtime compatibility) or retrain/re-export every relevant pipeline artifact using the selected locked version. Never assume persisted `.joblib`/pickle models are cross-version compatible. Store training/dependency metadata beside artifacts and fail clearly on unsupported mismatches.
- Pin a supported Python version based on the real libraries; do not blindly use the host's Python 3.13 if dependencies are not compatible. The supplied logs also show a project virtual environment in a Downloads path while code runs under a separate PyCharm project path. Verify the selected interpreter and remove accidental dependence on host paths.
- Use `.env` for non-secret local configuration and `.env.example` for documented defaults. Do not hard-code machine-specific paths, usernames, tokens, or credentials.
- Keep service ports minimal. Bind the dashboard to loopback for local-only development by default unless public/LAN access is explicitly needed.
- Make builds reproducible; avoid floating `latest` tags for production-like components. Choose mutually compatible pinned image versions after auditing the repository.
- Do not silently delete volumes on stop. Make data reset a separate, clearly named, confirmable operation.
- Include container stdout/stderr logs and app logs that can be viewed from the dashboard where possible.

## Compose architecture decision

Audit the actual code and implement the simplest safe topology that works. A likely topology is:

- `kafka` — broker, internal listener for containers and optionally a host listener for external tools.
- `app` or `dashboard` — FastAPI + static dashboard + restricted lifecycle/status API, always up while operating the UI.
- `producer` — generated telemetry process, disabled/stopped until the operator starts it if practical.
- `consumer`/`twin-worker` — direct consumer/twin/ML path if the architecture actually uses it.
- `spark-streaming` — Spark Structured Streaming worker with matching Kafka connector, if the Spark path is part of the runnable demo.
- optional one-shot `train-models`/`benchmark` jobs that run on demand and exit.

Do not automatically implement all these containers if the existing architecture makes a smaller topology more correct. Clearly state the dataflow and ensure the live UI reports the component that actually generated each result. Avoid two independent inference pipelines racing to publish the same event.

# Workstream E — Fix the supplied runtime errors

Reproduce or reason from source; fix the root cause and test each item. Do not merely suppress logs.

## 1. `kafka.errors.NoBrokersAvailable`

The log attempts `localhost:9092` and resolves IPv6 loopback (`::1`). The most likely cause is that there is no reachable broker at that address/port, or the configured bootstrap address is wrong for the runtime context. Verify broker state, listeners, port mapping, topic readiness, and effective configuration. In Docker, use the broker service DNS name from other containers. Add bounded retries/backoff and expose connection state and a clear UI error. Do not claim Kafka is connected just because client construction succeeded.

## 2. Spark: `Failed to find data source: kafka`

The Spark job cannot load the Spark Structured Streaming Kafka source connector. Identify the installed Spark/PySpark version and Scala binary version; provide the matching `spark-sql-kafka-0-10` package/JAR through the Docker build or Spark submit configuration. Test a minimal read from the Kafka topic. Do not confuse the Python Kafka client dependency with Spark's JVM-side connector.

## 3. scikit-learn unpickle/version warnings

The saved `StandardScaler`, `SGDClassifier`, `ExtraTreeClassifier`, and `ExtraTreesClassifier` artifacts report training with scikit-learn 1.5.1 and inference under 1.9.1. Resolve this with a locked, compatible model environment or retraining/re-export in the chosen environment. Add artifact metadata and a deterministic model-loading test. Do not claim predictions are trustworthy while the mismatch remains unresolved.

## 4. `winutils.exe` / native Hadoop warnings on Windows

The log reports `HADOOP_HOME`/`hadoop.home.dir` unset and failure to load the native Hadoop library. Prefer running Spark in a Linux container with a compatible Java version; confirm which warnings are harmless for this job. Do not tell the user to download random binaries from untrusted sources. If local Windows execution remains supported, document an optional supported setup separately.

## 5. pandas 3.x / PySpark warning

The current job emits a warning that this PySpark release does not fully support pandas >= 3.0. Pin pandas below 3 for the selected compatible stack, or upgrade the entire Spark stack and test it. Record the exact version combination in the dependency lock and docs.

## 6. `KeyboardInterrupt` / `asyncio.exceptions.CancelledError` on manual shutdown

The supplied traceback occurs while stopping Uvicorn and cancelling a WebSocket handler. It may be shutdown noise rather than the root cause of the failed pipeline. Ensure expected cancellation and normal WebSocket disconnect are handled quietly and that background tasks/subprocesses terminate gracefully. Preserve logging for real unexpected failures. Test Ctrl+C and UI-triggered stop separately. The goal is clean shutdown, not blanket exception suppression.

## 7. Possible mismatched Python environment

The logs show paths from both `PycharmProjects\streaming_project` and a virtual environment under `Downloads\lab_2023BCS0210_Amit_kumar`. Audit project interpreter selection, import paths, startup script, `.venv` location, and environment activation. The Docker image must not depend on either machine-specific Windows path.

# Workstream F — Telemetry, models, and scientific correctness

- Document the actual event schema and validate every incoming Kafka payload. Handle malformed records explicitly with counters/logging rather than crashing the worker or silently fabricating defaults.
- Use event timestamps and clear units. Distinguish event time from processing time.
- Make the window length, slide, watermark, topic, rates, thresholds, and model paths configuration-driven. Provide safe defaults in `.env.example` and validate configuration at startup.
- Verify the residual definition, feature order, scaler feature count, class-index mapping, and stage-one/stage-two control flow against the actual model-training code and persisted artifacts.
- The supplied CHI formula is `100 * exp(-sum(w_i * z_i))`, while the notes also claim the result belongs to `[0, 100]`. That range is not guaranteed by the written formula for arbitrary negative z-scores. Inspect the real code and choose a mathematically explicit normalization/clamping policy consistent with the intended interpretation. Document the change and test edge cases.
- Verify whether there are actually 7 raw features plus 7 residual features (14-D) and whether the streaming schema preserves the same order used in training.
- Ensure fault-injection scenarios modify the same source of telemetry consumed by the selected processing pipeline. Resetting to normal must clear persistent fault state and produce observable normal readings again.
- Label all generated/test telemetry as simulated; do not imply a connection to real industrial hardware.
- Re-run benchmark evaluation using the repository's actual dataset and code. Save results with the run timestamp, dependency versions, model identifier, dataset/split provenance, metrics, and latency methodology. Never bake a benchmark number into live KPIs unless it is explicitly labelled as an offline result.
- Do not imply the reported 99.71%/4.78 ms values were independently reproduced unless the tests actually establish that.

# Workstream G — Tests and validation

Add or update automated tests at the right level. Use mocks for unit tests and real container integration for integration tests where possible.

## Required checks

1. Python import/compile checks and formatting/linting consistent with the repository.
2. Unit tests for digital-twin residuals, CHI boundaries, feature order, class mapping, telemetry validation, and fault reset.
3. Unit tests for lifecycle state transitions, idempotent start/stop, unknown service rejection, timeouts, and error reporting.
4. API tests for health, services/status, lifecycle controls, log retrieval bounds, telemetry, fault injection, and reset.
5. WebSocket tests for streaming messages, malformed/disconnected client handling, reconnect-friendly semantics, and graceful cancellation.
6. Docker Compose config validation and build checks. If Docker daemon is available, start the stack and wait for broker/app readiness.
7. Kafka integration test: broker becomes ready, topic exists, producer publishes at least one valid event, consumer or Spark job reads it.
8. Spark integration test: connector loads and the configured streaming query starts; do not treat Spark startup alone as proof of end-to-end processing.
9. End-to-end smoke test: launch; observe non-fabricated telemetry; activate each supported fault; see the resulting readings/classification/alert; reset to normal; stop processing from the UI; restart it; confirm dashboard/control plane remains usable.
10. Shutdown test: stop worker(s) from the UI and stop the stack via launcher; verify no orphan processes or noisy exception tracebacks from expected cancellation.
11. Verify desktop-sized and narrow layouts, browser console/network errors, and accessible control names if browser automation is available.
12. Model loading test ensures the runtime dependency version is compatible with serialized model metadata.

Report each check as `PASS`, `FAIL`, or `NOT RUN`, with evidence and reason. Do not list an unrun test as passing.

# Workstream H — Deliverable documentation (must actually be generated at the end)

Create and maintain the following Markdown documents. The docs must describe the code that exists after implementation—not an idealized system that was never built.

1. `README.md` — short entry point with prerequisites, launch/stop instructions, dashboard URL, controls, and links to deeper docs.
2. `docs/PROJECT_OVERVIEW.md` — problem statement, project goals, scope, assumptions, users, capabilities, limitations, and demo workflow.
3. `docs/ARCHITECTURE.md` — Mermaid architecture diagram, event flow, ports, topic names, schemas, WebSocket/API flow, service ownership, and dataflow explanation.
4. `docs/COMPONENTS_DEEP_DIVE.md` — for each real module/file: responsibility, inputs, outputs, dependencies, key functions/classes, lifecycle, error paths, and how it connects to other components.
5. `docs/SETUP_DOCKER_AND_RUNBOOK.md` — exact Windows double-click procedure, Docker Desktop prerequisites, build/start/stop/rebuild procedure, browser URL, persisted data locations, ports, environment variables, optional CLI troubleshooting, and safe reset procedure.
6. `docs/DASHBOARD_AND_OPERATIONS.md` — every UI section and button, expected effect, API endpoint called, service-state meanings, telemetry/chart provenance, fault-injection workflow, and screenshots only if actually captured.
7. `docs/TROUBLESHOOTING.md` — symptom → verified cause or likely cause → diagnostic evidence → fix → verification. Include all seven supplied issues above and other discovered failures. Mark hypotheses as hypotheses until tested.
8. `docs/MODELS_AND_METRICS.md` — dataset origin, feature schema/order, preprocessing, models and class names, training/evaluation procedure, artifacts, dependency versions, formulas, threshold selection, metrics provenance, latency definition, reproducibility, and limitations. Clearly separate reported, reproduced, and live-observed metrics.
9. `docs/DEVELOPMENT_ROADMAP.md` — implemented items, remaining blockers, short-term fixes, next technical improvements, test gaps, and research improvements, with priority and acceptance criteria.
10. `docs/VALIDATION_REPORT.md` — exact commands and tests actually executed, Docker build/runtime result, evidence, known failures, and checks that could not be run.
11. `docs/IMPLEMENTATION_AUDIT.md` — the pre-change audit required above, updated with final resolution statuses.
12. One comprehensive, navigable `docs/PROJECT_MASTER_GUIDE.md` that ties everything together in enough depth for a new teammate to understand the system, follow a telemetry event end-to-end, find each responsible file, operate the project without CLI for normal use, debug failures, explain the ML/digital-twin logic in a viva, and decide how to extend it.

Documentation quality requirements:

- Include a glossary for IIoT, digital twin, residual, CHI, Kafka topic/partition/consumer group, Structured Streaming, event-time window, watermark, WebSocket, two-stage inference, model artifact, health check, and lifecycle controller.
- Include tables for services/ports/environment variables/event schema/fault classes/important files.
- Explain the full life of one event: generation → validation/serialization → Kafka → processing/windowing → residual/feature construction → Stage 1 → conditional Stage 2 → alert/status event → WebSocket → UI chart/audit log.
- Explain startup and shutdown order, readiness dependencies, what happens if Kafka is unavailable, how retries work, where logs appear, what data persists, and how to restore a clean normal scenario.
- Explain the difference between a service being alive, ready, receiving events, processing events, and publishing UI updates.
- Include formulas and example calculation only when aligned with the code; if the code differs from supplied notes, document both the old claim and verified implementation.
- Include practical extension instructions: add a sensor feature, add a fault scenario, update/retrain models safely, add a new processing metric, add an API endpoint, and add a test.
- Do not copy the stack trace wholesale; summarize it and show actionable fixes.
- Do not invent citations, paper results, file/function names, or test outcomes. Link the supplied baseline paper by DOI only as project context; distinguish external-paper claims from this project's reproduction.

# Workstream I — Final completion report

When work is complete, provide a concise report with:

1. What changed in the UI and why.
2. Docker/Compose topology and the exact no-terminal start path for Windows.
3. What components can be controlled through the UI and what must remain alive.
4. Root cause and tested resolution for each supplied error, or why it remains unresolved.
5. Exact tests executed with pass/fail/not-run status.
6. Key new/changed files and documentation entry points.
7. Known remaining limitations and a prioritized next step.

## Definition of done

The task is complete only when the repository contains the implemented UI refresh, a repeatable containerized stack, browser-based lifecycle/fault controls connected to real backend actions, corrected dependency/network/model configuration, automated tests, and the documentation set above. A beautiful dashboard with fake data, Docker files that do not build, buttons that only change labels, or docs describing unimplemented features is not done.

Start with the repository audit, then implement and validate all workstreams in the same task. Do not ask for confirmation between workstreams unless an irreversible operation or a genuinely missing secret/decision makes it necessary. Preserve existing user work.
