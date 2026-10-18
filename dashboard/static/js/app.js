/**
 * Digital Twin Operations Console — Main Application Controller
 * IOE413 Streaming & Data Analytics | Team 10
 *
 * Responsibilities:
 *  - WebSocket connection with exponential-backoff reconnect
 *  - Real-time telemetry rendering (no fake/hardcoded values)
 *  - Service lifecycle controls (Start/Stop/Restart)
 *  - Fault injection wired to real backend API
 *  - Benchmark data fetch and display with provenance labels
 *  - Alert feed with stable IDs and empty state
 */

(function initApp() {
  'use strict';

  // ------------------------------------------------------------------
  // Sub-module references (loaded before this script)
  // ------------------------------------------------------------------
  let twinVisualizer = null;
  let chartsManager  = null;

  try { twinVisualizer = new DigitalTwinVisualizer('twinCanvas'); }
  catch (e) { console.error('[App] twinVisualizer init failed:', e); }

  try { chartsManager = new TelemetryChartsManager(); }
  catch (e) { console.error('[App] chartsManager init failed:', e); }

  // ------------------------------------------------------------------
  // DOM cache
  // ------------------------------------------------------------------
  const $ = id => document.getElementById(id);

  const dom = {
    // Header
    wsDot:          $('wsDot'),
    wsLabel:        $('wsLabel'),
    engineDot:      $('engineDot'),
    engineLabel:    $('engineLabel'),
    kafkaDot:       $('kafkaDot'),
    kafkaLabel:     $('kafkaLabel'),
    pipeKafka:      $('pipeKafka'),
    pipeKafkaSub:   $('pipeKafkaSub'),
    packetCount:    $('packetCount'),
    reconnectBanner:$('reconnectBanner'),
    reconnectMsg:   $('reconnectMsg'),

    // KPI strip
    kpiCHI:         $('kpiCHI'),
    kpiCHIStatus:   $('kpiCHIStatus'),
    kpiCHIBar:      $('kpiCHIBar'),
    kpiCardCHI:     $('kpiCardCHI'),
    kpiCondition:   $('kpiCondition'),
    kpiConfText:    $('kpiConfText'),
    kpiAnomalyCount:$('kpiAnomalyCount'),
    kpiLastAlert:   $('kpiLastAlert'),
    kpiDelay:       $('kpiDelay'),
    kpiDelaySub:    $('kpiDelaySub'),
    kpiRate:        $('kpiRate'),
    kpiWindow:      $('kpiWindow'),

    // Twin panel
    twinChipRPM:    $('twinChipRPM'),
    twinChipLoad:   $('twinChipLoad'),
    twinChipFault:  $('twinChipFault'),
    sensorTemp:     $('sensorTemp'),
    sensorVib:      $('sensorVib'),
    sensorCurr:     $('sensorCurr'),
    sensorPress:    $('sensorPress'),
    twinTemp:       $('twinTemp'),
    twinVib:        $('twinVib'),
    twinCurr:       $('twinCurr'),
    twinPress:      $('twinPress'),
    resVib:         $('resVib'),
    resTemp:        $('resTemp'),
    resPress:       $('resPress'),
    resAcoustic:    $('resAcoustic'),

    // Inference panel
    stage1Box:      $('stage1Box'),
    stage1Result:   $('stage1Result'),
    stage1Delay:    $('stage1Delay'),
    stage2Box:      $('stage2Box'),
    stage2Result:   $('stage2Result'),
    stage2Delay:    $('stage2Delay'),
    kpiFaultName:   $('kpiFaultName'),
    kpiConfidence:  $('kpiConfidence'),
    kpiSeverity:    $('kpiSeverity'),
    kpiTotalDelay:  $('kpiTotalDelay'),

    // Fault injection
    faultIntensity: $('faultIntensity'),
    faultIntensityVal: $('faultIntensityVal'),
    faultFeedback:  $('faultFeedback'),

    // Service / controls
    serviceList:    $('serviceList'),
    btnStartEngine: $('btnStartEngine'),
    btnStopEngine:  $('btnStopEngine'),
    btnRestartEngine: $('btnRestartEngine'),
    btnResetFault:  $('btnResetFault'),
    ctrlLog:        $('ctrlLog'),

    // Alerts
    alertTableBody: $('alertTableBody'),
    alertEmptyRow:  $('alertEmptyRow'),
    alertCountBadge:$('alertCountBadge'),

    // Benchmark
    benchTableBody: $('benchTableBody'),
  };

  // ------------------------------------------------------------------
  // State
  // ------------------------------------------------------------------
  let ws            = null;
  let wsRetryCount  = 0;
  let wsRetryTimer  = null;
  let isDisposed    = false;
  let alertCount    = 0;
  let activeFault   = 0;
  let engineRunning = true;
  let lastPacketTime = 0;
  let staleSentinel  = null;

  // ------------------------------------------------------------------
  // Utility
  // ------------------------------------------------------------------
  function setText(el, val) { if (el) el.textContent = val; }
  function setClass(el, cls) { if (el) el.className = cls; }

  function appendCtrlLog(msg, type = 'info') {
    if (!dom.ctrlLog) return;
    const el = document.createElement('div');
    el.className = `ctrl-log-line ctrl-log-${type}`;
    el.textContent = `[${new Date().toLocaleTimeString()}] ${msg}`;
    dom.ctrlLog.appendChild(el);
    dom.ctrlLog.scrollTop = dom.ctrlLog.scrollHeight;
    // Remove initial "empty" placeholder
    const empty = dom.ctrlLog.querySelector('.ctrl-log-empty');
    if (empty) empty.remove();
  }

  function showFaultFeedback(msg, type = 'ok') {
    if (!dom.faultFeedback) return;
    dom.faultFeedback.textContent = msg;
    dom.faultFeedback.className = `fault-feedback ${type}`;
    dom.faultFeedback.hidden = false;
    clearTimeout(dom.faultFeedback._timer);
    dom.faultFeedback._timer = setTimeout(() => {
      if (dom.faultFeedback) dom.faultFeedback.hidden = true;
    }, 3500);
  }

  // ------------------------------------------------------------------
  // WebSocket — exponential backoff reconnect
  // ------------------------------------------------------------------
  function wsConnect() {
    if (isDisposed) return;
    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${protocol}//${location.host}/ws/stream`;

    try {
      ws = new WebSocket(url);
    } catch (e) {
      console.error('[WS] Construction error:', e);
      scheduleReconnect();
      return;
    }

    ws.onopen = () => {
      wsRetryCount = 0;
      setWsStatus('healthy');
      if (dom.reconnectBanner) dom.reconnectBanner.hidden = true;
      appendCtrlLog('WebSocket connected', 'ok');
    };

    ws.onmessage = evt => {
      try {
        const packet = JSON.parse(evt.data);
        handleTelemetry(packet);
      } catch (e) {
        console.error('[WS] Parse error:', e);
      }
    };

    ws.onclose = () => {
      setWsStatus('offline');
      scheduleReconnect();
    };

    ws.onerror = err => {
      console.error('[WS] Error:', err);
    };
  }

  function scheduleReconnect() {
    if (isDisposed) return;
    if (wsRetryTimer) clearTimeout(wsRetryTimer);
    const delay = Math.min(1000 * Math.pow(1.6, wsRetryCount), 15000);
    wsRetryCount++;
    if (dom.reconnectBanner) dom.reconnectBanner.hidden = false;
    setText(dom.reconnectMsg, `WebSocket disconnected — reconnecting in ${(delay / 1000).toFixed(1)} s…`);
    wsRetryTimer = setTimeout(wsConnect, delay);
  }

  function setWsStatus(state) {
    const classes = { healthy: 'status-dot dot-healthy', degraded: 'status-dot dot-degraded', offline: 'status-dot dot-offline' };
    const labels  = { healthy: 'Stream Live', degraded: 'Degraded', offline: 'Disconnected' };
    setClass(dom.wsDot, classes[state] || 'status-dot dot-offline');
    setText(dom.wsLabel, labels[state] || 'Unknown');
  }

  // HTTP fallback polling (100 ms) when WebSocket is not open
  setInterval(async () => {
    if (ws && ws.readyState === WebSocket.OPEN) return;
    try {
      const r = await fetch('/api/status');
      const d = await r.json();
      if (d && d.latest_packet && d.latest_packet.packet_id) handleTelemetry(d.latest_packet);
      updateEngineStatus(d.stream_running);
    } catch (_) {}
  }, 200);

  // Stale-data detector — if no packet for 3 s, show degraded state
  function resetStaleDetector() {
    lastPacketTime = Date.now();
    clearTimeout(staleSentinel);
    staleSentinel = setTimeout(() => {
      if (Date.now() - lastPacketTime > 3000) setWsStatus('degraded');
    }, 3500);
  }

  // ------------------------------------------------------------------
  // Telemetry handler
  // ------------------------------------------------------------------
  function handleTelemetry(packet) {
    if (!packet) return;
    resetStaleDetector();

    setText(dom.packetCount, packet.packet_id ?? '');

    // Sub-module update
    try { if (twinVisualizer) twinVisualizer.updateTelemetry(packet); } catch (_) {}
    try { if (chartsManager)  chartsManager.update(packet); } catch (_) {}

    // --- Digital twin state ---
    const dt  = packet.digital_twin || {};
    const chi = typeof dt.composite_health_index === 'number' ? dt.composite_health_index : null;
    const raw = packet.raw_telemetry || {};
    const exp = dt.expected_state   || {};
    const res = dt.residuals        || {};
    const pred = packet.prediction  || {};
    const win  = packet.window_aggregates || {};

    // CHI KPI
    if (chi !== null) {
      setText(dom.kpiCHI, chi.toFixed(1));
      setText(dom.kpiCHIStatus, dt.health_status || '');
      if (dom.kpiCHIBar) {
        dom.kpiCHIBar.style.width = `${chi}%`;
        dom.kpiCHIBar.style.background = chi >= 85 ? 'var(--green)' : chi >= 60 ? 'var(--amber)' : 'var(--red)';
      }
      const card = dom.kpiCardCHI;
      if (card) {
        card.classList.toggle('state-warn', chi < 85 && chi >= 60);
        card.classList.toggle('state-crit', chi < 60);
      }
    }

    // Condition KPI
    const cond = dom.kpiCondition;
    if (cond && pred.fault_name) {
      cond.textContent = pred.is_anomaly ? pred.fault_name : 'Normal Operation';
      cond.className = 'kpi-badge-lg' +
        (pred.severity_level === 'CRITICAL' ? ' state-crit' :
         pred.severity_level === 'WARNING'  ? ' state-warn' : '');
    }
    setText(dom.kpiConfText, pred.confidence_percent != null ? `Confidence: ${pred.confidence_percent.toFixed(1)}%` : '');

    // Anomaly count KPI
    if (pred.is_anomaly) {
      alertCount++;
      setText(dom.kpiAnomalyCount, alertCount);
      setText(dom.kpiLastAlert, `Last: ${pred.fault_name || 'anomaly'}`);
      if (dom.alertCountBadge) dom.alertCountBadge.textContent = `${alertCount} alert${alertCount !== 1 ? 's' : ''}`;
      appendAlertRow(pred, chi);
    }

    // Delay KPI
    const delay = pred.estimation_delay_ms;
    if (delay != null) {
      setText(dom.kpiDelay, delay.toFixed(2));
      setText(dom.kpiDelaySub, delay <= 5.5 ? '✓ Within target (< ~5 ms)' : 'Exceeds target');
    }

    // Rate KPI
    if (win.window_size_samples) {
      setText(dom.kpiRate, ((win.window_size_samples / 5) || 10).toFixed(1));
    }
    if (win.rolling_max_vibration != null) {
      setText(dom.kpiWindow, `Roll. max vib: ${win.rolling_max_vibration.toFixed(2)} mm/s`);
    }

    // Twin readings
    setText(dom.sensorTemp,  raw.temperature    != null ? `${raw.temperature.toFixed(1)} °C`    : '—');
    setText(dom.sensorVib,   raw.vibration_rms  != null ? `${raw.vibration_rms.toFixed(3)} mm/s` : '—');
    setText(dom.sensorCurr,  raw.current_amp    != null ? `${raw.current_amp.toFixed(2)} A`      : '—');
    setText(dom.sensorPress, raw.pressure_bar   != null ? `${raw.pressure_bar.toFixed(3)} bar`   : '—');
    setText(dom.twinTemp,    exp.temperature    != null ? `${exp.temperature.toFixed(1)} °C`    : '—');
    setText(dom.twinVib,     exp.vibration_rms  != null ? `${exp.vibration_rms.toFixed(3)} mm/s` : '—');
    setText(dom.twinCurr,    exp.current_amp    != null ? `${exp.current_amp.toFixed(2)} A`      : '—');
    setText(dom.twinPress,   exp.pressure_bar   != null ? `${exp.pressure_bar.toFixed(3)} bar`   : '—');

    // Residuals
    setText(dom.resVib,      res.vibration_rms  != null ? `${res.vibration_rms.toFixed(4)}`  : '—');
    setText(dom.resTemp,     res.temperature    != null ? `${res.temperature.toFixed(3)} °C` : '—');
    setText(dom.resPress,    res.pressure_bar   != null ? `${res.pressure_bar.toFixed(4)}`   : '—');
    setText(dom.resAcoustic, res.acoustic_db    != null ? `${res.acoustic_db.toFixed(2)} dB` : '—');

    // Twin overlay chips
    setText(dom.twinChipRPM,  raw.rpm    != null ? `RPM: ${raw.rpm.toFixed(0)}`   : 'RPM: —');
    setText(dom.twinChipLoad, raw.current_amp != null
      ? `Load: ${Math.min(100, Math.max(0, ((raw.current_amp - 10) / 18.5) * 100)).toFixed(0)}%` : 'Load: —');

    if (dom.twinChipFault) {
      dom.twinChipFault.textContent = pred.is_anomaly ? (pred.fault_name || 'Anomaly') : 'Normal';
      dom.twinChipFault.className = 'twin-chip twin-chip-right' +
        (pred.severity_level === 'CRITICAL' ? ' state-crit' :
         pred.severity_level === 'WARNING'  ? ' state-warn' : '');
    }

    // Two-stage inference
    const s1 = pred.stage1 || {};
    const s2 = pred.stage2 || {};

    if (dom.stage1Box) {
      dom.stage1Box.className = 'stage-box' + (s1.flagged_anomaly ? ' state-anomaly' : '');
    }
    setText(dom.stage1Result, s1.flagged_anomaly ? `ANOMALY (p=${(s1.anomaly_probability || 0).toFixed(3)})` : 'NORMAL');
    setText(dom.stage1Delay,  `delay: ${(s1.delay_ms || 0).toFixed(2)} ms`);

    if (dom.stage2Box) {
      dom.stage2Box.className = 'stage-box' + (s2.invoked ? ' state-anomaly' : '');
    }
    setText(dom.stage2Result, s2.invoked ? `DIAGNOSED` : 'BYPASSED (fast-path)');
    setText(dom.stage2Delay,  `delay: ${(s2.delay_ms || 0).toFixed(2)} ms`);

    if (dom.kpiFaultName) {
      dom.kpiFaultName.textContent = pred.fault_name || 'Normal Operation';
      dom.kpiFaultName.className = 'fd-name' + (pred.is_anomaly ? ' state-crit' : '');
    }
    setText(dom.kpiConfidence, pred.confidence_percent != null ? `${pred.confidence_percent.toFixed(1)}%` : '—');
    setText(dom.kpiSeverity,   pred.severity_level || '—');
    setText(dom.kpiTotalDelay, delay != null ? `${delay.toFixed(2)} ms` : '—');

    updateEngineStatus(true);
  }

  // ------------------------------------------------------------------
  // Alert feed
  // ------------------------------------------------------------------
  let alertIds = new Set();

  function appendAlertRow(pred, chi) {
    const id = `ALT-${Date.now()}-${pred.fault_code}`;
    if (!dom.alertTableBody) return;

    // Remove empty-state row if present
    if (dom.alertEmptyRow && dom.alertEmptyRow.parentNode) {
      dom.alertEmptyRow.remove();
    }

    const tr = document.createElement('tr');
    tr.dataset.alertId = id;
    const sevClass = pred.severity_level === 'CRITICAL' ? 'sev-critical' : pred.severity_level === 'WARNING' ? 'sev-warning' : 'sev-normal';
    tr.innerHTML = `
      <td>${new Date().toLocaleTimeString()}</td>
      <td style="font-weight:600;color:var(--red)">${escHtml(pred.fault_name || '?')}</td>
      <td class="${sevClass}">${escHtml(pred.severity_level || '—')}</td>
      <td>${pred.estimation_delay_ms != null ? pred.estimation_delay_ms.toFixed(2) + ' ms' : '—'}</td>
      <td style="color:var(--cyan);font-family:var(--mono)">${chi != null ? chi.toFixed(1) + '%' : '—'}</td>
    `;
    dom.alertTableBody.insertBefore(tr, dom.alertTableBody.firstChild);

    // Keep max 15 rows
    while (dom.alertTableBody.children.length > 15) {
      dom.alertTableBody.removeChild(dom.alertTableBody.lastChild);
    }
  }

  function escHtml(s) {
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }

  // ------------------------------------------------------------------
  // Service status polling (every 3 s)
  // ------------------------------------------------------------------
  async function pollServices() {
    try {
      const r = await fetch('/api/services');
      const d = await r.json();
      renderServiceList(d.services || []);
    } catch (_) {}
  }

  function renderServiceList(services) {
    if (!dom.serviceList) return;
    dom.serviceList.innerHTML = '';
    services.forEach(svc => {
      const dot = stateToClass(svc.state);
      const row = document.createElement('div');
      row.className = 'service-row';
      row.innerHTML = `
        <div class="service-row-name">
          <span class="status-dot ${dot}"></span>
          ${escHtml(svc.name)}
          ${svc.note ? `<span class="text-dim" style="font-size:0.7rem">${escHtml(svc.note)}</span>` : ''}
        </div>
        <span class="service-row-state" style="color:${stateToColor(svc.state)}">${escHtml(svc.state)}</span>
      `;
      dom.serviceList.appendChild(row);
    });

    const kafkaSvc = services.find(s => s.id === 'kafka-broker');
    if (kafkaSvc && kafkaSvc.state === 'healthy') {
      if (dom.kafkaDot) setClass(dom.kafkaDot, 'status-dot dot-healthy');
      if (dom.kafkaLabel) setText(dom.kafkaLabel, `Kafka: Active (9092)`);
      if (dom.pipeKafka) setClass(dom.pipeKafka, 'pipe-node pipe-active');
      if (dom.pipeKafkaSub) setText(dom.pipeKafkaSub, `${kafkaSvc.messages_produced || 0} msgs`);
    } else {
      if (dom.kafkaDot) setClass(dom.kafkaDot, 'status-dot dot-offline');
      if (dom.kafkaLabel) setText(dom.kafkaLabel, 'Kafka: Offline');
      if (dom.pipeKafka) setClass(dom.pipeKafka, 'pipe-node pipe-offline');
      if (dom.pipeKafkaSub) setText(dom.pipeKafkaSub, 'Fallback');
    }
  }

  function stateToClass(state) {
    if (state === 'healthy') return 'dot-healthy';
    if (state === 'degraded' || state === 'starting') return 'dot-degraded';
    if (state === 'failed' || state === 'stopped') return 'dot-error';
    return 'dot-offline';
  }
  function stateToColor(state) {
    if (state === 'healthy' || state === 'restarted') return 'var(--green)';
    if (state === 'degraded' || state === 'starting') return 'var(--amber)';
    if (state === 'failed') return 'var(--red)';
    if (state === 'stopped') return 'var(--red)';
    return 'var(--text-muted)';
  }

  function updateEngineStatus(running) {
    engineRunning = running;
    const s = running ? 'healthy' : 'stopped';
    setClass(dom.engineDot, `status-dot ${stateToClass(s)}`);
    setText(dom.engineLabel, running ? 'Engine Running' : 'Engine Stopped');
    if (dom.btnStartEngine) dom.btnStartEngine.disabled = running;
    if (dom.btnStopEngine)  dom.btnStopEngine.disabled  = !running;
  }

  setInterval(pollServices, 3000);
  pollServices();

  // ------------------------------------------------------------------
  // Operator lifecycle controls
  // ------------------------------------------------------------------
  async function serviceAction(id, action) {
    const btns = [dom.btnStartEngine, dom.btnStopEngine, dom.btnRestartEngine, dom.btnResetFault];
    btns.forEach(b => { if (b) b.disabled = true; });
    appendCtrlLog(`Requesting ${action} for ${id}…`, 'info');
    try {
      const r = await fetch(`/api/services/${id}/${action}`, { method: 'POST' });
      const d = await r.json();
      appendCtrlLog(`${action} → ${d.status}: ${d.message}`, r.ok ? 'ok' : 'err');
      await pollServices();
    } catch (e) {
      appendCtrlLog(`Error: ${e.message}`, 'err');
    } finally {
      // Re-enable based on actual state
      const r2 = await fetch('/api/services').catch(() => null);
      if (r2) {
        const d2 = await r2.json().catch(() => ({}));
        const eng = (d2.services || []).find(s => s.id === 'stream-engine');
        if (eng) updateEngineStatus(eng.state === 'healthy');
      }
      if (dom.btnRestartEngine) dom.btnRestartEngine.disabled = false;
      if (dom.btnResetFault)    dom.btnResetFault.disabled    = false;
    }
  }

  if (dom.btnStartEngine)   dom.btnStartEngine.addEventListener('click',   () => serviceAction('stream-engine', 'start'));
  if (dom.btnStopEngine)    dom.btnStopEngine.addEventListener('click',    () => serviceAction('stream-engine', 'stop'));
  if (dom.btnRestartEngine) dom.btnRestartEngine.addEventListener('click', () => serviceAction('stream-engine', 'restart'));

  if (dom.btnResetFault) {
    dom.btnResetFault.addEventListener('click', async () => {
      dom.btnResetFault.disabled = true;
      try {
        const r = await fetch('/api/fault/reset', { method: 'POST' });
        const d = await r.json();
        showFaultFeedback('Fault reset — normal operation restored', 'ok');
        appendCtrlLog('Fault reset to Normal', 'ok');
        // Update button states
        document.querySelectorAll('.fault-btn').forEach(b => {
          b.classList.remove('active');
          b.setAttribute('aria-pressed', 'false');
        });
        const btn0 = $('faultBtn0');
        if (btn0) { btn0.classList.add('active'); btn0.setAttribute('aria-pressed', 'true'); }
        activeFault = 0;
      } catch (e) {
        appendCtrlLog(`Reset error: ${e.message}`, 'err');
      } finally {
        dom.btnResetFault.disabled = false;
      }
    });
  }

  // ------------------------------------------------------------------
  // Fault injection buttons
  // ------------------------------------------------------------------
  document.querySelectorAll('.fault-btn').forEach(btn => {
    btn.addEventListener('click', async () => {
      const code = parseInt(btn.dataset.fault, 10);
      const intensity = dom.faultIntensity ? parseFloat(dom.faultIntensity.value) / 100 : 1.0;

      btn.disabled = true;
      try {
        const r = await fetch('/api/fault/inject', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ fault_code: code, intensity }),
        });
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const d = await r.json();

        document.querySelectorAll('.fault-btn').forEach(b => {
          b.classList.remove('active');
          b.setAttribute('aria-pressed', 'false');
        });
        btn.classList.add('active');
        btn.setAttribute('aria-pressed', 'true');
        activeFault = code;

        const faultNames = ['Normal','Bearing Fault','Stator Overheating','Rotor Imbalance','Cavitation','Sensor Drift'];
        showFaultFeedback(code === 0
          ? 'Fault cleared — normal operation restored'
          : `Injected: ${faultNames[code] || 'fault'} at ${(intensity * 100).toFixed(0)}% severity`, 'ok');
        appendCtrlLog(d.message || `Fault ${code} injected`, 'info');
      } catch (e) {
        showFaultFeedback(`Error injecting fault: ${e.message}`, 'err');
        appendCtrlLog(`Fault inject error: ${e.message}`, 'err');
      } finally {
        btn.disabled = false;
      }
    });
  });

  if (dom.faultIntensity && dom.faultIntensityVal) {
    dom.faultIntensity.addEventListener('input', () => {
      dom.faultIntensityVal.textContent = `${dom.faultIntensity.value}%`;
    });
  }

  // ------------------------------------------------------------------
  // Benchmark data
  // ------------------------------------------------------------------
  async function loadBenchmark() {
    if (!dom.benchTableBody) return;
    try {
      const r = await fetch('/api/benchmark');
      const d = await r.json();
      if (!d.models) {
        dom.benchTableBody.innerHTML = `<tr><td colspan="5" style="color:var(--text-muted);text-align:center;padding:0.75rem">${escHtml(d.message || 'No benchmark data')}</td></tr>`;
        return;
      }
      dom.benchTableBody.innerHTML = '';
      let first = true;
      for (const [key, m] of Object.entries(d.models)) {
        const tr = document.createElement('tr');
        if (first) { tr.className = 'bench-proposed'; first = false; }
        tr.innerHTML = `
          <td>${escHtml(m.name || key)}</td>
          <td>${m.accuracy != null ? m.accuracy.toFixed(2) + '%' : '—'}</td>
          <td>${m.f1_score != null ? m.f1_score.toFixed(2) + '%' : '—'}</td>
          <td>${m.avg_delay_ms != null ? m.avg_delay_ms.toFixed(2) + ' ms' : '—'}</td>
          <td>${escHtml(m.features_used || '—')}</td>
        `;
        dom.benchTableBody.appendChild(tr);
      }
    } catch (e) {
      if (dom.benchTableBody) {
        dom.benchTableBody.innerHTML = `<tr><td colspan="5" style="color:var(--text-muted);text-align:center;padding:0.75rem">Failed to load benchmark data</td></tr>`;
      }
    }
  }

  loadBenchmark();

  // ------------------------------------------------------------------
  // Bootstrap: fetch initial snapshot then connect WebSocket
  // ------------------------------------------------------------------
  fetch('/api/status')
    .then(r => r.json())
    .then(d => {
      if (d && d.latest_packet && Object.keys(d.latest_packet).length > 0) {
        handleTelemetry(d.latest_packet);
      }
      updateEngineStatus(d.stream_running !== false);
    })
    .catch(() => {});

  wsConnect();

  // Cleanup on page unload
  window.addEventListener('beforeunload', () => {
    isDisposed = true;
    if (wsRetryTimer) clearTimeout(wsRetryTimer);
    if (staleSentinel)  clearTimeout(staleSentinel);
    if (ws) ws.close();
  });

})();
