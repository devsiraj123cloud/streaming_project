"""
Unified Real-Time Streaming & Orchestration Engine.
Coordinates simulated telemetry, digital-twin residuals, two-stage ML,
rolling window metrics, optional Kafka transport, and WebSocket broadcasts.
"""

import asyncio
import logging
import threading
import time
from collections import deque
from typing import Any, Dict, Set

from data.generator import IIoTSensorStreamSimulator
from models.digital_twin import IndustrialDigitalTwin
from models.two_stage_ensemble import TwoStageFaultDetectionEnsemble
from streaming.kafka_producer import IIoTKafkaProducer
from streaming.kafka_consumer import IIoTKafkaConsumer
from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_ENABLED, KAFKA_TOPIC_TELEMETRY

logger = logging.getLogger("StreamEngine")


class RealTimeStreamingEngine:
    """Coordinates live telemetry, model inference and dashboard broadcasts."""

    def __init__(self, interval_sec: float = 0.1):
        self.interval_sec = interval_sec
        self.simulator = IIoTSensorStreamSimulator()
        self.digital_twin = IndustrialDigitalTwin()
        self.ensemble = TwoStageFaultDetectionEnsemble()
        self.is_running = False
        self.packet_counter = 0
        self.start_time = time.time()
        self.recent_buffer = deque(maxlen=100)
        self.recent_alerts = deque(maxlen=20)
        self.subscribers: Set[Any] = set()
        self.avg_stream_throughput = 10.0
        self.last_packet: Dict[str, Any] = {}
        self.kafka_consumer = None
        self.kafka_producer = None
        self._kafka_consumer_thread = None
        self._kafka_error = None
        self._event_loop = None
        self._publisher_count = 0
        self._consumer_callback_count = 0
        self._kafka_lock = threading.Lock()

        self.kafka_bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
        self.kafka_topic = KAFKA_TOPIC_TELEMETRY
        self.kafka_active = False
        if KAFKA_ENABLED:
            try:
                self.kafka_producer = IIoTKafkaProducer(
                    bootstrap_servers=self.kafka_bootstrap_servers,
                    topic=self.kafka_topic,
                )
                self.kafka_active = self.kafka_producer.is_connected()
                if self.kafka_active:
                    logger.info("Kafka active on topic '%s' (%s)", self.kafka_topic, self.kafka_bootstrap_servers)
            except Exception as exc:
                logger.warning("Kafka unavailable; using in-process fallback: %s", exc)
        if not self.kafka_active:
            logger.info("In-process streaming mode enabled")

    def inject_fault(self, fault_code: int, intensity: float = 1.0):
        """Apply a fault to the simulator used by the dashboard stream."""
        self.simulator.set_fault_mode(fault_code, intensity)
        logger.info("Fault state set to %s (intensity %.2f)", fault_code, intensity)

    def reset_fault(self):
        """Restore normal simulator operation."""
        self.simulator.reset_to_normal()
        logger.info("Restored normal operating conditions")

    def get_kafka_info(self) -> Dict[str, Any]:
        """Return connection state plus send/receive counters from the active clients."""
        connected = bool(self.kafka_active and self.kafka_producer and self.kafka_producer.is_connected())
        consumer_error = str(self.kafka_consumer.error) if self.kafka_consumer and self.kafka_consumer.error else None
        return {
            "active": connected,
            "bootstrap_servers": self.kafka_bootstrap_servers,
            "topic": self.kafka_topic,
            "group_id": "iiot-digital-twin-cg",
            "messages_produced": self._publisher_count,
            "messages_consumed": self._consumer_callback_count,
            "consumer_error": consumer_error,
            "mode": "Active Kafka stream" if connected else "In-process simulation fallback",
        }

    def register_client(self, ws):
        self.subscribers.add(ws)

    def unregister_client(self, ws):
        self.subscribers.discard(ws)

    def compute_sliding_window_metrics(self) -> Dict[str, float]:
        """Calculate rolling aggregates over up to 50 samples (~5 seconds at 10 Hz)."""
        if not self.recent_buffer:
            return {}
        recent = list(self.recent_buffer)[-50:]
        vibs = [p["raw_telemetry"]["vibration_rms"] for p in recent]
        temps = [p["raw_telemetry"]["temperature"] for p in recent]
        currs = [p["raw_telemetry"]["current_amp"] for p in recent]
        delays = [p["prediction"]["estimation_delay_ms"] for p in recent]
        return {
            "window_size_samples": len(recent),
            "window_duration_sec": round(len(recent) * self.interval_sec, 1),
            "rolling_avg_vibration": round(sum(vibs) / len(vibs), 3),
            "rolling_max_vibration": round(max(vibs), 3),
            "rolling_avg_temp": round(sum(temps) / len(temps), 2),
            "rolling_avg_current": round(sum(currs) / len(currs), 2),
            "rolling_avg_delay_ms": round(sum(delays) / len(delays), 2),
        }

    def _build_packet(self, raw: dict, twin_state: dict, prediction: dict, timestamp: float) -> dict:
        packet_id = self.packet_counter + 1
        packet = {
            "packet_id": packet_id,
            "timestamp": round(timestamp, 4),
            "raw_telemetry": raw,
            "digital_twin": twin_state,
            "prediction": prediction,
            "window_aggregates": {},
            "kafka_enabled": self.kafka_active,
        }
        self.recent_buffer.append(packet)
        packet["window_aggregates"] = self.compute_sliding_window_metrics()
        self.packet_counter = packet_id
        self.last_packet = packet
        self._record_alert(packet)
        return packet

    def _record_alert(self, packet: dict):
        pred = packet["prediction"]
        if not pred.get("is_anomaly"):
            return
        alert = {
            "id": f"ALT-{packet['packet_id']}",
            "timestamp": time.strftime("%H:%M:%S", time.localtime(packet["timestamp"])),
            "fault_name": pred.get("fault_name", "Unknown anomaly"),
            "severity": pred.get("severity_level", "WARNING"),
            "confidence": pred.get("confidence_percent", 0.0),
            "delay_ms": pred.get("estimation_delay_ms", 0.0),
            "health_index": packet["digital_twin"].get("composite_health_index"),
        }
        if not self.recent_alerts or self.recent_alerts[-1]["fault_name"] != alert["fault_name"]:
            self.recent_alerts.append(alert)

    async def step(self) -> Dict[str, Any]:
        """Generate and process one simulator sample in-process."""
        raw = self.simulator.generate_sample(time.time())
        twin = self.digital_twin.process_telemetry(raw)
        pred = self.ensemble.predict_stream_packet(raw, twin["residuals"])
        return self._build_packet(raw, twin, pred, raw.get("timestamp", time.time()))

    async def run_loop(self):
        """Run the dashboard stream continuously; Kafka publication is an observable side path."""
        self._event_loop = asyncio.get_running_loop()
        if KAFKA_ENABLED and self.kafka_active:
            self._start_kafka_consumer()
        self.is_running = True
        logger.info("Streaming loop active at %.1f Hz", 1.0 / self.interval_sec)

        while self.is_running:
            started = time.monotonic()
            try:
                # Always produce a dashboard packet so the UI updates even if the Kafka
                # consumer is slow or unavailable. Publish the same telemetry record to Kafka.
                packet = await self.step()
                if KAFKA_ENABLED and self.kafka_active and self.kafka_producer:
                    try:
                        self.kafka_producer.send_sample(packet["raw_telemetry"])
                        with self._kafka_lock:
                            self._publisher_count = self.kafka_producer.messages_sent
                    except Exception:
                        logger.exception("Kafka publish failed; dashboard continues in in-process mode")
                if self.subscribers:
                    await self._broadcast_packet(packet)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Streaming cycle failed")
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.005, self.interval_sec - elapsed))

    def _start_kafka_consumer(self):
        if self._kafka_consumer_thread and self._kafka_consumer_thread.is_alive():
            return
        self.kafka_consumer = IIoTKafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC_TELEMETRY,
            message_callback=self._handle_kafka_packet,
        )
        self._kafka_consumer_thread = threading.Thread(
            target=self.kafka_consumer.start_consuming,
            name="kafka-telemetry-consumer",
            daemon=True,
        )
        self._kafka_consumer_thread.start()

    def _handle_kafka_packet(self, processed: Dict[str, Any]):
        """Count successfully consumed records; dashboard rendering stays on one live path."""
        with self._kafka_lock:
            self._consumer_callback_count += 1
        # Dashboard data is broadcast from the authoritative in-process stream above.
        # The consumer callback exists to make Kafka delivery/consumption observable.
        logger.debug("Kafka record consumed: %s", processed.get("kafka_metadata"))

    async def _broadcast_packet(self, packet: Dict[str, Any]):
        dead = set()
        for ws in list(self.subscribers):
            try:
                await ws.send_json(packet)
            except Exception:
                dead.add(ws)
        for ws in dead:
            self.unregister_client(ws)

    def stop(self):
        self.is_running = False
        if self.kafka_producer:
            self.kafka_producer.stop()
        if self.kafka_consumer:
            self.kafka_consumer.stop()
