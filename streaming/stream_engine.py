"""
Unified Real-Time Streaming & Orchestration Engine.
Coordinates:
- Sensor Stream Simulator (Edge IIoT)
- Digital Twin Physics Model & Residuals
- Two-Stage ML Ensemble Fault Classifier (Ranpariya & Sharma 2025)
- Sliding Window Stream Aggregations (Spark Structured Streaming equivalent)
- WebSocket Real-time Broadcast to Live Dashboard
"""

import time
import asyncio
import logging
import json
import threading
from collections import deque
from typing import Dict, Any, List, Set
from data.generator import IIoTSensorStreamSimulator
from models.digital_twin import IndustrialDigitalTwin
from models.two_stage_ensemble import TwoStageFaultDetectionEnsemble
from streaming.kafka_producer import IIoTKafkaProducer
from streaming.kafka_consumer import IIoTKafkaConsumer
from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_TELEMETRY

logger = logging.getLogger("StreamEngine")
_kafka_queue = None
_kafka_worker_started = False
_kafka_producer = None

def _start_kafka_publisher():
    """Publish queued telemetry off the async dashboard event loop."""
    global _kafka_queue, _kafka_worker_started, _kafka_producer
    if _kafka_worker_started or not KAFKA_ENABLED:
        return
    import queue
    if _kafka_queue is None:
        _kafka_queue = queue.Queue(maxsize=1000)
    try:
        from kafka import KafkaProducer
        _kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            key_serializer=lambda key: key.encode("utf-8") if key else None,
            request_timeout_ms=3000,
            max_block_ms=3000,
        )
        def publish_loop():
            while True:
                record = _kafka_queue.get()
                try:
                    _kafka_producer.send(KAFKA_TOPIC_TELEMETRY, key=record["asset_id"], value=record)
                except Exception:
                    logger.exception("Kafka telemetry publish failed")
                finally:
                    _kafka_queue.task_done()

        threading.Thread(target=publish_loop, name="kafka-telemetry-publisher", daemon=True).start()
        _kafka_worker_started = True
    except Exception:
        logger.exception("Unable to initialize Kafka telemetry publisher; continuing with in-process processing")

class RealTimeStreamingEngine:
    """
    Central real-time streaming engine managing high-frequency telemetry,
    digital twin state updates, ML inference, and dashboard streaming.
    """
    def __init__(self, interval_sec: float = 0.1):
        self.interval_sec = interval_sec
        self.simulator = IIoTSensorStreamSimulator()
        self.digital_twin = IndustrialDigitalTwin()
        self.ensemble = TwoStageFaultDetectionEnsemble()
        
        # State tracking
        self.is_running = False
        self.packet_counter = 0
        self.start_time = time.time()
        
        # Sliding buffer for rolling statistics (last 100 packets)
        self.recent_buffer = deque(maxlen=100)
        self.recent_alerts = deque(maxlen=20)
        
        # Active WebSocket clients
        self.subscribers: Set[Any] = set()
        
        # Performance profiling
        self.avg_stream_throughput = 10.0  # packets/sec
        self.last_packet: Dict[str, Any] = {}
        self.kafka_consumer = None
        self._kafka_consumer_thread = None
        self._kafka_error = None
        self._event_loop = None

        # Kafka Streaming Integration (Ranpariya & Sharma 2025 Architecture)
        self.kafka_bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
        self.kafka_topic = KAFKA_TOPIC_TELEMETRY
        try:
            self.kafka_producer = IIoTKafkaProducer(bootstrap_servers=self.kafka_bootstrap_servers, topic=self.kafka_topic)
            self.kafka_consumer = IIoTKafkaConsumer(bootstrap_servers=self.kafka_bootstrap_servers, topic=self.kafka_topic)
            self.kafka_active = bool(self.kafka_producer.producer is not None)
            if self.kafka_active:
                logger.info(f"[Engine] Kafka streaming active on topic '{self.kafka_topic}' ({self.kafka_bootstrap_servers})")
        except Exception as exc:
            logger.warning(f"[Engine] Kafka broker unavailable ({exc}); defaulting to in-process pipeline.")
            self.kafka_producer = None
            self.kafka_consumer = None
            self.kafka_active = False

    def inject_fault(self, fault_code: int, intensity: float = 1.0):
        """Dynamic fault injection triggered from dashboard controls."""
        self.simulator.set_fault_mode(fault_code, intensity)
        if self.kafka_producer:
            self.kafka_producer.inject_fault(fault_code, intensity)
        logger.info(f"[Engine] Fault state set to {fault_code} (intensity: {intensity})")

    def reset_fault(self):
        """Restore pristine normal operating conditions."""
        self.simulator.reset_to_normal()
        if self.kafka_producer:
            self.kafka_producer.reset_fault()
        logger.info("[Engine] Restored to normal operation.")

    def get_kafka_info(self) -> Dict[str, Any]:
        """Returns live Kafka broker and telemetry metrics."""
        connected = bool(self.kafka_producer and self.kafka_producer.producer is not None)
        return {
            "active": connected,
            "bootstrap_servers": self.kafka_bootstrap_servers,
            "topic": self.kafka_topic,
            "group_id": "iiot-digital-twin-cg",
            "messages_produced": getattr(self.kafka_producer, "messages_sent", 0) if self.kafka_producer else 0,
            "messages_consumed": getattr(self.kafka_consumer, "messages_received", 0) if self.kafka_consumer else 0,
            "mode": "Active Kafka KRaft Broker Stream" if connected else "In-Process Simulation Fallback"
        }

    def register_client(self, ws):
        """Add active WebSocket connection."""
        self.subscribers.add(ws)

    def unregister_client(self, ws):
        """Remove WebSocket connection."""
        self.subscribers.discard(ws)

    def compute_sliding_window_metrics(self) -> Dict[str, float]:
        """
        Computes 5-second window rolling aggregates matching Spark Structured Streaming aggregations.
        """
        if not self.recent_buffer:
            return {}
            
        recent = list(self.recent_buffer)[-50:]  # roughly last 5 seconds at 10 Hz
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
            "rolling_avg_delay_ms": round(sum(delays) / len(delays), 2)
        }

    async def step(self) -> Dict[str, Any]:
        """
        Executes a single streaming cycle:
        1. Sensor sample generation & Kafka publishing
        2. Kafka message polling & Digital Twin synchronization
        3. Two-Stage Ensemble inference
        4. Alert logging
        """
        if KAFKA_ENABLED:
            raise RuntimeError("step() is only available when Kafka mode is disabled; Kafka mode processes records in its consumer")
        self.packet_counter += 1
        t_now = time.time()
        
        # 1. Physics-based telemetry
        raw_telemetry = self.simulator.generate_sample(t_now)
        if KAFKA_ENABLED:
            _start_kafka_publisher()
            if _kafka_queue is None or not _kafka_worker_started:
                raise RuntimeError("Kafka is enabled, but the telemetry producer is unavailable")
            try:
                _kafka_queue.put_nowait(raw_telemetry)
            except Exception as exc:
                logger.error("Kafka publish queue is full; telemetry sample was not published")
                raise RuntimeError("Kafka telemetry queue is full") from exc
        
        # 2. Ingest through Kafka if active
        kafka_meta = None
        if self.kafka_active and self.kafka_producer and self.kafka_producer.producer:
            try:
                self.kafka_producer.send_sample(raw_telemetry)
                if self.kafka_consumer:
                    consumed = self.kafka_consumer.poll_packet(timeout_ms=10)
                    if consumed:
                        raw_telemetry = consumed["raw_telemetry"]
                        twin_state = consumed["digital_twin"]
                        prediction = consumed["prediction"]
                        kafka_meta = consumed.get("kafka_metadata")
            except Exception as e:
                logger.warning(f"[Engine] Kafka step error: {e}")

        # In-process Digital Twin mathematical synchronization & residuals if not consumed from Kafka
        if kafka_meta is None:
            twin_state = self.digital_twin.process_telemetry(raw_telemetry)
            prediction = self.ensemble.predict_stream_packet(raw_telemetry, twin_state["residuals"])
        
        # 4. Construct unified telemetry payload
        packet = {
            "packet_id": self.packet_counter,
            "timestamp": round(t_now, 4),
            "raw_telemetry": raw_telemetry,
            "digital_twin": twin_state,
            "prediction": prediction,
            "window_aggregates": self.compute_sliding_window_metrics(),
            "kafka_enabled": self.kafka_active,
            "kafka_metadata": kafka_meta
        }
        
        self.recent_buffer.append(packet)
        self.last_packet = packet
        
        # 5. Manage alert audit feed
        if prediction["is_anomaly"]:
            alert_entry = {
                "id": f"ALT-{self.packet_counter}",
                "timestamp": time.strftime("%H:%M:%S", time.localtime(t_now)),
                "fault_name": prediction["fault_name"],
                "severity": prediction["severity_level"],
                "confidence": prediction["confidence_percent"],
                "delay_ms": prediction["estimation_delay_ms"],
                "health_index": twin_state["composite_health_index"]
            }
            # Avoid flooding duplicate alerts within 5 packets
            if not self.recent_alerts or self.recent_alerts[-1]["fault_name"] != alert_entry["fault_name"]:
                self.recent_alerts.append(alert_entry)

        return packet

    async def run_loop(self):
        """Asynchronous streaming loop running continuously."""
        if KAFKA_ENABLED:
            self._event_loop = asyncio.get_running_loop()
            self._start_kafka_consumer()
        self.is_running = True
        logger.info("[Engine] Streaming loop active at 10 Hz.")
        
        while self.is_running:
            t0 = time.time()
            if KAFKA_ENABLED:
                raw_telemetry = self.simulator.generate_sample(time.time())
                _start_kafka_publisher()
                if _kafka_queue is None or not _kafka_worker_started:
                    self.is_running = False
                    raise RuntimeError("Kafka is enabled, but the telemetry producer is unavailable")
                try:
                    _kafka_queue.put_nowait(raw_telemetry)
                except Exception as exc:
                    logger.error("Kafka publish queue is full; stopping stream")
                    self.is_running = False
                    raise RuntimeError("Kafka telemetry queue is full") from exc
                packet = None
            else:
                packet = await self.step()
            
            # Broadcast to active WebSocket connections
            if packet is not None and self.subscribers:
                dead_sockets = set()
                for ws in self.subscribers:
                    try:
                        await ws.send_json(packet)
                    except Exception:
                        dead_sockets.add(ws)
                for dead in dead_sockets:
                    self.unregister_client(dead)
                    
            elapsed = time.time() - t0
            sleep_time = max(0.005, self.interval_sec - elapsed)
            await asyncio.sleep(sleep_time)

    def _start_kafka_consumer(self):
        if self._kafka_consumer_thread and self._kafka_consumer_thread.is_alive():
            return
        self.kafka_consumer = IIoTKafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            message_callback=self._handle_kafka_packet,
            simulator=self.simulator,
        )
        self._kafka_consumer_thread = threading.Thread(
            target=self.kafka_consumer.start_consuming,
            name="kafka-telemetry-consumer",
            daemon=True,
        )
        self._kafka_consumer_thread.start()

    def _handle_kafka_packet(self, processed: Dict[str, Any]):
        """Update dashboard state using a telemetry record consumed from Kafka."""
        self.packet_counter += 1
        t_now = processed["raw_telemetry"].get("timestamp", time.time())
        packet = {
            "packet_id": self.packet_counter,
            "timestamp": round(t_now, 4),
            "raw_telemetry": processed["raw_telemetry"],
            "digital_twin": processed["digital_twin"],
            "prediction": processed["prediction"],
            "window_aggregates": self.compute_sliding_window_metrics(),
        }
        self.recent_buffer.append(packet)
        self.last_packet = packet
        if packet["prediction"]["is_anomaly"]:
            alert = {
                "id": f"ALT-{self.packet_counter}",
                "timestamp": time.strftime("%H:%M:%S", time.localtime(t_now)),
                "fault_name": packet["prediction"]["fault_name"],
                "severity": packet["prediction"]["severity_level"],
                "confidence": packet["prediction"]["confidence_percent"],
                "delay_ms": packet["prediction"]["estimation_delay_ms"],
                "health_index": packet["digital_twin"]["composite_health_index"],
            }
            if not self.recent_alerts or self.recent_alerts[-1]["fault_name"] != alert["fault_name"]:
                self.recent_alerts.append(alert)
        if self._event_loop and self._event_loop.is_running():
            asyncio.run_coroutine_threadsafe(self._broadcast_packet(packet), self._event_loop)

    async def _broadcast_packet(self, packet: Dict[str, Any]):
        dead = set()
        for ws in self.subscribers:
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
