"""
Kafka Producer for IIoT Sensor Telemetry Stream.
Publishes the dashboard simulator's sensor records to Kafka.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Optional, Dict, Any

base_dir = Path(__file__).resolve().parent.parent
if str(base_dir) not in sys.path:
    sys.path.insert(0, str(base_dir))

from data.generator import IIoTSensorStreamSimulator
from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_TELEMETRY, STREAM_BATCH_INTERVAL_SEC

logger = logging.getLogger("KafkaProducer")


class IIoTKafkaProducer:
    """Publishes sensor telemetry to a Kafka topic."""

    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
                 topic: str = KAFKA_TOPIC_TELEMETRY):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.simulator = IIoTSensorStreamSimulator()
        self.producer = None
        self.is_running = False
        self.messages_sent = 0
        self.messages_failed = 0
        self._init_producer()

    def _init_producer(self):
        try:
            from kafka import KafkaProducer
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                api_version=(2, 6, 0),
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                key_serializer=lambda key: key.encode("utf-8") if key else None,
                acks="all",
                retries=3,
                request_timeout_ms=3000,
                max_block_ms=3000,
            )
            logger.info("Kafka producer initialized for %s", self.bootstrap_servers)
        except Exception as exc:
            self.producer = None
            logger.warning("Could not initialize Kafka producer at %s: %s", self.bootstrap_servers, exc)

    def send_sample(self, record: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Queue one record and count it only after Kafka acknowledges delivery."""
        if record is None:
            record = self.simulator.generate_sample()
        if self.producer is None:
            self.messages_failed += 1
            logger.warning("Telemetry not published: Kafka producer is unavailable")
            return record

        try:
            future = self.producer.send(
                self.topic,
                key=record.get("asset_id", "TWIN-MTR-01"),
                value=record,
            )
            future.get(timeout=5)
            self.messages_sent += 1
        except Exception as exc:
            self.messages_failed += 1
            logger.warning("Kafka send failed for topic %s: %s", self.topic, exc)
        return record

    def is_connected(self) -> bool:
        if self.producer is None:
            return False
        try:
            return bool(self.producer.bootstrap_connected())
        except Exception:
            return False

    def start_streaming(self, max_records: Optional[int] = None,
                        interval_sec: float = STREAM_BATCH_INTERVAL_SEC):
        self.is_running = True
        count = 0
        logger.info("Starting Kafka stream to topic '%s' at %.3f-second cadence", self.topic, interval_sec)
        try:
            while self.is_running:
                self.send_sample(self.simulator.generate_sample())
                count += 1
                if max_records is not None and count >= max_records:
                    break
                import time
                time.sleep(interval_sec)
        except KeyboardInterrupt:
            logger.info("Kafka streaming stopped by user")
        finally:
            self.stop()

    def inject_fault(self, fault_code: int, intensity: float = 1.0):
        self.simulator.set_fault_mode(fault_code, intensity)

    def reset_fault(self):
        self.simulator.reset_to_normal()

    def stop(self):
        self.is_running = False
        if self.producer is not None:
            try:
                self.producer.flush(timeout=3)
                self.producer.close(timeout=3)
            except Exception:
                logger.debug("Error closing Kafka producer", exc_info=True)
            finally:
                self.producer = None


if __name__ == "__main__":
    IIoTKafkaProducer().start_streaming()
