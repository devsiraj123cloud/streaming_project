"""
Kafka Producer for IIoT Sensor Telemetry Stream.
Continuously streams multi-variable sensor readings from physical asset simulator to Kafka broker.
"""

import time
import json
import logging
import sys
from pathlib import Path
from typing import Optional, Dict, Any

base_dir = Path(__file__).resolve().parent.parent
if str(base_dir) not in sys.path:
    sys.path.insert(0, str(base_dir))

from data.generator import IIoTSensorStreamSimulator
from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_ENABLED, KAFKA_TOPIC_TELEMETRY, STREAM_BATCH_INTERVAL_SEC

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("KafkaProducer")

class IIoTKafkaProducer:
    """Publishes continuous telemetry to Kafka topic."""
    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS, topic: str = KAFKA_TOPIC_TELEMETRY):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.simulator = IIoTSensorStreamSimulator()
        self.producer = None
        self.is_running = False
        self.messages_sent = 0
        
        self._init_producer()

    def _init_producer(self):
        try:
            from kafka import KafkaProducer
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                api_version=(2, 6, 0),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks=1,
                retries=3,
                request_timeout_ms=3000,
                max_block_ms=3000
            )
            logger.info(f"Connected to Kafka broker at {self.bootstrap_servers}")
        except Exception as e:
            logger.warning(f"Could not connect to Kafka broker at {self.bootstrap_servers} ({e}). Running in fallback mode.")
            self.producer = None

    def send_sample(self, record: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Publishes a single sensor sample to Kafka topic (or generates one if omitted)."""
        if record is None:
            record = self.simulator.generate_sample()
        key = record.get("asset_id", "TWIN-MTR-01")
        if self.producer:
            try:
                self.producer.send(self.topic, key=key, value=record)
                self.messages_sent += 1
            except Exception as e:
                logger.warning(f"Kafka send failed: {e}")
        return record

    def is_connected(self) -> bool:
        """Return whether the Kafka client currently has a broker connection."""
        if self.producer is None:
            return False
        try:
            return bool(self.producer.bootstrap_connected())
        except Exception:
            return False

    def start_streaming(self, max_records: Optional[int] = None, interval_sec: float = STREAM_BATCH_INTERVAL_SEC):
        """Streams sensor data continuously to Kafka."""
        self.is_running = True
        logger.info(f"Starting continuous streaming to topic '{self.topic}' (cadence: {interval_sec}s)...")
        count = 0
        
        try:
            while self.is_running:
                record = self.simulator.generate_sample()
                key = record["asset_id"]
                
                if self.producer:
                    self.producer.send(self.topic, key=key, value=record)
                    self.producer.flush()
                else:
                    # Simulation logging
                    if count % 10 == 0:
                        logger.info(f"[Simulated Kafka Stream] Asset: {key} | RMS: {record['vibration_rms']} | Temp: {record['temperature']}°C | Mode: {record['fault_name']}")
                        
                count += 1
                if max_records and count >= max_records:
                    break
                    
                time.sleep(interval_sec)
        except KeyboardInterrupt:
            logger.info("Kafka streaming stopped by user.")
        finally:
            self.stop()

    def inject_fault(self, fault_code: int, intensity: float = 1.0):
        """Dynamically switches generator fault state."""
        self.simulator.set_fault_mode(fault_code, intensity)
        logger.info(f"Injected fault code {fault_code} (intensity: {intensity}) into stream.")

    def reset_fault(self):
        """Restores normal operating state."""
        self.simulator.reset_to_normal()
        logger.info("Reset simulator to normal operation.")

    def stop(self):
        self.is_running = False
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer closed.")

if __name__ == "__main__":
    producer = IIoTKafkaProducer()
    producer.start_streaming()
