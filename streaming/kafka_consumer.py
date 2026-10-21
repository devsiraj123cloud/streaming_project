"""
Kafka Consumer for IIoT Sensor Stream.
Consumes continuous sensor data from Kafka and pipes into Digital Twin & ML Pipeline.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Callable, Optional

base_dir = Path(__file__).resolve().parent.parent
if str(base_dir) not in sys.path:
    sys.path.insert(0, str(base_dir))

from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_TELEMETRY
from models.digital_twin import IndustrialDigitalTwin
from models.two_stage_ensemble import TwoStageFaultDetectionEnsemble

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("KafkaConsumer")


class IIoTKafkaConsumer:
    """Consumes real-time sensor messages and invokes an optional callback."""

    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
                 topic: str = KAFKA_TOPIC_TELEMETRY,
                 message_callback: Optional[Callable] = None):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.message_callback = message_callback
        self.digital_twin = IndustrialDigitalTwin()
        self.ensemble = TwoStageFaultDetectionEnsemble()
        self.is_running = False
        self.consumer = None
        self.messages_received = 0
        self.error = None
        self._init_consumer()

    def _init_consumer(self):
        try:
            from kafka import KafkaConsumer
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                api_version=(2, 6, 0),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                group_id="iiot-digital-twin-cg",
                consumer_timeout_ms=500,
            )
            self.error = None
            logger.info("Subscribed to topic '%s' at %s", self.topic, self.bootstrap_servers)
        except Exception as exc:
            self.error = exc
            self.consumer = None
            logger.warning("Could not connect to Kafka consumer at %s (%s)", self.bootstrap_servers, exc)

    def _process_message(self, msg) -> dict:
        telemetry = msg.value
        self.messages_received += 1
        twin_state = self.digital_twin.process_telemetry(telemetry)
        prediction = self.ensemble.predict_stream_packet(telemetry, twin_state["residuals"])
        return {
            "raw_telemetry": telemetry,
            "digital_twin": twin_state,
            "prediction": prediction,
            "kafka_metadata": {
                "topic": msg.topic, "partition": msg.partition,
                "offset": msg.offset, "timestamp": msg.timestamp,
            },
        }

    def poll_packet(self, timeout_ms: int = 50) -> Optional[dict]:
        """Poll one message and process it through the digital twin and classifier."""
        if self.consumer is None:
            self._init_consumer()
        if self.consumer is None:
            return None
        try:
            records = self.consumer.poll(timeout_ms=timeout_ms, max_records=1)
            for messages in records.values():
                for msg in messages:
                    return self._process_message(msg)
        except Exception as exc:
            self.error = exc
            logger.warning("Kafka consumer poll error: %s", exc)
        return None

    def start_consuming(self, message_callback: Optional[Callable] = None):
        """Poll messages continuously, invoking the configured or supplied callback."""
        if self.consumer is None:
            self._init_consumer()
        if self.consumer is None:
            return
        callback = message_callback or self.message_callback
        self.is_running = True
        try:
            while self.is_running:
                records = self.consumer.poll(timeout_ms=500, max_records=100)
                for messages in records.values():
                    for msg in messages:
                        if not self.is_running:
                            break
                        processed = self._process_message(msg)
                        if callback:
                            callback(processed)
                        else:
                            prediction = processed["prediction"]
                            chi = processed["digital_twin"]["composite_health_index"]
                            logger.info("Anomaly: %s | Fault: %s | Delay: %sms | CHI: %s%%",
                                        prediction["is_anomaly"], prediction["fault_name"],
                                        prediction["estimation_delay_ms"], chi)
        except Exception as exc:
            self.error = exc
            logger.exception("Kafka consumer failed")
        finally:
            if self.consumer is not None:
                try:
                    self.consumer.close()
                except Exception:
                    logger.debug("Could not close Kafka consumer cleanly", exc_info=True)
                self.consumer = None

    def stop(self):
        """Request stop; poll timeout allows the consumer thread to exit cleanly."""
        self.is_running = False


if __name__ == "__main__":
    IIoTKafkaConsumer().start_consuming()
