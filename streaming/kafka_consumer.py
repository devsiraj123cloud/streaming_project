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
from data.generator import IIoTSensorStreamSimulator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("KafkaConsumer")

class IIoTKafkaConsumer:
    """Consumes real-time stream from Kafka topic and runs Digital Twin processing."""
    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS, topic: str = KAFKA_TOPIC_TELEMETRY, message_callback: Optional[Callable] = None, simulator: Optional[IIoTSensorStreamSimulator] = None):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.digital_twin = IndustrialDigitalTwin()
        self.ensemble = TwoStageFaultDetectionEnsemble()
        self.is_running = False
        self.consumer = None
        self.messages_received = 0
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
                consumer_timeout_ms=500
            )
            logger.info(f"Subscribed to topic '{self.topic}' at {self.bootstrap_servers}")
        except Exception as e:
            logger.warning(f"Could not connect to Kafka consumer at {self.bootstrap_servers} ({e})")
            self.consumer = None

    def poll_packet(self, timeout_ms: int = 50) -> Optional[dict]:
        """Polls a single message from Kafka topic, processes through Digital Twin & ML Ensemble."""
        if not self.consumer:
            self._init_consumer()
        if not self.consumer:
            return None
        try:
            records_dict = self.consumer.poll(timeout_ms=timeout_ms, max_records=1)
            for tp, records in records_dict.items():
                for msg in records:
                    telemetry = msg.value
                    self.messages_received += 1
                    twin_state = self.digital_twin.process_telemetry(telemetry)
                    prediction = self.ensemble.predict_stream_packet(telemetry, twin_state["residuals"])
                    return {
                        "raw_telemetry": telemetry,
                        "digital_twin": twin_state,
                        "prediction": prediction,
                        "kafka_metadata": {
                            "topic": msg.topic,
                            "partition": msg.partition,
                            "offset": msg.offset,
                            "timestamp": msg.timestamp
                        }
                    }
        except Exception as e:
            logger.warning(f"Kafka consumer poll error: {e}")
            return None
        return None

    def start_consuming(self, message_callback: Optional[Callable] = None):
        """Starts real-time consumption loop."""
        if not self.consumer:
            self._init_consumer()
        if not self.consumer:
            return
        self.is_running = True
        try:
            for msg in self.consumer:
                if not self.is_running:
                    break
                telemetry = msg.value
                self.messages_received += 1
                twin_state = self.digital_twin.process_telemetry(telemetry)
                prediction = self.ensemble.predict_stream_packet(telemetry, twin_state["residuals"])
                
                processed_packet = {
                    "raw_telemetry": telemetry,
                    "digital_twin": twin_state,
                    "prediction": prediction
                }
                
                if message_callback:
                    message_callback(processed_packet)
                else:
                    logger.info(f"[Consumer] Anomaly: {prediction['is_anomaly']} | Fault: {prediction['fault_name']} | Delay: {prediction['estimation_delay_ms']}ms | CHI: {twin_state['composite_health_index']}%")
                    
        except Exception as e:
            self.error = e
            logger.exception("Kafka consumer could not start")
        finally:
            if consumer is not None:
                consumer.close()

    def stop(self):
        self.is_running = False
        if self.consumer:
            try:
                self.consumer.close()
            except Exception:
                pass

if __name__ == "__main__":
    consumer = IIoTKafkaConsumer()
    consumer.start_consuming()
