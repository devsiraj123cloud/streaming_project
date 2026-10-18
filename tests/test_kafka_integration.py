"""
Integration tests for Kafka Producer and Consumer pipeline.
Verifies payload schema, simulation fallback, and broker integration when available.
"""

import pytest
import time
from streaming.kafka_producer import IIoTKafkaProducer
from streaming.kafka_consumer import IIoTKafkaConsumer
from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_TELEMETRY

def test_kafka_producer_sample_generation():
    producer = IIoTKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    sample = producer.simulator.generate_sample()
    
    assert "asset_id" in sample
    assert sample["asset_id"] == "TWIN-MTR-01"
    assert "vibration_rms" in sample
    assert "temperature" in sample
    assert "current_amp" in sample
    assert "fault_label" in sample
    assert "fault_name" in sample

def test_kafka_producer_fault_injection():
    producer = IIoTKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    producer.inject_fault(1, 0.9)
    assert producer.simulator.current_mode == 1
    assert producer.simulator.mode_intensity == 0.9
    
    producer.reset_fault()
    assert producer.simulator.current_mode == 0
    assert producer.simulator.mode_intensity == 0.0

def test_kafka_consumer_message_processing_contract():
    consumer = IIoTKafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    test_telemetry = {
        "timestamp": time.time(),
        "asset_id": "TWIN-MTR-01",
        "vibration_rms": 1.85,
        "vibration_peak": 2.8,
        "temperature": 58.0,
        "rpm": 1480.0,
        "current_amp": 24.0,
        "pressure_bar": 4.2,
        "acoustic_db": 67.5,
        "fault_label": 0,
        "fault_name": "Normal Operation"
    }
    
    # Process through digital twin and ML ensemble
    twin_state = consumer.digital_twin.process_telemetry(test_telemetry)
    prediction = consumer.ensemble.predict_stream_packet(test_telemetry, twin_state["residuals"])
    
    assert "composite_health_index" in twin_state
    assert "is_anomaly" in prediction
    assert "fault_code" in prediction
    assert prediction["is_anomaly"] is False
    assert prediction["fault_code"] == 0

def test_kafka_live_publish_contract():
    producer = IIoTKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    sample = producer.send_sample()
    assert sample is not None
    assert "asset_id" in sample
    consumer = IIoTKafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    assert consumer.digital_twin is not None
    assert consumer.ensemble is not None

