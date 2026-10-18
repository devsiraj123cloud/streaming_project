"""
Tests for IndustrialDigitalTwin physics model and Health Index computation.
"""

import pytest
from models.digital_twin import IndustrialDigitalTwin

def test_digital_twin_init():
    dt = IndustrialDigitalTwin(asset_id="TEST-TWIN-01")
    assert dt.asset_id == "TEST-TWIN-01"
    assert dt.health_index == 100.0
    assert "vibration_rms" in dt.residual_baselines

def test_digital_twin_nominal_processing():
    dt = IndustrialDigitalTwin()
    nominal_telemetry = {
        "timestamp": 1000.0,
        "vibration_rms": 1.95,
        "vibration_peak": 2.9,
        "temperature": 58.0,
        "rpm": 1480.0,
        "current_amp": 24.0,
        "pressure_bar": 4.2,
        "acoustic_db": 67.5,
    }
    result = dt.process_telemetry(nominal_telemetry)
    assert result["asset_id"] == "TWIN-MTR-01"
    assert "residuals" in result
    assert "z_scores" in result
    assert "composite_health_index" in result
    assert result["composite_health_index"] >= 80.0
    assert result["health_status"] in ["Optimal / Healthy", "Degradation Warning"]

def test_digital_twin_fault_degradation():
    dt = IndustrialDigitalTwin()
    severe_telemetry = {
        "timestamp": 1001.0,
        "vibration_rms": 9.5,
        "vibration_peak": 18.0,
        "temperature": 92.0,
        "rpm": 1350.0,
        "current_amp": 36.0,
        "pressure_bar": 2.1,
        "acoustic_db": 88.0,
    }
    # Process multiple packets to allow smoothing filter to decay
    for _ in range(15):
        result = dt.process_telemetry(severe_telemetry)
    
    assert result["composite_health_index"] < 60.0
    assert result["health_status"] == "Critical Fault Imminent"
    assert result["health_color"] == "#ef4444"

def test_digital_twin_clamping():
    dt = IndustrialDigitalTwin()
    extreme_telemetry = {
        "timestamp": 1002.0,
        "vibration_rms": 99.0,
        "vibration_peak": 199.0,
        "temperature": 250.0,
        "rpm": 500.0,
        "current_amp": 100.0,
        "pressure_bar": 0.1,
        "acoustic_db": 140.0,
    }
    for _ in range(30):
        result = dt.process_telemetry(extreme_telemetry)
    assert result["composite_health_index"] >= 5.0
    assert result["composite_health_index"] <= 100.0

def test_digital_twin_faults_do_not_collapse_everything_to_floor():
    for fault_code in range(1, 6):
        dt = IndustrialDigitalTwin()
        telemetry = {
            "timestamp": 1003.0,
            "vibration_rms": 7.0,
            "vibration_peak": 12.0,
            "temperature": 100.0,
            "rpm": 1420.0,
            "current_amp": 36.0,
            "pressure_bar": 1.5,
            "acoustic_db": 95.0,
        }
        for _ in range(20):
            result = dt.process_telemetry(telemetry)

        assert result["composite_health_index"] > 5.0
        assert result["composite_health_index"] < 60.0
