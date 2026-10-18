"""
Digital Twin Core Engine for Industrial Rotating Asset (Induction Motor-Pump).
Provides real-time mathematical state tracking, residual computation, and Health Index evaluation.
Based on Ranpariya & Sharma (2025) Digital Twin methodology.
"""

import math
import numpy as np
from typing import Dict, Any, Tuple

class IndustrialDigitalTwin:
    """
    Virtual mathematical twin representing physical asset TWIN-MTR-01.
    Tracks expected operational physics, calculates multi-sensor residuals,
    and maintains composite asset health index (CHI).
    """
    def __init__(self, asset_id: str = "TWIN-MTR-01"):
        self.asset_id = asset_id
        
        # Nominal physical parameters (derived from asset rating specs)
        self.rated_rpm = 1480.0
        self.ambient_temp = 25.0
        self.rated_current = 28.5
        self.rated_pressure = 4.2
        self.thermal_tau = 35.0  # thermal time constant
        
        # Internal virtual twin state
        self.virtual_temperature = 58.0
        self.virtual_rpm = 1480.0
        self.virtual_current = 24.0
        self.virtual_pressure = 4.2
        self.virtual_vib_rms = 1.85
        self.virtual_acoustic = 67.5
        
        # Residual normalization parameters (mean and std dev from healthy baseline calibration)
        self.residual_baselines = {
            "vibration_rms": {"mean": 0.08, "std": 0.06, "weight": 0.25},
            "vibration_peak": {"mean": 0.15, "std": 0.10, "weight": 0.20},
            "temperature": {"mean": 0.45, "std": 0.35, "weight": 0.15},
            "rpm": {"mean": 1.20, "std": 0.90, "weight": 0.10},
            "current_amp": {"mean": 0.30, "std": 0.22, "weight": 0.10},
            "pressure_bar": {"mean": 0.05, "std": 0.04, "weight": 0.10},
            "acoustic_db": {"mean": 0.60, "std": 0.45, "weight": 0.10}
        }
        
        # Health Index smoothing
        self.health_index = 100.0
        self.cumulative_anomalies = 0
        self.max_health_z_score = 4.0

    def compute_virtual_state(self, sensor_reading: Dict[str, Any]) -> Dict[str, float]:
        """
        Calculates the expected physical baseline values based on the virtual twin's physics model.
        Uses estimated load and operational conditions.
        """
        rpm_meas = sensor_reading.get("rpm", self.rated_rpm)
        curr_meas = sensor_reading.get("current_amp", 24.0)
        
        # Estimate active load from slip and current
        slip = max(0.0, (1500.0 - rpm_meas) / 1500.0)
        load_est = max(0.4, min(1.0, (curr_meas - 10.0) / 18.5))
        
        # Virtual expected values
        expected_rpm = 1500.0 * (1.0 - slip)
        expected_current = 15.0 + load_est * 13.5
        
        # Expected steady-state temperature
        expected_temp = self.ambient_temp + 35.0 * (expected_current / self.rated_current) ** 1.8
        self.virtual_temperature += (expected_temp - self.virtual_temperature) * 0.05
        
        # Expected structural vibration
        expected_vib_rms = 1.5 + 0.6 * load_est
        expected_vib_peak = expected_vib_rms * 1.55
        
        # Expected hydraulic discharge pressure
        expected_pressure = 4.2 - 0.25 * (load_est - 0.75)
        
        # Expected acoustic signature
        expected_acoustic = 63.0 + 6.0 * load_est
        
        return {
            "vibration_rms": round(expected_vib_rms, 4),
            "vibration_peak": round(expected_vib_peak, 4),
            "temperature": round(self.virtual_temperature, 2),
            "rpm": round(expected_rpm, 1),
            "current_amp": round(expected_current, 2),
            "pressure_bar": round(expected_pressure, 3),
            "acoustic_db": round(expected_acoustic, 2)
        }

    def process_telemetry(self, sensor_reading: Dict[str, Any]) -> Dict[str, Any]:
        """
        Synchronizes digital twin with incoming physical sensor telemetry,
        computes residuals, normalized Z-scores, and the composite health index.
        """
        expected = self.compute_virtual_state(sensor_reading)
        
        residuals = {}
        z_scores = {}
        weighted_residual_sum = 0.0
        
        for feature, base_info in self.residual_baselines.items():
            actual_val = sensor_reading.get(feature, 0.0)
            exp_val = expected.get(feature, 0.0)
            raw_residual = abs(actual_val - exp_val)
            residuals[feature] = round(raw_residual, 4)
            
            # Z-score normalization against healthy baseline distribution
            z = max(0.0, (raw_residual - base_info["mean"]) / base_info["std"])
            z_scores[feature] = round(z, 2)
            weighted_residual_sum += min(z, self.max_health_z_score) * base_info["weight"]

        # Health Index calculation:
        # 100% when weighted residual Z-score is 0; declines exponentially as residual grows
        raw_health = 100.0 * math.exp(-0.25 * weighted_residual_sum)
        # Smoothing filter
        self.health_index = 0.85 * self.health_index + 0.15 * raw_health
        self.health_index = round(max(5.0, min(100.0, self.health_index)), 1)
        
        # Health status tier
        if self.health_index >= 85.0:
            health_status = "Optimal / Healthy"
            health_color = "#10b981"  # Emerald
        elif self.health_index >= 60.0:
            health_status = "Degradation Warning"
            health_color = "#f59e0b"  # Amber
        else:
            health_status = "Critical Fault Imminent"
            health_color = "#ef4444"  # Red

        return {
            "asset_id": self.asset_id,
            "timestamp": sensor_reading.get("timestamp"),
            "expected_state": expected,
            "sensor_actual": {
                k: sensor_reading.get(k) for k in self.residual_baselines.keys()
            },
            "residuals": residuals,
            "z_scores": z_scores,
            "composite_health_index": self.health_index,
            "health_status": health_status,
            "health_color": health_color,
            "sync_latency_ms": 1.2  # Virtual twin synchronization latency
        }
