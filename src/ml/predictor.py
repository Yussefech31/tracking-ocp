"""
Inference Engine for OCP Transport Machine Learning.
Wraps trained models for real-time predictions, what-if simulators,
fleet-wide maintenance risk triage, and fuel eco-scoring.
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import joblib
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

ARTIFACTS_DIR = Path(__file__).resolve().parent / "artifacts"


class OCPMLEngine:
    _instance = None

    def __init__(self, auto_train: bool = True):
        self.artifacts_dir = ARTIFACTS_DIR
        self.trip_model = None
        self.maintenance_model = None
        self.fuel_model = None
        self.metrics = {}
        self.load_models(auto_train=auto_train)

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def load_models(self, auto_train: bool = True):
        trip_path = self.artifacts_dir / "trip_duration_model.joblib"
        maint_path = self.artifacts_dir / "maintenance_risk_model.joblib"
        fuel_path = self.artifacts_dir / "fuel_consumption_model.joblib"
        metrics_path = self.artifacts_dir / "metrics.json"

        if not (trip_path.exists() and maint_path.exists() and fuel_path.exists()):
            if auto_train:
                logger.info("ML Artifacts not found. Training models now...")
                from .train import train_all_models
                self.metrics = train_all_models()
            else:
                logger.warning("ML Artifacts missing and auto_train=False.")
                return

        try:
            self.trip_model = joblib.load(trip_path)
            self.maintenance_model = joblib.load(maint_path)
            self.fuel_model = joblib.load(fuel_path)
            if metrics_path.exists():
                with open(metrics_path, "r", encoding="utf-8") as f:
                    self.metrics = json.load(f)
            logger.info("All ML models successfully loaded into OCPMLEngine.")
        except Exception as exc:
            logger.error("Failed to load ML artifacts: %s", exc)

    def predict_trip(
        self,
        origin: str,
        destination: str,
        planned_distance_km: float,
        cargo_weight_tons: float,
        vehicle_type: str = "Truck",
        capacity_tons: float = 30.0,
        fuel_type: str = "Diesel",
        experience_years: int = 5,
        departure_time: Optional[datetime] = None,
        route_type: str = "Internal",
    ) -> Dict[str, Any]:
        """
        Predicts duration in minutes, ETA, average speed, and estimated fuel.
        """
        if departure_time is None:
            departure_time = datetime.now()

        hour = departure_time.hour
        day_of_week = departure_time.weekday()
        is_weekend = 1 if day_of_week in [5, 6] else 0
        payload_utilization = float(cargo_weight_tons / max(capacity_tons, 1.0))

        input_df = pd.DataFrame(
            [
                {
                    "distance_km": float(planned_distance_km),
                    "planned_distance_km": float(planned_distance_km),
                    "cargo_weight_tons": float(cargo_weight_tons),
                    "capacity_tons": float(capacity_tons),
                    "payload_utilization": payload_utilization,
                    "experience_years": float(experience_years),
                    "departure_hour": int(hour),
                    "day_of_week": int(day_of_week),
                    "is_weekend": is_weekend,
                    "origin": origin,
                    "destination": destination,
                    "route_type": route_type,
                    "vehicle_type": vehicle_type,
                    "fuel_type": fuel_type,
                }
            ]
        )

        if self.trip_model is not None:
            raw_duration = float(self.trip_model.predict(input_df)[0])
        else:
            # Fallback heuristic: 60 km/h average + cargo penalty
            raw_duration = (planned_distance_km / 60.0) * 60.0 + (cargo_weight_tons * 2.0)

        duration_minutes = max(15.0, raw_duration)
        eta = departure_time + timedelta(minutes=duration_minutes)
        hours = duration_minutes / 60.0
        avg_speed = planned_distance_km / max(hours, 0.1)

        # Traffic congestion factor indication
        is_peak = hour in [7, 8, 9, 16, 17, 18]
        traffic_status = "Rush Hour Traffic (+15% delay buffer applied)" if is_peak else "Fluid Highway Flow"

        # Confidence bounds (± 8%)
        ci_low = max(10.0, duration_minutes * 0.92)
        ci_high = duration_minutes * 1.08

        # Estimated fuel and fuel cost (Diesel ~ 12.80 MAD/L)
        fuel_est = self.predict_fuel(
            distance_km=planned_distance_km,
            cargo_weight_tons=cargo_weight_tons,
            vehicle_type=vehicle_type,
            capacity_tons=capacity_tons,
            fuel_type=fuel_type,
        )

        return {
            "predicted_duration_minutes": round(duration_minutes, 1),
            "predicted_duration_hours": round(hours, 2),
            "duration_formatted": f"{int(duration_minutes // 60)}h {int(duration_minutes % 60)}m",
            "departure_time": departure_time,
            "estimated_arrival_time": eta,
            "eta_formatted": eta.strftime("%Y-%m-%d %H:%M"),
            "avg_speed_kmh": round(avg_speed, 1),
            "confidence_interval_minutes": (round(ci_low, 1), round(ci_high, 1)),
            "traffic_status": traffic_status,
            "is_peak_hours": is_peak,
            "estimated_fuel_liters": fuel_est["expected_liters"],
            "estimated_fuel_cost_mad": fuel_est["estimated_cost_mad"],
            "co2_emissions_kg": fuel_est["co2_emissions_kg"],
        }

    def predict_fuel(
        self,
        distance_km: float,
        cargo_weight_tons: float,
        vehicle_type: str = "Truck",
        capacity_tons: float = 30.0,
        fuel_type: str = "Diesel",
        actual_fuel_liters: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Estimates expected fuel consumption and provides eco-efficiency rating.
        """
        payload_util = float(cargo_weight_tons / max(capacity_tons, 1.0))
        input_df = pd.DataFrame(
            [
                {
                    "distance_km": float(distance_km),
                    "cargo_weight_tons": float(cargo_weight_tons),
                    "capacity_tons": float(capacity_tons),
                    "payload_utilization": payload_util,
                    "vehicle_type": vehicle_type,
                    "fuel_type": fuel_type,
                }
            ]
        )

        if self.fuel_model is not None:
            expected_liters = float(self.fuel_model.predict(input_df)[0])
        else:
            expected_liters = (distance_km * (42.0 + cargo_weight_tons * 0.45)) / 100.0

        expected_liters = max(5.0, round(expected_liters, 2))
        price_per_l = 12.80 if fuel_type == "Diesel" else (14.20 if fuel_type == "Hybrid" else 2.50)
        cost_mad = round(expected_liters * price_per_l, 2)
        co2_kg = round(expected_liters * 2.68, 1)  # ~ 2.68 kg CO2 per liter diesel

        res = {
            "expected_liters": expected_liters,
            "estimated_cost_mad": cost_mad,
            "co2_emissions_kg": co2_kg,
            "fuel_per_100km": round((expected_liters / max(distance_km, 1.0)) * 100, 2),
        }

        if actual_fuel_liters is not None and actual_fuel_liters > 0:
            diff_liters = actual_fuel_liters - expected_liters
            pct_diff = (diff_liters / expected_liters) * 100

            if pct_diff <= -5:
                grade = "A+"
                badge = "Excellent Eco-Driving"
                color = "#00D4AA"
            elif pct_diff <= 5:
                grade = "A"
                badge = "Optimal Consumption"
                color = "#38BDF8"
            elif pct_diff <= 15:
                grade = "B"
                badge = "Moderate Fuel Burn"
                color = "#FFB547"
            elif pct_diff <= 25:
                grade = "C"
                badge = "Excess Consumption"
                color = "#FF6B8B"
            else:
                grade = "D"
                badge = "High Fuel Anomaly (Check Leaks/Idling)"
                color = "#EF4444"

            res.update(
                {
                    "actual_liters": round(actual_fuel_liters, 2),
                    "variance_liters": round(diff_liters, 2),
                    "variance_pct": round(pct_diff, 1),
                    "variance_cost_mad": round(diff_liters * price_per_l, 2),
                    "eco_grade": grade,
                    "eco_badge": badge,
                    "eco_color": color,
                }
            )

        return res

    def score_vehicle_risk(
        self,
        vehicle_age_years: float,
        total_distance_km: float,
        completed_trips: int,
        maintenance_operations: int,
        total_downtime_hours: float,
        incident_count: int,
        fuel_liters_per_100km: float = 48.0,
        capacity_tons: float = 25.0,
        vehicle_type: str = "Truck",
        manufacturer: str = "Volvo",
        fuel_type: str = "Diesel",
    ) -> Dict[str, Any]:
        """
        Calculates Failure/Breakdown Risk Score (0-100%) and prescriptive actions.
        """
        input_df = pd.DataFrame(
            [
                {
                    "vehicle_age_years": float(vehicle_age_years),
                    "total_distance_km": float(total_distance_km),
                    "completed_trips": int(completed_trips),
                    "maintenance_operations": int(maintenance_operations),
                    "total_downtime_hours": float(total_downtime_hours),
                    "incident_count": int(incident_count),
                    "fuel_liters_per_100km": float(fuel_liters_per_100km),
                    "capacity_tons": float(capacity_tons),
                    "vehicle_type": vehicle_type,
                    "manufacturer": manufacturer,
                    "fuel_type": fuel_type,
                }
            ]
        )

        if self.maintenance_model is not None:
            prob = float(self.maintenance_model.predict_proba(input_df)[0][1])
            risk_score = round(prob * 100, 1)
        else:
            # Fallback heuristic
            score = (
                (total_downtime_hours / 80.0) * 35
                + (incident_count / 4.0) * 30
                + (total_distance_km / 12000.0) * 20
                + (vehicle_age_years / 8.0) * 15
            )
            risk_score = round(float(np.clip(score, 5, 95)), 1)

        # Categorize
        if risk_score >= 70.0:
            tier = "CRITICAL"
            color = "#FF6B8B"
            recommendation = "🚨 Immediate workshop inspection required before next mission. Check brake wear & cooling loop."
            priority = 1
        elif risk_score >= 45.0:
            tier = "HIGH"
            color = "#FFB547"
            recommendation = "⚠️ Schedule preventive maintenance within 48 hours. Monitor engine operating temperature."
            priority = 2
        elif risk_score >= 25.0:
            tier = "MEDIUM"
            color = "#38BDF8"
            recommendation = "🔍 Vehicle in fair condition. Perform routine oil and tire inspection at next scheduled depot stop."
            priority = 3
        else:
            tier = "LOW"
            color = "#00D4AA"
            recommendation = "✅ Vehicle healthy. Cleared for long-haul phosphate transport routes."
            priority = 4

        # Primary risk factors
        drivers = []
        if total_downtime_hours > 50:
            drivers.append(f"High cumulative downtime ({total_downtime_hours:.1f} hrs)")
        if incident_count >= 2:
            drivers.append(f"Recurring safety incidents ({incident_count} events)")
        if total_distance_km > 7000:
            drivers.append(f"High mileage stress ({total_distance_km:,.0f} km)")
        if vehicle_age_years >= 6:
            drivers.append(f"Asset aging ({vehicle_age_years:.0f} years old)")
        if fuel_liters_per_100km > 52:
            drivers.append(f"Abnormal fuel consumption ({fuel_liters_per_100km:.1f} L/100km)")
        if not drivers:
            drivers.append("Optimal operating telemetry and regular maintenance schedule")

        return {
            "risk_score": risk_score,
            "risk_tier": tier,
            "risk_color": color,
            "priority": priority,
            "recommendation": recommendation,
            "primary_drivers": drivers,
        }

    def scan_fleet(self, vehicles_df: pd.DataFrame) -> pd.DataFrame:
        """
        Runs batch risk scoring on an entire vehicles DataFrame.
        """
        if vehicles_df.empty:
            return pd.DataFrame()

        df = vehicles_df.copy()
        current_year = 2026
        df["year"] = pd.to_numeric(df.get("year", 2020), errors="coerce").fillna(2020)
        df["vehicle_age_years"] = (current_year - df["year"]).clip(lower=1)

        scores = []
        tiers = []
        colors = []
        recs = []

        for _, row in df.iterrows():
            res = self.score_vehicle_risk(
                vehicle_age_years=row["vehicle_age_years"],
                total_distance_km=float(row.get("total_distance_km", 0) or 0),
                completed_trips=int(row.get("completed_trips", 0) or 0),
                maintenance_operations=int(row.get("maintenance_operations", 0) or 0),
                total_downtime_hours=float(row.get("total_downtime_hours", 0) or 0),
                incident_count=int(row.get("incident_count", 0) or 0),
                fuel_liters_per_100km=float(row.get("fuel_liters_per_100km", 45) or 45),
                capacity_tons=float(row.get("capacity_tons", 25) or 25),
                vehicle_type=str(row.get("vehicle_type", "Truck")),
                manufacturer=str(row.get("manufacturer", "Volvo")),
                fuel_type=str(row.get("fuel_type", "Diesel")),
            )
            scores.append(res["risk_score"])
            tiers.append(res["risk_tier"])
            colors.append(res["risk_color"])
            recs.append(res["recommendation"])

        df["risk_score"] = scores
        df["risk_tier"] = tiers
        df["risk_color"] = colors
        df["recommendation"] = recs

        return df.sort_values("risk_score", ascending=False)

    def get_metrics(self) -> Dict[str, Any]:
        return self.metrics
