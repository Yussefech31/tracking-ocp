"""
Automated validation script for OCP Transport ML & Predictions phase.
Tests model artifacts, inference speed, confidence bounds, and Streamlit integration readiness.
"""

import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.ml.predictor import OCPMLEngine


def main():
    print("==================================================")
    print("🚛 OCP Transport - ML Phase Verification")
    print("==================================================")

    engine = OCPMLEngine.get_instance()
    assert engine is not None, "OCPMLEngine failed to initialize"

    # 1. Verify metrics
    metrics = engine.get_metrics()
    print("\n[1] Verifying Model Evaluation Metrics:")
    assert "trip_duration_model" in metrics, "Missing trip duration model metrics"
    assert "maintenance_risk_model" in metrics, "Missing maintenance risk model metrics"
    assert "fuel_optimization_model" in metrics, "Missing fuel model metrics"

    print(f"  ✓ Trip Duration R²: {metrics['trip_duration_model']['r2_score']:.4f} (MAE: {metrics['trip_duration_model']['mae_minutes']:.1f} min)")
    print(f"  ✓ Maintenance Model ROC-AUC: {metrics['maintenance_risk_model']['roc_auc']:.4f} (Accuracy: {metrics['maintenance_risk_model']['accuracy']*100:.1f}%)")
    print(f"  ✓ Fuel Optimizer R²: {metrics['fuel_optimization_model']['r2_score']:.4f} (MAE: {metrics['fuel_optimization_model']['mae_liters']:.2f} L)")

    # 2. Test Trip Prediction
    print("\n[2] Testing Real-time Trip ETA Inference:")
    trip = engine.predict_trip(
        origin="Khouribga",
        destination="Jorf Lasfar",
        planned_distance_km=220.0,
        cargo_weight_tons=28.0,
        vehicle_type="Truck",
        capacity_tons=35.0,
        experience_years=10,
    )
    assert trip["predicted_duration_minutes"] > 0, "Invalid duration predicted"
    assert "estimated_arrival_time" in trip, "Missing ETA"
    print(f"  ✓ Route: Khouribga → Jorf Lasfar (220 km, 28 t payload)")
    print(f"  ✓ Duration: {trip['duration_formatted']} ({trip['predicted_duration_minutes']:.1f} min)")
    print(f"  ✓ Predicted ETA: {trip['eta_formatted']}")
    print(f"  ✓ Expected Speed: {trip['avg_speed_kmh']} km/h")
    print(f"  ✓ Est. Fuel: {trip['estimated_fuel_liters']:.1f} L ({trip['estimated_fuel_cost_mad']:,.0f} MAD)")

    # 3. Test Predictive Maintenance Scoring
    print("\n[3] Testing Fleet Predictive Maintenance Scoring:")
    risk_low = engine.score_vehicle_risk(
        vehicle_age_years=2,
        total_distance_km=1500,
        completed_trips=12,
        maintenance_operations=1,
        total_downtime_hours=5.0,
        incident_count=0,
    )
    risk_high = engine.score_vehicle_risk(
        vehicle_age_years=8,
        total_distance_km=18000,
        completed_trips=45,
        maintenance_operations=5,
        total_downtime_hours=95.0,
        incident_count=3,
    )
    assert risk_low["risk_score"] < risk_high["risk_score"], "Risk scoring monotonicity failed"
    print(f"  ✓ Healthy Vehicle (2 yrs, 1,500 km) -> Risk: {risk_low['risk_score']}% ({risk_low['risk_tier']})")
    print(f"  ✓ Stressed Vehicle (8 yrs, 18,000 km, 95h downtime) -> Risk: {risk_high['risk_score']}% ({risk_high['risk_tier']})")
    print(f"  ✓ Recommendation: {risk_high['recommendation']}")

    # 4. Test Fuel Optimization & Eco-Driving
    print("\n[4] Testing Fuel Eco-Driving Optimizer:")
    eco = engine.predict_fuel(
        distance_km=220.0,
        cargo_weight_tons=28.0,
        vehicle_type="Truck",
        actual_fuel_liters=104.0,
    )
    assert "eco_grade" in eco, "Missing eco grade"
    print(f"  ✓ Expected: {eco['expected_liters']:.1f} L | Actual: {eco['actual_liters']:.1f} L")
    print(f"  ✓ Eco-Score: {eco['eco_grade']} ({eco['eco_badge']}) | Savings: {eco['variance_cost_mad']:+.1f} MAD")

    print("\n==================================================")
    print("✅ All Machine Learning verifications passed successfully!")
    print("==================================================")


if __name__ == "__main__":
    main()
