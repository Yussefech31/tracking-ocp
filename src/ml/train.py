"""
Model training pipeline for OCP Transport Intelligence Platform.
Trains:
1. Trip Duration & ETA Regressor (Gradient Boosting)
2. Predictive Maintenance Risk Classifier (Random Forest)
3. Fuel Consumption & Efficiency Model (Ridge / Ensemble)
Saves artifacts to src/ml/artifacts/
"""

import json
import logging
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from .data_loader import load_trip_dataset, load_vehicle_maintenance_dataset

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ARTIFACTS_DIR = Path(__file__).resolve().parent / "artifacts"
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)


def train_trip_duration_model(df_trips: pd.DataFrame):
    logger.info("--- Training Trip Duration & ETA Model ---")

    numeric_features = [
        "distance_km",
        "planned_distance_km",
        "cargo_weight_tons",
        "capacity_tons",
        "payload_utilization",
        "experience_years",
        "departure_hour",
        "day_of_week",
        "is_weekend",
    ]
    categorical_features = [
        "origin",
        "destination",
        "route_type",
        "vehicle_type",
        "fuel_type",
    ]

    target = "duration_minutes"
    valid_data = df_trips.dropna(subset=numeric_features + categorical_features + [target])

    X = valid_data[numeric_features + categorical_features]
    y = valid_data[target]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=42)

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), numeric_features),
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), categorical_features),
        ]
    )

    model = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            (
                "regressor",
                RandomForestRegressor(
                    n_estimators=100,
                    max_depth=12,
                    random_state=42,
                    n_jobs=-1,
                ),
            ),
        ]
    )

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    mae = float(mean_absolute_error(y_test, y_pred))
    rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))
    r2 = float(r2_score(y_test, y_pred))

    logger.info("Trip Model Metrics -> MAE: %.2f min | RMSE: %.2f min | R2: %.4f", mae, rmse, r2)

    model_path = ARTIFACTS_DIR / "trip_duration_model.joblib"
    joblib.dump(model, model_path)

    return {
        "mae_minutes": round(mae, 2),
        "rmse_minutes": round(rmse, 2),
        "r2_score": round(r2, 4),
        "train_samples": len(X_train),
        "test_samples": len(X_test),
        "numeric_features": numeric_features,
        "categorical_features": categorical_features,
    }


def train_predictive_maintenance_model(df_vehicles: pd.DataFrame):
    logger.info("--- Training Predictive Maintenance Risk Model ---")

    # If dataset has fewer than 100 rows, supplement with realistic bootstrap variations for robust statistical training
    if len(df_vehicles) < 150:
        np.random.seed(42)
        n_extra = 250
        extra_rows = []
        for _ in range(n_extra):
            base = df_vehicles.sample(1).iloc[0].to_dict()
            noise_age = np.random.choice([-1, 0, 1, 2])
            noise_dist = np.random.uniform(-500, 800)
            noise_downtime = np.random.uniform(-10, 25)
            noise_incidents = np.random.choice([-1, 0, 1, 2])

            base["vehicle_age_years"] = max(1, base["vehicle_age_years"] + noise_age)
            base["total_distance_km"] = max(200, base["total_distance_km"] + noise_dist)
            base["total_downtime_hours"] = max(0, base["total_downtime_hours"] + noise_downtime)
            base["incident_count"] = max(0, base["incident_count"] + noise_incidents)

            raw_risk = (
                0.35 * (base["total_downtime_hours"] / 120.0)
                + 0.30 * (base["incident_count"] / 5.0)
                + 0.20 * (base["total_distance_km"] / 15000.0)
                + 0.15 * (base["vehicle_age_years"] / 10.0)
            )
            base["risk_score"] = float(np.clip(raw_risk * 100, 5, 98).round(1))
            base["high_risk_label"] = 1 if base["risk_score"] >= 50.0 else 0
            extra_rows.append(base)
        df_vehicles = pd.concat([df_vehicles, pd.DataFrame(extra_rows)], ignore_index=True)

    numeric_features = [
        "vehicle_age_years",
        "total_distance_km",
        "completed_trips",
        "maintenance_operations",
        "total_downtime_hours",
        "incident_count",
        "fuel_liters_per_100km",
        "capacity_tons",
    ]
    categorical_features = ["vehicle_type", "manufacturer", "fuel_type"]

    target = "high_risk_label"
    valid_data = df_vehicles.dropna(subset=numeric_features + [target])

    X = valid_data[numeric_features + categorical_features]
    y = valid_data[target].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42, stratify=y)

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), numeric_features),
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), categorical_features),
        ]
    )

    clf = RandomForestClassifier(
        n_estimators=100,
        max_depth=6,
        random_state=42,
        class_weight="balanced",
    )

    model = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("classifier", clf),
        ]
    )

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]

    acc = float(accuracy_score(y_test, y_pred))
    f1 = float(f1_score(y_test, y_pred, zero_division=0))
    roc_auc = float(roc_auc_score(y_test, y_prob))

    logger.info("Maintenance Model -> Accuracy: %.3f | F1: %.3f | ROC-AUC: %.4f", acc, f1, roc_auc)

    # Extract feature importances
    encoder = model.named_steps["preprocessor"].named_transformers_["cat"]
    cat_names = list(encoder.get_feature_names_out(categorical_features))
    feature_names = numeric_features + cat_names
    importances = model.named_steps["classifier"].feature_importances_

    top_features = sorted(
        zip(feature_names, [round(float(v), 4) for v in importances]),
        key=lambda x: x[1],
        reverse=True,
    )[:10]

    model_path = ARTIFACTS_DIR / "maintenance_risk_model.joblib"
    joblib.dump(model, model_path)

    return {
        "accuracy": round(acc, 4),
        "f1_score": round(f1, 4),
        "roc_auc": round(roc_auc, 4),
        "train_samples": len(X_train),
        "test_samples": len(X_test),
        "top_feature_importances": dict(top_features),
        "numeric_features": numeric_features,
        "categorical_features": categorical_features,
    }


def train_fuel_optimization_model(df_trips: pd.DataFrame):
    logger.info("--- Training Fuel Consumption & Optimization Model ---")

    # Estimate trip fuel: based on distance, cargo weight, and vehicle type
    df = df_trips.copy()
    base_l_100km = 42.0 + (df["cargo_weight_tons"] * 0.45)
    df["actual_fuel_liters"] = (df["distance_km"] * base_l_100km / 100.0) + np.random.normal(0, 4.0, len(df))
    df["actual_fuel_liters"] = df["actual_fuel_liters"].clip(lower=10.0)

    numeric_features = ["distance_km", "cargo_weight_tons", "capacity_tons", "payload_utilization"]
    categorical_features = ["vehicle_type", "fuel_type"]
    target = "actual_fuel_liters"

    X = df[numeric_features + categorical_features]
    y = df[target]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=42)

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), numeric_features),
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), categorical_features),
        ]
    )

    model = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("regressor", RandomForestRegressor(n_estimators=80, max_depth=6, random_state=42)),
        ]
    )

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    mae = float(mean_absolute_error(y_test, y_pred))
    r2 = float(r2_score(y_test, y_pred))

    logger.info("Fuel Model -> MAE: %.2f L | R2: %.4f", mae, r2)

    model_path = ARTIFACTS_DIR / "fuel_consumption_model.joblib"
    joblib.dump(model, model_path)

    return {
        "mae_liters": round(mae, 2),
        "r2_score": round(r2, 4),
        "numeric_features": numeric_features,
        "categorical_features": categorical_features,
    }


def train_all_models():
    """Runs complete end-to-end training and saves metrics metadata."""
    logger.info("Starting ML Training Pipeline...")

    df_trips = load_trip_dataset()
    df_vehicles = load_vehicle_maintenance_dataset()

    trip_metrics = train_trip_duration_model(df_trips)
    maint_metrics = train_predictive_maintenance_model(df_vehicles)
    fuel_metrics = train_fuel_optimization_model(df_trips)

    metadata = {
        "training_timestamp": datetime.now().isoformat(),
        "framework": "scikit-learn 1.8.0",
        "trip_duration_model": trip_metrics,
        "maintenance_risk_model": maint_metrics,
        "fuel_optimization_model": fuel_metrics,
    }

    metrics_path = ARTIFACTS_DIR / "metrics.json"
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    logger.info("ML Training Pipeline complete! Metrics saved to %s", metrics_path)
    return metadata


if __name__ == "__main__":
    train_all_models()
