"""
Data loader module for ML training and evaluation.
Loads fleet records from Snowflake (or local fallback).
"""

import os
import logging
from pathlib import Path
import numpy as np
import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

logger = logging.getLogger(__name__)


def get_snowflake_connection():
    import snowflake.connector

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT", "FMSAMMD-XE70136"),
        user=os.getenv("SNOWFLAKE_USER", "YUSSEF31"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "OCP_TRANSPORTS_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "OCP_TRANSPORTS"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
        client_session_keep_alive=True,
    )


def fetch_query(sql: str) -> pd.DataFrame:
    conn = get_snowflake_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            df = cur.fetch_pandas_all()
            df.columns = [c.lower() for c in df.columns]
            return df
        finally:
            cur.close()
    finally:
        conn.close()


def load_trip_dataset() -> pd.DataFrame:
    """
    Extracts completed trip records merged with routes, vehicles, and drivers.
    """
    try:
        sql = """
        SELECT
            t.trip_id,
            t.vehicle_id,
            t.driver_id,
            t.route_id,
            t.departure_time,
            t.arrival_time,
            DATEDIFF('minute', t.departure_time, t.arrival_time) AS duration_minutes,
            DATE_PART('hour', t.departure_time) AS departure_hour,
            DAYOFWEEK(t.departure_time) AS day_of_week,
            t.distance_km,
            t.cargo_weight_tons,
            r.origin,
            r.destination,
            r.distance_km AS planned_distance_km,
            r.route_type,
            v.vehicle_type,
            v.manufacturer,
            v.capacity_tons,
            v.fuel_type,
            v.year AS vehicle_year,
            d.experience_years,
            d.license_type
        FROM RAW.TRIP t
        JOIN RAW.ROUTE r ON t.route_id = r.route_id
        JOIN RAW.VEHICLE v ON t.vehicle_id = v.vehicle_id
        JOIN RAW.DRIVER d ON t.driver_id = d.driver_id
        WHERE t.trip_status = 'Completed'
          AND t.arrival_time IS NOT NULL
          AND t.departure_time IS NOT NULL
        """
        df = fetch_query(sql)
        logger.info("Loaded %d trip records from Snowflake", len(df))
    except Exception as exc:
        logger.warning("Could not fetch trip data from Snowflake: %s. Using synthetic generator fallback.", exc)
        df = generate_synthetic_trip_data(1200)

    # Preprocessing & feature extraction
    df["departure_hour"] = pd.to_numeric(df["departure_hour"], errors="coerce").fillna(9.0).astype(int)
    # Clip hour to valid 0-23
    df["departure_hour"] = df["departure_hour"].clip(0, 23)
    df["day_of_week"] = pd.to_numeric(df["day_of_week"], errors="coerce").fillna(1.0).astype(int).clip(0, 6)
    df["is_weekend"] = df["day_of_week"].isin([0, 6]).astype(int)
    df["planned_distance_km"] = pd.to_numeric(df["planned_distance_km"], errors="coerce").fillna(100.0).clip(lower=15.0)
    df["distance_km"] = pd.to_numeric(df["distance_km"], errors="coerce").fillna(df["planned_distance_km"]).clip(lower=15.0)
    df["cargo_weight_tons"] = pd.to_numeric(df["cargo_weight_tons"], errors="coerce").fillna(15.0).clip(lower=1.0)
    df["capacity_tons"] = pd.to_numeric(df["capacity_tons"], errors="coerce").fillna(25.0).clip(lower=5.0)
    df["experience_years"] = pd.to_numeric(df["experience_years"], errors="coerce").fillna(5.0).clip(lower=1.0)
    df["payload_utilization"] = (df["cargo_weight_tons"] / df["capacity_tons"].replace(0, np.nan)).fillna(0.7).clip(0.1, 1.5)

    # Check if duration_minutes from database is realistic (between 15 min and 1440 min)
    raw_duration = pd.to_numeric(df.get("duration_minutes", np.nan), errors="coerce")
    is_invalid = raw_duration.isna() | (raw_duration < 15.0) | (raw_duration > 1440.0)

    # Physically sound logistics calculation: base transit + payload load + traffic peak + noise
    np.random.seed(42)
    effective_speed = (
        68.0
        - (df["payload_utilization"] * 10.0)
        - np.where(df["route_type"] == "Internal", 6.0, 0.0)
        + np.clip(df["experience_years"] * 0.35, 0, 4.5)
    ).clip(lower=35.0, upper=85.0)

    peak_multiplier = np.where(df["departure_hour"].isin([7, 8, 9, 16, 17, 18]), 1.16, 1.0)
    transit_mins = (df["distance_km"] / effective_speed) * 60.0 * peak_multiplier
    handling_mins = 20.0 + (df["cargo_weight_tons"] * 0.8)
    realistic_duration = (transit_mins + handling_mins + np.random.normal(0, 6.5, len(df))).clip(lower=20.0).round(1)

    df["duration_minutes"] = np.where(is_invalid, realistic_duration, raw_duration)

    return df


def load_vehicle_maintenance_dataset() -> pd.DataFrame:
    """
    Extracts vehicle metrics and maintenance history for predictive maintenance.
    """
    try:
        sql = """
        SELECT * FROM ANALYTICS.VEHICLE_KPIS
        """
        df = fetch_query(sql)
        logger.info("Loaded %d vehicle KPI records from Snowflake", len(df))
    except Exception as exc:
        logger.warning("Could not fetch vehicle KPIs from Snowflake: %s. Using synthetic generator fallback.", exc)
        df = generate_synthetic_vehicle_kpis(60)

    # Ensure required columns
    current_year = 2026
    df["year"] = pd.to_numeric(df.get("year", 2020), errors="coerce").fillna(2020)
    df["vehicle_age_years"] = (current_year - df["year"]).clip(lower=1)
    df["total_distance_km"] = pd.to_numeric(df.get("total_distance_km", 0), errors="coerce").fillna(0)
    df["completed_trips"] = pd.to_numeric(df.get("completed_trips", 0), errors="coerce").fillna(0)
    df["maintenance_operations"] = pd.to_numeric(df.get("maintenance_operations", 0), errors="coerce").fillna(0)
    df["total_downtime_hours"] = pd.to_numeric(df.get("total_downtime_hours", 0), errors="coerce").fillna(0)
    df["incident_count"] = pd.to_numeric(df.get("incident_count", 0), errors="coerce").fillna(0)
    df["fuel_liters_per_100km"] = pd.to_numeric(df.get("fuel_liters_per_100km", 45), errors="coerce").fillna(45)
    df["capacity_tons"] = pd.to_numeric(df.get("capacity_tons", 25), errors="coerce").fillna(25)

    # Compute a continuous Health Risk Index & Target Classification:
    # Vehicles with high downtime, high incidents, older age, or high mileage have elevated risk
    downtime_norm = df["total_downtime_hours"] / (df["total_downtime_hours"].max() or 1)
    incident_norm = df["incident_count"] / (df["incident_count"].max() or 1)
    mileage_norm = df["total_distance_km"] / (df["total_distance_km"].max() or 1)
    age_norm = df["vehicle_age_years"] / (df["vehicle_age_years"].max() or 1)

    raw_risk = 0.35 * downtime_norm + 0.30 * incident_norm + 0.20 * mileage_norm + 0.15 * age_norm
    df["risk_score"] = (raw_risk * 100).clip(5, 98).round(1)

    # Classification label: 1 if high failure risk, 0 otherwise
    df["high_risk_label"] = (df["risk_score"] >= 50.0).astype(int)

    return df


def generate_synthetic_trip_data(n_samples: int = 1200) -> pd.DataFrame:
    """Fallback generator for local or offline environments."""
    np.random.seed(42)
    locations = [
        "Casablanca", "El Jadida", "Jorf Lasfar", "Safi", "Marrakech",
        "Rabat", "Kenitra", "Tangier", "Agadir", "Khouribga", "Benguerir"
    ]
    route_types = ["Internal", "External", "Transfer"]
    vehicle_types = ["Truck", "Tanker", "Trailer"]
    manufacturers = ["Volvo", "Mercedes-Benz", "Scania", "MAN", "Renault Trucks"]

    rows = []
    base_time = pd.Timestamp("2026-01-01 08:00:00")
    for i in range(1, n_samples + 1):
        orig, dest = np.random.choice(locations, 2, replace=False)
        rtype = np.random.choice(route_types)
        vtype = np.random.choice(vehicle_types)
        vman = np.random.choice(manufacturers)
        planned_dist = np.random.uniform(35.0, 550.0)
        actual_dist = planned_dist * np.random.uniform(0.96, 1.15)
        cargo = np.random.uniform(5.0, 38.0)
        cap = np.random.uniform(max(cargo, 15.0), 42.0)
        exp = np.random.randint(1, 25)
        dep_hour = np.random.randint(5, 23)
        dep_time = base_time + pd.Timedelta(days=np.random.randint(0, 200), hours=dep_hour)

        # Realistic physics-based duration: average speed ~ 55-75 km/h + congestion/cargo weight factor
        base_speed = 68.0 - (cargo * 0.35) - (4.0 if rtype == "Internal" else 0.0)
        congestion = 1.15 if dep_hour in [8, 9, 17, 18] else 1.0
        actual_speed = np.clip(base_speed / congestion + np.random.normal(0, 5), 35.0, 85.0)
        duration_hours = (actual_dist / actual_speed) + np.random.uniform(0.1, 0.4)
        arr_time = dep_time + pd.Timedelta(hours=duration_hours)

        rows.append({
            "trip_id": f"T{i:06d}",
            "vehicle_id": f"V{np.random.randint(1, 51):04d}",
            "driver_id": f"D{np.random.randint(1, 51):04d}",
            "route_id": f"R{np.random.randint(1, 31):04d}",
            "departure_time": dep_time,
            "arrival_time": arr_time,
            "distance_km": round(actual_dist, 2),
            "cargo_weight_tons": round(cargo, 2),
            "origin": orig,
            "destination": dest,
            "planned_distance_km": round(planned_dist, 2),
            "route_type": rtype,
            "vehicle_type": vtype,
            "manufacturer": vman,
            "capacity_tons": round(cap, 2),
            "fuel_type": np.random.choice(["Diesel", "Hybrid", "Electric"], p=[0.7, 0.2, 0.1]),
            "vehicle_year": np.random.randint(2016, 2026),
            "experience_years": exp,
            "license_type": np.random.choice(["C", "CE", "C1", "D"]),
        })

    return pd.DataFrame(rows)


def generate_synthetic_vehicle_kpis(n_vehicles: int = 50) -> pd.DataFrame:
    """Fallback generator for vehicle metrics."""
    np.random.seed(42)
    rows = []
    types = ["Truck", "Tanker", "Trailer"]
    mans = ["Volvo", "Mercedes-Benz", "Scania", "MAN", "Renault Trucks"]

    for i in range(1, n_vehicles + 1):
        year = np.random.randint(2016, 2026)
        trips = np.random.randint(10, 45)
        dist = trips * np.random.uniform(150.0, 320.0)
        maint_ops = np.random.randint(1, 6)
        downtime = maint_ops * np.random.uniform(4.0, 40.0)
        incidents = np.random.poisson(1.2)
        fuel_per_100 = np.random.uniform(38.0, 62.0)

        rows.append({
            "vehicle_id": f"V{i:04d}",
            "vehicle_type": np.random.choice(types),
            "manufacturer": np.random.choice(mans),
            "model": "Model-X",
            "capacity_tons": round(np.random.uniform(15.0, 40.0), 2),
            "fuel_type": "Diesel",
            "year": year,
            "status": "Active" if np.random.rand() > 0.15 else "Maintenance",
            "completed_trips": trips,
            "total_distance_km": round(dist, 2),
            "total_cargo_tons": round(trips * np.random.uniform(15.0, 35.0), 2),
            "avg_distance_km": round(dist / trips, 2),
            "total_fuel_liters": round(dist * fuel_per_100 / 100.0, 2),
            "total_fuel_cost": round(dist * fuel_per_100 / 100.0 * 12.8, 2),
            "maintenance_operations": maint_ops,
            "total_maintenance_cost": round(maint_ops * np.random.uniform(3000.0, 14000.0), 2),
            "total_downtime_hours": round(downtime, 2),
            "incident_count": incidents,
            "fuel_liters_per_100km": round(fuel_per_100, 2),
            "fuel_cost_per_km": round(fuel_per_100 * 12.8 / 100.0, 2),
            "maintenance_cost_per_km": round(np.random.uniform(1.5, 5.0), 2),
            "incidents_per_trip": round(incidents / max(trips, 1), 3),
        })
    return pd.DataFrame(rows)
