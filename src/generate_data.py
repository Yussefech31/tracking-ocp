import os
import random
from datetime import datetime, timedelta

import psycopg2
from faker import Faker
from dotenv import load_dotenv

load_dotenv()

fake = Faker()

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5433"),
    "database": os.getenv("POSTGRES_DB", "ocp_transport"),
    "user": os.getenv("POSTGRES_USER", "ocp_admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "ocp_password"),
}

VEHICLE_COUNT = 50
DRIVER_COUNT = 50
ROUTE_COUNT = 30
TRIP_COUNT = 1000
GPS_EVENT_COUNT = 10000
FUEL_TRANSACTION_COUNT = 500
MAINTENANCE_COUNT = 200
INCIDENT_COUNT = 100

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# -------------------------
# VEHICLES
# -------------------------

vehicle_ids = []

manufacturers = [
    "Volvo",
    "Mercedes-Benz",
    "Scania",
    "MAN",
    "Renault Trucks",
]

vehicle_models = {
    "Volvo": ["FH", "FM"],
    "Mercedes-Benz": ["Actros", "Arocs"],
    "Scania": ["R-Series", "S-Series"],
    "MAN": ["TGX", "TGS"],
    "Renault Trucks": ["T", "C"],
}

vehicle_types = ["Truck", "Tanker", "Trailer"]
fuel_types = ["Diesel", "Electric", "Hybrid"]
vehicle_statuses = ["Active", "Maintenance", "Inactive"]

for i in range(1, VEHICLE_COUNT + 1):

    vehicle_id = f"V{i:04d}"
    vehicle_ids.append(vehicle_id)

    vehicle_type = random.choice(vehicle_types)
    manufacturer = random.choice(manufacturers)
    model = random.choice(vehicle_models[manufacturer])
    matriculation = f"{random.randint(1, 99999)}-{fake.random_letter().upper()}-{random.randint(1, 99)}"
    capacity = round(random.uniform(5, 40), 2)
    fuel_type = random.choice(fuel_types)
    year = random.randint(2015, 2025)
    status = random.choices(
        vehicle_statuses,
        weights=[80, 15, 5]
    )[0]

    cur.execute(
        """
        INSERT INTO transports.vehicle
        (
            vehicle_id,
            vehicle_type,
            manufacturer,
            model,
            matriculation,
            capacity_tons,
            fuel_type,
            year,
            status
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            vehicle_id,
            vehicle_type,
            manufacturer,
            model,
            matriculation,
            capacity,
            fuel_type,
            year,
            status,
        ),
    )


# -------------------------
# DRIVERS
# -------------------------

driver_ids = []

license_types = ["C", "CE", "C1", "D"]
departments = [
    "Logistics",
    "Transportation",
    "Operations",
    "Industrial",
]
driver_statuses = ["Active", "Inactive", "Suspended"]

for i in range(1, DRIVER_COUNT + 1):

    driver_id = f"D{i:04d}"
    driver_ids.append(driver_id)

    first_name = fake.first_name()
    last_name = fake.last_name()
    license_type = random.choice(license_types)
    experience = random.randint(1, 25)
    department = random.choice(departments)
    status = random.choices(
        driver_statuses,
        weights=[90, 7, 3]
    )[0]

    cur.execute(
        """
        INSERT INTO transports.driver
        (
            driver_id,
            first_name,
            last_name,
            license_type,
            experience_years,
            department,
            status
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            driver_id,
            first_name,
            last_name,
            license_type,
            experience,
            department,
            status,
        ),
    )


# -------------------------
# ROUTES
# -------------------------

route_ids = []

locations = [
    "Casablanca",
    "El Jadida",
    "Jorf Lasfar",
    "Safi",
    "Marrakech",
    "Rabat",
    "Kenitra",
    "Tangier",
    "Agadir",
    "Khouribga",
    "Benguerir",
    "Oujda",
]

route_types = ["Internal", "External", "Transfer"]

for i in range(1, ROUTE_COUNT + 1):

    route_id = f"R{i:04d}"
    route_ids.append(route_id)

    origin, destination = random.sample(locations, 2)

    distance = round(random.uniform(20, 600), 2)
    route_type = random.choice(route_types)

    cur.execute(
        """
        INSERT INTO transports.route
        (
            route_id,
            origin,
            destination,
            distance_km,
            route_type
        )
        VALUES (%s,%s,%s,%s,%s)
        """,
        (
            route_id,
            origin,
            destination,
            distance,
            route_type,
        ),
    )


# -------------------------
# TRIPS
# -------------------------

trip_ids = []

trip_statuses = [
    "Planned",
    "In Progress",
    "Completed",
    "Cancelled",
]

start_date = datetime.now() - timedelta(days=365)

for i in range(1, TRIP_COUNT + 1):

    trip_id = f"T{i:06d}"
    trip_ids.append(trip_id)

    vehicle_id = random.choice(vehicle_ids)
    driver_id = random.choice(driver_ids)
    route_id = random.choice(route_ids)

    departure_time = start_date + timedelta(
        minutes=random.randint(0, 525600)
    )

    duration_hours = random.uniform(0.5, 12)

    arrival_time = departure_time + timedelta(
        hours=duration_hours
    )

    planned_distance = random.uniform(20, 600)

    actual_distance = planned_distance * random.uniform(
        0.95,
        1.20
    )

    cargo_weight = random.uniform(1, 35)

    trip_status = random.choices(
        trip_statuses,
        weights=[10, 5, 80, 5]
    )[0]

    if trip_status == "Cancelled":
        arrival_time = None

    cur.execute(
        """
        INSERT INTO transports.trip
        (
            trip_id,
            vehicle_id,
            driver_id,
            route_id,
            departure_time,
            arrival_time,
            distance_km,
            cargo_weight_tons,
            trip_status
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            trip_id,
            vehicle_id,
            driver_id,
            route_id,
            departure_time,
            arrival_time,
            round(actual_distance, 2),
            round(cargo_weight, 2),
            trip_status,
        ),
    )


# -------------------------
# GPS EVENTS
# -------------------------

for i in range(1, GPS_EVENT_COUNT + 1):

    event_id = f"GPS{i:08d}"

    vehicle_id = random.choice(vehicle_ids)

    timestamp = start_date + timedelta(
        minutes=random.randint(0, 525600)
    )

    latitude = round(random.uniform(30.0, 35.9), 6)
    longitude = round(random.uniform(-10.5, -1.0), 6)

    speed = round(random.uniform(0, 110), 2)
    fuel_level = round(random.uniform(5, 100), 2)
    engine_temperature = round(random.uniform(70, 110), 2)

    cur.execute(
        """
        INSERT INTO transports.gps_event
        (
            event_id,
            vehicle_id,
            timestamp,
            latitude,
            longitude,
            speed_kmh,
            fuel_level,
            engine_temperature
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            event_id,
            vehicle_id,
            timestamp,
            latitude,
            longitude,
            speed,
            fuel_level,
            engine_temperature,
        ),
    )


# -------------------------
# FUEL TRANSACTIONS
# -------------------------

stations = [
    "Station Casablanca",
    "Station Jorf Lasfar",
    "Station Safi",
    "Station Marrakech",
    "Station Rabat",
    "Station Agadir",
]

for i in range(1, FUEL_TRANSACTION_COUNT + 1):

    transaction_id = f"FUEL{i:07d}"

    vehicle_id = random.choice(vehicle_ids)

    timestamp = start_date + timedelta(
        minutes=random.randint(0, 525600)
    )

    liters = round(random.uniform(20, 500), 2)
    price = round(random.uniform(10, 15), 3)

    station = random.choice(stations)

    cur.execute(
        """
        INSERT INTO transports.fuel_transaction
        (
            fuel_transaction_id,
            vehicle_id,
            timestamp,
            liters,
            price_per_liter,
            station
        )
        VALUES (%s,%s,%s,%s,%s,%s)
        """,
        (
            transaction_id,
            vehicle_id,
            timestamp,
            liters,
            price,
            station,
        ),
    )


# -------------------------
# MAINTENANCE
# -------------------------

maintenance_types = [
    "Oil Change",
    "Brake Inspection",
    "Engine Repair",
    "Tire Replacement",
    "Electrical Repair",
    "Preventive Maintenance",
]

for i in range(1, MAINTENANCE_COUNT + 1):

    maintenance_id = f"MNT{i:06d}"

    vehicle_id = random.choice(vehicle_ids)

    maintenance_date = start_date + timedelta(
        minutes=random.randint(0, 525600)
    )

    maintenance_type = random.choice(maintenance_types)

    description = fake.sentence()

    cost = round(random.uniform(100, 15000), 2)
    downtime = round(random.uniform(1, 72), 2)

    cur.execute(
        """
        INSERT INTO transports.maintenance
        (
            maintenance_id,
            vehicle_id,
            maintenance_date,
            maintenance_type,
            description,
            cost,
            downtime_hours
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            maintenance_id,
            vehicle_id,
            maintenance_date,
            maintenance_type,
            description,
            cost,
            downtime,
        ),
    )


# -------------------------
# INCIDENTS
# -------------------------

incident_types = [
    "Speeding",
    "Mechanical Failure",
    "Accident",
    "Route Deviation",
    "Fuel Issue",
    "Engine Overheating",
]

severities = ["Low", "Medium", "High", "Critical"]

for i in range(1, INCIDENT_COUNT + 1):

    incident_id = f"INC{i:06d}"

    vehicle_id = random.choice(vehicle_ids)

    timestamp = start_date + timedelta(
        minutes=random.randint(0, 525600)
    )

    incident_type = random.choice(incident_types)
    severity = random.choices(
        severities,
        weights=[50, 30, 15, 5]
    )[0]

    description = fake.sentence()

    cur.execute(
        """
        INSERT INTO transports.incident
        (
            incident_id,
            vehicle_id,
            timestamp,
            incident_type,
            severity,
            description
        )
        VALUES (%s,%s,%s,%s,%s,%s)
        """,
        (
            incident_id,
            vehicle_id,
            timestamp,
            incident_type,
            severity,
            description,
        ),
    )


conn.commit()

cur.close()
conn.close()

print("Synthetic data generated successfully.")
print(f"Vehicles: {VEHICLE_COUNT}")
print(f"Drivers: {DRIVER_COUNT}")
print(f"Routes: {ROUTE_COUNT}")
print(f"Trips: {TRIP_COUNT}")
print(f"GPS events: {GPS_EVENT_COUNT}")
print(f"Fuel transactions: {FUEL_TRANSACTION_COUNT}")
print(f"Maintenance records: {MAINTENANCE_COUNT}")
print(f"Incidents: {INCIDENT_COUNT}")