CREATE SCHEMA IF NOT EXISTS transports;

CREATE TABLE IF NOT EXISTS transports.vehicle (
    vehicle_id VARCHAR(10) PRIMARY KEY,
    vehicle_type VARCHAR(30) NOT NULL,
    manufacturer VARCHAR(50) NOT NULL,
    model VARCHAR(50) NOT NULL,
    matriculation VARCHAR(50) NOT NULL UNIQUE,
    capacity_tons DECIMAL(6,2) NOT NULL,
    fuel_type VARCHAR(20) NOT NULL,
    year INTEGER NOT NULL,
    status VARCHAR(20) NOT NULL,

    CONSTRAINT chk_vehicle_capacity
        CHECK (capacity_tons > 0),

    CONSTRAINT chk_vehicle_year
        CHECK (year >= 2000),

    CONSTRAINT chk_vehicle_fuel_type
        CHECK (fuel_type IN ('Diesel', 'Electric', 'Hybrid')),

    CONSTRAINT chk_vehicle_status
        CHECK (status IN ('Active', 'Maintenance', 'Inactive'))
);

CREATE TABLE IF NOT EXISTS transports.driver (
    driver_id VARCHAR(10) PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    license_type VARCHAR(30) NOT NULL,
    experience_years INTEGER NOT NULL,
    department VARCHAR(50),
    status VARCHAR(20) NOT NULL,

    CONSTRAINT chk_driver_experience
        CHECK (experience_years >= 0),

    CONSTRAINT chk_driver_status
        CHECK (status IN ('Active', 'Inactive', 'Suspended'))
);

CREATE TABLE IF NOT EXISTS transports.route (
    route_id VARCHAR(10) PRIMARY KEY,
    origin VARCHAR(100) NOT NULL,
    destination VARCHAR(100) NOT NULL,
    distance_km DECIMAL(8,2) NOT NULL,
    route_type VARCHAR(30) NOT NULL,

    CONSTRAINT chk_route_distance
        CHECK (distance_km > 0),

    CONSTRAINT chk_route_origin_destination
        CHECK (origin <> destination)
);

CREATE TABLE IF NOT EXISTS transports.trip (
    trip_id VARCHAR(15) PRIMARY KEY,
    vehicle_id VARCHAR(10) NOT NULL,
    driver_id VARCHAR(10) NOT NULL,
    route_id VARCHAR(10) NOT NULL,
    departure_time TIMESTAMP NOT NULL,
    arrival_time TIMESTAMP,
    distance_km DECIMAL(8,2) NOT NULL,
    cargo_weight_tons DECIMAL(8,2) NOT NULL,
    trip_status VARCHAR(20) NOT NULL,

    CONSTRAINT fk_trip_vehicle
        FOREIGN KEY (vehicle_id)
        REFERENCES transports.vehicle(vehicle_id),

    CONSTRAINT fk_trip_driver
        FOREIGN KEY (driver_id)
        REFERENCES transports.driver(driver_id),

    CONSTRAINT fk_trip_route
        FOREIGN KEY (route_id)
        REFERENCES transports.route(route_id),

    CONSTRAINT chk_trip_distance
        CHECK (distance_km >= 0),

    CONSTRAINT chk_trip_cargo
        CHECK (cargo_weight_tons >= 0),

    CONSTRAINT chk_trip_times
        CHECK (arrival_time IS NULL OR arrival_time >= departure_time),

    CONSTRAINT chk_trip_status
        CHECK (trip_status IN ('Planned', 'In Progress', 'Completed', 'Cancelled'))
);

CREATE TABLE IF NOT EXISTS transports.gps_event (
    event_id VARCHAR(30) PRIMARY KEY,
    vehicle_id VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    latitude DECIMAL(9,6) NOT NULL,
    longitude DECIMAL(9,6) NOT NULL,
    speed_kmh DECIMAL(6,2) NOT NULL,
    fuel_level DECIMAL(5,2),
    engine_temperature DECIMAL(6,2),

    CONSTRAINT fk_gps_vehicle
        FOREIGN KEY (vehicle_id)
        REFERENCES transports.vehicle(vehicle_id),

    CONSTRAINT chk_gps_latitude
        CHECK (latitude BETWEEN -90 AND 90),

    CONSTRAINT chk_gps_longitude
        CHECK (longitude BETWEEN -180 AND 180),

    CONSTRAINT chk_gps_speed
        CHECK (speed_kmh >= 0),

    CONSTRAINT chk_gps_fuel
        CHECK (fuel_level IS NULL OR fuel_level BETWEEN 0 AND 100)
);

CREATE TABLE IF NOT EXISTS transports.fuel_transaction (
    fuel_transaction_id VARCHAR(20) PRIMARY KEY,
    vehicle_id VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    liters DECIMAL(8,2) NOT NULL,
    price_per_liter DECIMAL(6,3) NOT NULL,
    station VARCHAR(100) NOT NULL,

    CONSTRAINT fk_fuel_vehicle
        FOREIGN KEY (vehicle_id)
        REFERENCES transports.vehicle(vehicle_id),

    CONSTRAINT chk_fuel_liters
        CHECK (liters > 0),

    CONSTRAINT chk_fuel_price
        CHECK (price_per_liter > 0)
);

CREATE TABLE IF NOT EXISTS transports.maintenance (
    maintenance_id VARCHAR(20) PRIMARY KEY,
    vehicle_id VARCHAR(10) NOT NULL,
    maintenance_date TIMESTAMP NOT NULL,
    maintenance_type VARCHAR(50) NOT NULL,
    description TEXT,
    cost DECIMAL(10,2) NOT NULL,
    downtime_hours DECIMAL(8,2) NOT NULL,

    CONSTRAINT fk_maintenance_vehicle
        FOREIGN KEY (vehicle_id)
        REFERENCES transports.vehicle(vehicle_id),

    CONSTRAINT chk_maintenance_cost
        CHECK (cost >= 0),

    CONSTRAINT chk_maintenance_downtime
        CHECK (downtime_hours >= 0)
);

CREATE TABLE IF NOT EXISTS transports.incident (
    incident_id VARCHAR(20) PRIMARY KEY,
    vehicle_id VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    incident_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    description TEXT,

    CONSTRAINT fk_incident_vehicle
        FOREIGN KEY (vehicle_id)
        REFERENCES transports.vehicle(vehicle_id),

    CONSTRAINT chk_incident_severity
        CHECK (severity IN ('Low', 'Medium', 'High', 'Critical'))
);