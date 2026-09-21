# OCP Transport Intelligence Platform

## Logical Data Model

This document defines the logical data model for the OCP Transport Intelligence Platform. The model represents the operational transportation system and will later be transformed into an analytical model for Snowflake.

---

## 1. VEHICLE

| Column          | Data Type    | PK  | FK | Nullable | Business Rule                 |
| --------------- | ------------ | --- | -- | -------- | ----------------------------- |
| `vehicle_id`    | VARCHAR(10)  | YES | NO | NO       | Unique vehicle identifier     |
| `vehicle_type`  | VARCHAR(30)  | NO  | NO | NO       | Truck, Tanker, Trailer        |
| `manufacturer`  | VARCHAR(50)  | NO  | NO | NO       | Vehicle manufacturer          |
| `model`         | VARCHAR(50)  | NO  | NO | NO       | Vehicle model                 |
| `capacity_tons` | DECIMAL(6,2) | NO  | NO | NO       | Must be > 0                   |
| `fuel_type`     | VARCHAR(20)  | NO  | NO | NO       | Diesel, Electric, Hybrid      |
| `year`          | INTEGER      | NO  | NO | NO       | Valid vehicle year            |
| `status`        | VARCHAR(20)  | NO  | NO | NO       | Active, Maintenance, Inactive |

**Primary Key:** `vehicle_id`

---

## 2. DRIVER

| Column             | Data Type   | PK  | FK | Nullable | Business Rule               |
| ------------------ | ----------- | --- | -- | -------- | --------------------------- |
| `driver_id`        | VARCHAR(10) | YES | NO | NO       | Unique driver identifier    |
| `first_name`       | VARCHAR(50) | NO  | NO | NO       | Driver first name           |
| `last_name`        | VARCHAR(50) | NO  | NO | NO       | Driver last name            |
| `license_type`     | VARCHAR(30) | NO  | NO | NO       | Valid license category      |
| `experience_years` | INTEGER     | NO  | NO | NO       | Must be >= 0                |
| `department`       | VARCHAR(50) | NO  | NO | YES      | Driver department           |
| `status`           | VARCHAR(20) | NO  | NO | NO       | Active, Inactive, Suspended |

**Primary Key:** `driver_id`

---

## 3. ROUTE

| Column        | Data Type    | PK  | FK | Nullable | Business Rule                |
| ------------- | ------------ | --- | -- | -------- | ---------------------------- |
| `route_id`    | VARCHAR(10)  | YES | NO | NO       | Unique route identifier      |
| `origin`      | VARCHAR(100) | NO  | NO | NO       | Starting location            |
| `destination` | VARCHAR(100) | NO  | NO | NO       | Destination                  |
| `distance_km` | DECIMAL(8,2) | NO  | NO | NO       | Planned distance > 0         |
| `route_type`  | VARCHAR(30)  | NO  | NO | NO       | Internal, External, Transfer |

**Primary Key:** `route_id`

### Distance definition

`ROUTE.distance_km` represents the **planned/standard distance** of the route.

Example:

```text
R001
Khouribga → Benguerir
Planned distance = 180 km
```

---

## 4. TRIP

| Column              | Data Type    | PK  | FK  | Nullable | Business Rule                              |
| ------------------- | ------------ | --- | --- | -------- | ------------------------------------------ |
| `trip_id`           | VARCHAR(15)  | YES | NO  | NO       | Unique trip identifier                     |
| `vehicle_id`        | VARCHAR(10)  | NO  | YES | NO       | Must exist in VEHICLE                      |
| `driver_id`         | VARCHAR(10)  | NO  | YES | NO       | Must exist in DRIVER                       |
| `route_id`          | VARCHAR(10)  | NO  | YES | NO       | Must exist in ROUTE                        |
| `departure_time`    | TIMESTAMP    | NO  | NO  | NO       | Trip start                                 |
| `arrival_time`      | TIMESTAMP    | NO  | NO  | YES      | Must be >= departure                       |
| `distance_km`       | DECIMAL(8,2) | NO  | NO  | NO       | Actual distance >= 0                       |
| `cargo_weight_tons` | DECIMAL(8,2) | NO  | NO  | NO       | Must be >= 0                               |
| `trip_status`       | VARCHAR(20)  | NO  | NO  | NO       | Planned, In Progress, Completed, Cancelled |

**Primary Key:** `trip_id`

### Distance definition

`TRIP.distance_km` represents the **actual distance traveled** during a specific trip.

Therefore:

```text
ROUTE.distance_km = planned distance
TRIP.distance_km  = actual distance
```

Example:

```text
ROUTE R001 = 180 km planned

TRIP T001 = 187 km actual
```

This allows us to calculate route deviation:

```text
actual_distance - planned_distance
```

---

## 5. GPS_EVENT

| Column               | Data Type    | PK  | FK  | Nullable | Business Rule          |
| -------------------- | ------------ | --- | --- | -------- | ---------------------- |
| `event_id`           | VARCHAR(30)  | YES | NO  | NO       | Unique telemetry event |
| `vehicle_id`         | VARCHAR(10)  | NO  | YES | NO       | Must exist in VEHICLE  |
| `timestamp`          | TIMESTAMP    | NO  | NO  | NO       | Event timestamp        |
| `latitude`           | DECIMAL(9,6) | NO  | NO  | NO       | Between -90 and 90     |
| `longitude`          | DECIMAL(9,6) | NO  | NO  | NO       | Between -180 and 180   |
| `speed_kmh`          | DECIMAL(6,2) | NO  | NO  | NO       | Must be >= 0           |
| `fuel_level`         | DECIMAL(5,2) | NO  | NO  | YES      | Between 0 and 100      |
| `engine_temperature` | DECIMAL(6,2) | NO  | NO  | YES      | Realistic temperature  |

**Primary Key:** `event_id`

This will eventually become one of the main high-volume streaming datasets for Kafka and Spark Structured Streaming.

---

## 6. FUEL_TRANSACTION

| Column                | Data Type    | PK  | FK  | Nullable | Business Rule         |
| --------------------- | ------------ | --- | --- | -------- | --------------------- |
| `fuel_transaction_id` | VARCHAR(20)  | YES | NO  | NO       | Unique transaction    |
| `vehicle_id`          | VARCHAR(10)  | NO  | YES | NO       | Must exist in VEHICLE |
| `timestamp`           | TIMESTAMP    | NO  | NO  | NO       | Transaction time      |
| `liters`              | DECIMAL(8,2) | NO  | NO  | NO       | Must be > 0           |
| `price_per_liter`     | DECIMAL(6,3) | NO  | NO  | NO       | Must be > 0           |
| `station`             | VARCHAR(100) | NO  | NO  | NO       | Fuel station          |

**Primary Key:** `fuel_transaction_id`

### Derived value

We do not initially store `total_cost`.

Instead:

```text
total_cost = liters × price_per_liter
```

This is a derived metric that can later be calculated using SQL/dbt.

---

## 7. MAINTENANCE

| Column             | Data Type     | PK  | FK  | Nullable | Business Rule                |
| ------------------ | ------------- | --- | --- | -------- | ---------------------------- |
| `maintenance_id`   | VARCHAR(20)   | YES | NO  | NO       | Unique maintenance operation |
| `vehicle_id`       | VARCHAR(10)   | NO  | YES | NO       | Must exist in VEHICLE        |
| `maintenance_date` | TIMESTAMP     | NO  | NO  | NO       | Maintenance date             |
| `maintenance_type` | VARCHAR(50)   | NO  | NO  | NO       | Type of maintenance          |
| `description`      | TEXT          | NO  | NO  | YES      | Maintenance details          |
| `cost`             | DECIMAL(10,2) | NO  | NO  | NO       | Must be >= 0                 |
| `downtime_hours`   | DECIMAL(8,2)  | NO  | NO  | NO       | Must be >= 0                 |

**Primary Key:** `maintenance_id`

---

## 8. INCIDENT

| Column          | Data Type   | PK  | FK  | Nullable | Business Rule               |
| --------------- | ----------- | --- | --- | -------- | --------------------------- |
| `incident_id`   | VARCHAR(20) | YES | NO  | NO       | Unique incident             |
| `vehicle_id`    | VARCHAR(10) | NO  | YES | NO       | Must exist in VEHICLE       |
| `timestamp`     | TIMESTAMP   | NO  | NO  | NO       | Incident time               |
| `incident_type` | VARCHAR(50) | NO  | NO  | NO       | Type of incident            |
| `severity`      | VARCHAR(20) | NO  | NO  | NO       | Low, Medium, High, Critical |
| `description`   | TEXT        | NO  | NO  | YES      | Incident details            |

**Primary Key:** `incident_id`

---

# 9. Relationships

The logical relationships are:

```text
VEHICLE 1 ───── N TRIP
DRIVER  1 ───── N TRIP
ROUTE   1 ───── N TRIP

VEHICLE 1 ───── N GPS_EVENT
VEHICLE 1 ───── N FUEL_TRANSACTION
VEHICLE 1 ───── N MAINTENANCE
VEHICLE 1 ───── N INCIDENT
```

Conceptual representation:

```text
                     ┌──────────────┐
                     │    DRIVER    │
                     │ driver_id PK │
                     └──────┬───────┘
                            │
                            │ 1:N
                            ▼
┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│   VEHICLE    │─────▶│     TRIP     │◀─────│    ROUTE     │
│ vehicle_id PK│ 1:N  │  trip_id PK  │ N:1  │ route_id PK  │
└──────┬───────┘      │ vehicle_id FK│      └──────────────┘
       │              │ driver_id FK │
       │              │ route_id FK  │
       │              └──────────────┘
       │
       ├────────────── 1:N ────────────── GPS_EVENT
       │
       ├────────────── 1:N ────────────── FUEL_TRANSACTION
       │
       ├────────────── 1:N ────────────── MAINTENANCE
       │
       └────────────── 1:N ────────────── INCIDENT
```

---

# 10. Business Rules

## VEHICLE

```text
vehicle_id must be unique.
capacity_tons > 0.
status ∈ {Active, Maintenance, Inactive}.
```

## DRIVER

```text
driver_id must be unique.
experience_years >= 0.
```

## ROUTE

```text
route_id must be unique.
distance_km > 0.
origin != destination.
```

## TRIP

```text
trip_id must be unique.

vehicle_id must exist in VEHICLE.
driver_id must exist in DRIVER.
route_id must exist in ROUTE.

distance_km >= 0.
cargo_weight_tons >= 0.

arrival_time >= departure_time.

trip_status ∈ {
    Planned,
    In Progress,
    Completed,
    Cancelled
}.
```

## GPS_EVENT

```text
event_id must be unique.

latitude BETWEEN -90 AND 90.
longitude BETWEEN -180 AND 180.

speed_kmh >= 0.

fuel_level BETWEEN 0 AND 100.
```

## FUEL_TRANSACTION

```text
fuel_transaction_id must be unique.

liters > 0.
price_per_liter > 0.
```

## MAINTENANCE

```text
maintenance_id must be unique.

cost >= 0.
downtime_hours >= 0.
```

## INCIDENT

```text
incident_id must be unique.

severity ∈ {
    Low,
    Medium,
    High,
    Critical
}.
```

---

# 11. Operational vs Analytical Model

This document represents the **operational/logical model** of the transportation system.

It is not yet the final Snowflake analytical model.

The operational model is organized around entities and relationships:

```text
Vehicle
Driver
Route
Trip
GPS Event
Fuel Transaction
Maintenance
Incident
```

Later, we will transform this model into an analytical **star schema** optimized for analytics in Snowflake.

For example:

```text
                    DIM_VEHICLE
                         │
                         │
DIM_DRIVER ──────── FACT_TRIP ──────── DIM_ROUTE
                         │
                         │
                    DIM_DATE
```

Additional analytical fact tables will later be considered for:

```text
FACT_FUEL
FACT_MAINTENANCE
FACT_INCIDENT
FACT_GPS
```

The operational model and analytical model therefore serve different purposes:

```text
Operational Model
        ↓
Efficient data organization and relationships

Analytical Model
        ↓
Efficient reporting, aggregation and BI
```

---

# 12. Summary

The current logical model contains:

```text
8 entities

2 main dimensions:
    VEHICLE
    DRIVER

1 reference entity:
    ROUTE

5 event/transaction entities:
    TRIP
    GPS_EVENT
    FUEL_TRANSACTION
    MAINTENANCE
    INCIDENT
```

The model establishes:

```text
Primary Keys
Foreign Keys
Data Types
Nullability
Business Rules
Relationships
Derived Metrics
```

The next stage is to implement this logical model as a **physical PostgreSQL database**, including tables, constraints, indexes, and test data.
