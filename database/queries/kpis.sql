WITH trip_metrics AS (
    SELECT
        vehicle_id,
        COUNT(*) AS completed_trips,
        SUM(distance_km) AS total_distance_km,
        SUM(cargo_weight_tons) AS total_cargo_tons
    FROM transports.trip
    WHERE trip_status = 'Completed'
    GROUP BY vehicle_id
),

fuel_metrics AS (
    SELECT
        vehicle_id,
        SUM(liters) AS total_fuel_liters,
        SUM(liters * price_per_liter) AS total_fuel_cost
    FROM transports.fuel_transaction
    GROUP BY vehicle_id
),

maintenance_metrics AS (
    SELECT
        vehicle_id,
        SUM(cost) AS total_maintenance_cost,
        SUM(downtime_hours) AS total_downtime_hours
    FROM transports.maintenance
    GROUP BY vehicle_id
),

incident_metrics AS (
    SELECT
        vehicle_id,
        COUNT(*) AS incident_count
    FROM transports.incident
    GROUP BY vehicle_id
)

SELECT
    v.vehicle_id,
    v.manufacturer,
    v.model,

    COALESCE(t.completed_trips, 0) AS completed_trips,
    COALESCE(t.total_distance_km, 0) AS total_distance_km,
    COALESCE(t.total_cargo_tons, 0) AS total_cargo_tons,

    COALESCE(f.total_fuel_liters, 0) AS total_fuel_liters,
    COALESCE(f.total_fuel_cost, 0) AS total_fuel_cost,

    COALESCE(m.total_maintenance_cost, 0) AS total_maintenance_cost,
    COALESCE(m.total_downtime_hours, 0) AS total_downtime_hours,

    COALESCE(i.incident_count, 0) AS incident_count,

    ROUND(
        COALESCE(f.total_fuel_liters, 0)
        / NULLIF(t.total_distance_km, 0) * 100,
        2
    ) AS liters_per_100km,

    ROUND(
        COALESCE(f.total_fuel_cost, 0)
        / NULLIF(t.total_distance_km, 0),
        2
    ) AS fuel_cost_per_km,

    ROUND(
        COALESCE(m.total_maintenance_cost, 0)
        / NULLIF(t.total_distance_km, 0),
        2
    ) AS maintenance_cost_per_km

FROM transports.vehicle v

LEFT JOIN trip_metrics t
    ON v.vehicle_id = t.vehicle_id

LEFT JOIN fuel_metrics f
    ON v.vehicle_id = f.vehicle_id

LEFT JOIN maintenance_metrics m
    ON v.vehicle_id = m.vehicle_id

LEFT JOIN incident_metrics i
    ON v.vehicle_id = i.vehicle_id

ORDER BY total_distance_km DESC;