with trip_kpis as (
    select
        vehicle_id,
        count(trip_id) as completed_trips,
        sum(distance_km) as total_distance_km,
        sum(cargo_weight_tons) as total_cargo_tons,
        avg(distance_km) as avg_distance_km
    from {{ ref('stg_trip') }}
    where trip_status = 'Completed'
    group by vehicle_id
),

fuel_kpis as (
    select
        vehicle_id,
        sum(liters) as total_fuel_liters,
        sum(total_cost) as total_fuel_cost
    from {{ ref('stg_fuel_transaction') }}
    group by vehicle_id
),

maintenance_kpis as (
    select
        vehicle_id,
        count(maintenance_id) as maintenance_operations,
        sum(cost) as total_maintenance_cost,
        sum(downtime_hours) as total_downtime_hours
    from {{ ref('stg_maintenance') }}
    group by vehicle_id
),

incident_kpis as (
    select
        vehicle_id,
        count(incident_id) as incident_count
    from {{ ref('stg_incident') }}
    group by vehicle_id
)

select
    v.vehicle_id,
    v.vehicle_type,
    v.manufacturer,
    v.model,
    v.capacity_tons,
    v.fuel_type,
    v.year,
    v.status,
    coalesce(t.completed_trips, 0) as completed_trips,
    coalesce(t.total_distance_km, 0) as total_distance_km,
    coalesce(t.total_cargo_tons, 0) as total_cargo_tons,
    coalesce(t.avg_distance_km, 0) as avg_distance_km,
    coalesce(f.total_fuel_liters, 0) as total_fuel_liters,
    coalesce(f.total_fuel_cost, 0) as total_fuel_cost,
    coalesce(m.maintenance_operations, 0) as maintenance_operations,
    coalesce(m.total_maintenance_cost, 0) as total_maintenance_cost,
    coalesce(m.total_downtime_hours, 0) as total_downtime_hours,
    coalesce(i.incident_count, 0) as incident_count,
    case
        when coalesce(t.total_distance_km, 0) > 0
        then round(
            coalesce(f.total_fuel_liters, 0)
            / t.total_distance_km * 100,
            2
        )
        else 0
    end as fuel_liters_per_100km,
    case
        when coalesce(t.total_distance_km, 0) > 0
        then round(
            coalesce(f.total_fuel_cost, 0)
            / t.total_distance_km,
            2
        )
        else 0
    end as fuel_cost_per_km,
    case
        when coalesce(t.total_distance_km, 0) > 0
        then round(
            coalesce(m.total_maintenance_cost, 0)
            / t.total_distance_km,
            2
        )
        else 0
    end as maintenance_cost_per_km,
    case
        when coalesce(t.completed_trips, 0) > 0
        then round(
            coalesce(i.incident_count, 0)
            / t.completed_trips,
            3
        )
        else 0
    end as incidents_per_trip
from {{ ref('stg_vehicle') }} v
left join trip_kpis t
    on v.vehicle_id = t.vehicle_id
left join fuel_kpis f
    on v.vehicle_id = f.vehicle_id
left join maintenance_kpis m
    on v.vehicle_id = m.vehicle_id
left join incident_kpis i
    on v.vehicle_id = i.vehicle_id