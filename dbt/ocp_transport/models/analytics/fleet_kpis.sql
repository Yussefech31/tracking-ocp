with vehicle_kpis as (
    select
        count(vehicle_id) as total_vehicles,
        sum(
            case
                when status = 'Active' then 1
                else 0
            end
        ) as active_vehicles
    from {{ ref('stg_vehicle') }}
),

trip_kpis as (
    select
        count(trip_id) as total_trips,
        sum(
            case
                when trip_status = 'Completed' then 1
                else 0
            end
        ) as completed_trips,
        sum(
            case
                when trip_status = 'Completed'
                    then distance_km
                else 0
            end
        ) as total_distance_km,
        sum(
            case
                when trip_status = 'Completed'
                    then cargo_weight_tons
                else 0
            end
        ) as total_cargo_tons
    from {{ ref('stg_trip') }}
),

fuel_kpis as (
    select
        sum(liters) as total_fuel_liters,
        sum(total_cost) as total_fuel_cost
    from {{ ref('stg_fuel_transaction') }}
),

maintenance_kpis as (
    select
        sum(cost) as total_maintenance_cost,
        sum(downtime_hours) as total_downtime_hours
    from {{ ref('stg_maintenance') }}
),

incident_kpis as (
    select count(incident_id) as total_incidents
    from {{ ref('stg_incident') }}
)

select
    v.total_vehicles,
    v.active_vehicles,
    t.total_trips,
    t.completed_trips,
    t.total_distance_km,
    t.total_cargo_tons,
    f.total_fuel_liters,
    f.total_fuel_cost,
    m.total_maintenance_cost,
    m.total_downtime_hours,
    i.total_incidents,
    case
        when t.total_distance_km > 0
            then round(
                (f.total_fuel_liters / t.total_distance_km) * 100,
                2
            )
        else 0
    end as fuel_liters_per_100km,
    case
        when t.total_distance_km > 0
            then round(
                f.total_fuel_cost / t.total_distance_km,
                2
            )
        else 0
    end as fuel_cost_per_km,
    case
        when t.total_distance_km > 0
            then round(
                m.total_maintenance_cost / t.total_distance_km,
                2
            )
        else 0
    end as maintenance_cost_per_km,
    case
        when t.completed_trips > 0
            then round(
                i.total_incidents / t.completed_trips,
                3
            )
        else 0
    end as incidents_per_trip
from vehicle_kpis as v
cross join trip_kpis as t
cross join fuel_kpis as f
cross join maintenance_kpis as m
cross join incident_kpis as i
