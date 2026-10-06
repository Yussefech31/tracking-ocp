with trip_kpis as (
    select
        route_id,
        count(trip_id) as completed_trips,
        sum(distance_km) as total_actual_distance_km,
        avg(distance_km) as avg_actual_distance_km,
        sum(cargo_weight_tons) as total_cargo_tons,
        avg(cargo_weight_tons) as avg_cargo_tons,
        avg(trip_duration_minutes) as avg_trip_duration_minutes
    from {{ ref('stg_trip') }}
    where trip_status = 'Completed'
    group by route_id
)

select
    r.route_id,
    r.origin,
    r.destination,
    r.distance_km as planned_distance_km,
    r.route_type,
    coalesce(t.completed_trips, 0) as completed_trips,
    coalesce(t.total_actual_distance_km, 0) as total_actual_distance_km,
    coalesce(t.avg_actual_distance_km, 0) as avg_actual_distance_km,
    coalesce(t.total_cargo_tons, 0) as total_cargo_tons,
    coalesce(t.avg_cargo_tons, 0) as avg_cargo_tons,
    coalesce(t.avg_trip_duration_minutes, 0) as avg_trip_duration_minutes,
    case
        when coalesce(t.completed_trips, 0) > 0
        then round(
            t.avg_actual_distance_km - r.distance_km,
            2
        )
        else 0
    end as route_deviation_km,
    case
        when r.distance_km > 0
             and coalesce(t.completed_trips, 0) > 0
        then round(
            (
                (t.avg_actual_distance_km - r.distance_km)
                / r.distance_km
            ) * 100,
            2
        )
        else 0
    end as route_deviation_percent
from {{ ref('stg_route') }} r
left join trip_kpis t
    on r.route_id = t.route_id