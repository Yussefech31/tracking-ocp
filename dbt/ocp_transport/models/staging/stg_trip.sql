select
    trip_id,
    vehicle_id,
    driver_id,
    route_id,
    departure_time,
    arrival_time,
    distance_km,
    cargo_weight_tons,
    trip_status,
    datediff(
        minute,
        departure_time,
        arrival_time
    ) as trip_duration_minutes
from {{ source('raw', 'trip') }}
where
    trip_id is not null
    and vehicle_id is not null
    and driver_id is not null
    and route_id is not null
    and distance_km >= 0
    and cargo_weight_tons >= 0
    and (
        arrival_time is null
        or arrival_time >= departure_time
    )
