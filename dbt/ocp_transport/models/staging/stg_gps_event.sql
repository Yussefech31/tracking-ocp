select
    event_id,
    vehicle_id,
    timestamp,
    latitude,
    longitude,
    speed_kmh,
    fuel_level,
    engine_temperature
from {{ source('raw', 'gps_event') }}
where
    event_id is not null
    and vehicle_id is not null
    and latitude between -90 and 90
    and longitude between -180 and 180
    and speed_kmh >= 0
    and (
        fuel_level is null
        or fuel_level between 0 and 100
    )
