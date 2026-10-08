{{ config(tags=['streaming']) }}

select
    vehicle_id,
    event_time,
    latitude,
    longitude,
    speed_kmh,
    fuel_level,
    engine_temperature,
    is_overspeed,
    is_overheating,
    is_low_fuel,
    is_stopped,
    operational_status,
    loaded_at
from {{ source('raw', 'stream_telemetry') }}
where
    vehicle_id is not null
    and event_time is not null
    and latitude between -90 and 90
    and longitude between -180 and 180
    and speed_kmh >= 0
