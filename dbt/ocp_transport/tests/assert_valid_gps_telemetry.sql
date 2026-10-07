select
    event_id,
    latitude,
    longitude,
    speed_kmh,
    fuel_level,
    engine_temperature
from {{ ref('stg_gps_event') }}
where
    latitude < -90
    or latitude > 90
    or longitude < -180
    or longitude > 180
    or speed_kmh < 0
    or (fuel_level is not null and (fuel_level < 0 or fuel_level > 100))
    or (
        engine_temperature is not null
        and (engine_temperature < -20 or engine_temperature > 150)
    )
