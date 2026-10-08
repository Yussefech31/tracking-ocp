{{ config(tags=['streaming']) }}

select
    window_start,
    window_end,
    active_vehicles,
    avg_speed_kmh,
    max_speed_kmh,
    avg_engine_temperature,
    max_engine_temperature,
    avg_fuel_level,
    overspeed_events,
    temperature_anomalies,
    low_fuel_events,
    stopped_vehicles,
    total_events,
    loaded_at
from {{ source('raw', 'stream_fleet_kpis') }}
where window_start is not null
