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
    overspeed_events
    + temperature_anomalies
    + low_fuel_events
    + stopped_vehicles as total_flags,
    round(
        (overspeed_events + temperature_anomalies + low_fuel_events)
        / nullif(total_events, 0) * 100,
        2
    ) as anomaly_rate_pct
from {{ ref('stg_stream_fleet_kpis') }}
