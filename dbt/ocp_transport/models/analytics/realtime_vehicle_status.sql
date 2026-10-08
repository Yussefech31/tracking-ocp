{{ config(tags=['streaming']) }}

with latest_telemetry as (
    select *
    from {{ ref('stg_stream_telemetry') }}
    qualify row_number() over (
        partition by vehicle_id
        order by event_time desc
    ) = 1
),

telemetry_flags as (
    select
        vehicle_id,
        count(*) as telemetry_events,
        sum(iff(is_overspeed, 1, 0)) as overspeed_flags,
        sum(iff(is_overheating, 1, 0)) as overheating_flags,
        sum(iff(is_low_fuel, 1, 0)) as low_fuel_flags,
        sum(iff(is_stopped, 1, 0)) as stopped_flags
    from {{ ref('stg_stream_telemetry') }}
    group by vehicle_id
),

alert_summary as (
    select
        vehicle_id,
        count(*) as total_alerts,
        sum(iff(severity = 'CRITICAL', 1, 0)) as critical_alerts,
        max(alert_time) as last_alert_time
    from {{ ref('stg_stream_alert') }}
    group by vehicle_id
)

select
    t.vehicle_id,
    v.vehicle_type,
    v.manufacturer,
    v.status as vehicle_status,
    t.event_time as last_seen_at,
    t.latitude,
    t.longitude,
    t.speed_kmh,
    t.fuel_level,
    t.engine_temperature,
    t.is_overspeed,
    t.is_overheating,
    t.is_low_fuel,
    t.is_stopped,
    t.operational_status,
    f.telemetry_events,
    f.overspeed_flags,
    f.overheating_flags,
    f.low_fuel_flags,
    f.stopped_flags,
    coalesce(a.total_alerts, 0) as total_alerts,
    coalesce(a.critical_alerts, 0) as critical_alerts,
    a.last_alert_time
from latest_telemetry as t
inner join telemetry_flags as f
    on t.vehicle_id = f.vehicle_id
left join alert_summary as a
    on t.vehicle_id = a.vehicle_id
left join {{ ref('stg_vehicle') }} as v
    on t.vehicle_id = v.vehicle_id
