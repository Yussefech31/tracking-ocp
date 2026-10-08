{{ config(tags=['streaming']) }}

select
    a.alert_id,
    a.vehicle_id,
    v.vehicle_type,
    v.manufacturer,
    v.model,
    v.status as vehicle_status,
    a.alert_time,
    cast(a.alert_time as date) as alert_date,
    date_trunc('hour', a.alert_time) as alert_hour,
    a.alert_type,
    a.severity,
    case a.severity when 'CRITICAL' then 1 else 2 end as severity_rank,
    a.metric_value,
    a.threshold,
    round(a.metric_value - a.threshold, 2) as threshold_breach,
    a.latitude,
    a.longitude,
    a.description,
    a.loaded_at
from {{ ref('stg_stream_alert') }} as a
left join {{ ref('stg_vehicle') }} as v
    on a.vehicle_id = v.vehicle_id
