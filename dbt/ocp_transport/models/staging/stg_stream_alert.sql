{{ config(tags=['streaming']) }}

select
    alert_id,
    vehicle_id,
    alert_time,
    alert_type,
    severity,
    metric_value,
    threshold,
    latitude,
    longitude,
    description,
    loaded_at
from {{ source('raw', 'stream_alerts') }}
where
    alert_id is not null
    and vehicle_id is not null
    and alert_time is not null
