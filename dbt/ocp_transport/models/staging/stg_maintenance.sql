select
    maintenance_id,
    vehicle_id,
    maintenance_date,
    maintenance_type,
    description,
    cost,
    downtime_hours
from {{ source('raw', 'maintenance') }}
where
    maintenance_id is not null
    and vehicle_id is not null
    and cost >= 0
    and downtime_hours >= 0
