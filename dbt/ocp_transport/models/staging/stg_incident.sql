select
    incident_id,
    vehicle_id,
    timestamp,
    incident_type,
    severity,
    description
from {{ source('raw', 'incident') }}
where incident_id is not null
  and vehicle_id is not null
  and severity in ('Low', 'Medium', 'High', 'Critical')