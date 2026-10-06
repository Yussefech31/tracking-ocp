select
    route_id,
    origin,
    destination,
    distance_km,
    route_type
from {{ source('raw', 'route') }}
where route_id is not null
  and distance_km > 0
  and origin <> destination