select
    trip_id,
    departure_time,
    arrival_time
from {{ ref('stg_trip') }}
where
    arrival_time is not null
    and arrival_time < departure_time
