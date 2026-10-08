select
    driver_id,
    first_name,
    last_name,
    license_type,
    experience_years,
    department,
    status
from {{ source('raw', 'driver') }}
where
    driver_id is not null
    and experience_years >= 0
