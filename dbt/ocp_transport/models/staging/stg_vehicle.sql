select
    vehicle_id,
    vehicle_type,
    manufacturer,
    model,
    matriculation,
    capacity_tons,
    fuel_type,
    year,
    status
from {{ source('raw', 'vehicle') }}
where
    vehicle_id is not null
    and matriculation is not null
    and capacity_tons > 0
    and year >= 2000
    and fuel_type in ('Diesel', 'Electric', 'Hybrid')
    and status in ('Active', 'Maintenance', 'Inactive')
