select
    fuel_transaction_id,
    vehicle_id,
    timestamp,
    liters,
    price_per_liter,
    station,
    round(liters * price_per_liter, 2) as total_cost
from {{ source('raw', 'fuel_transaction') }}
where fuel_transaction_id is not null
  and vehicle_id is not null
  and liters > 0
  and price_per_liter > 0