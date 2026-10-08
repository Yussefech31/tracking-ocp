with invalid_trips as (
    select
        'trip' as entity_name,
        trip_id as record_id
    from {{ ref('stg_trip') }}
    where
        distance_km < 0
        or cargo_weight_tons < 0
),

invalid_fuel as (
    select
        'fuel_transaction' as entity_name,
        fuel_transaction_id as record_id
    from {{ ref('stg_fuel_transaction') }}
    where
        liters <= 0
        or price_per_liter <= 0
),

invalid_maintenance as (
    select
        'maintenance' as entity_name,
        maintenance_id as record_id
    from {{ ref('stg_maintenance') }}
    where
        cost < 0
        or downtime_hours < 0
),

invalid_routes as (
    select
        'route' as entity_name,
        route_id as record_id
    from {{ ref('stg_route') }}
    where distance_km <= 0
)

select *
from invalid_trips
union all
select *
from invalid_fuel
union all
select *
from invalid_maintenance
union all
select *
from invalid_routes
