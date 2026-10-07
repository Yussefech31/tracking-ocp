with table_counts as (
    select
        'vehicle_kpis' as table_name,
        count(*) as row_count
    from {{ ref('vehicle_kpis') }}
    union all
    select
        'route_kpis' as table_name,
        count(*) as row_count
    from {{ ref('route_kpis') }}
    union all
    select
        'driver_kpis' as table_name,
        count(*) as row_count
    from {{ ref('driver_kpis') }}
    union all
    select
        'fleet_kpis' as table_name,
        count(*) as row_count
    from {{ ref('fleet_kpis') }}
)

select
    table_name,
    row_count
from table_counts
where row_count = 0
