select
    d.driver_id,
    d.first_name,
    d.last_name,
    d.license_type,
    d.experience_years,
    d.department,
    d.status,
    count(t.trip_id) as total_trips,
    sum(
        case
            when t.trip_status = 'Completed' then 1
            else 0
        end
    ) as completed_trips,
    sum(
        case
            when t.trip_status = 'Completed'
            then t.distance_km
            else 0
        end
    ) as total_distance_km,
    sum(
        case
            when t.trip_status = 'Completed'
            then t.cargo_weight_tons
            else 0
        end
    ) as total_cargo_tons,
    avg(
        case
            when t.trip_status = 'Completed'
            then t.distance_km
        end
    ) as avg_trip_distance_km,
    avg(
        case
            when t.trip_status = 'Completed'
            then t.trip_duration_minutes
        end
    ) as avg_trip_duration_minutes
from {{ ref('stg_driver') }} d
left join {{ ref('stg_trip') }} t
    on d.driver_id = t.driver_id
group by
    d.driver_id,
    d.first_name,
    d.last_name,
    d.license_type,
    d.experience_years,
    d.department,
    d.status