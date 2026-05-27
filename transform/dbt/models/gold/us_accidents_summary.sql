-- Gold model for Transport analytics over US Accidents.
with distance_unit as (
    select cast(scale_factor as double) as miles_to_km
    from {{ ref('unit_mapping') }}
    where dimension_type = 'distance'
      and source_unit = 'mi'
      and lower(cast(conversion_supported as varchar)) = 'true'
)

select
    state,
    cast(severity as integer) as severity,
    count(*) as accident_count,
    sum(cast(distance_mi as double)) as total_distance_mi,
    sum(cast(distance_mi as double) * distance_unit.miles_to_km) as total_distance_km
from {{ source('silver', 'us_accidents') }}
cross join distance_unit
group by 1, 2
