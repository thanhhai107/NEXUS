-- Gold model for Transport analytics over US Accidents.
select
    state,
    cast(severity as integer) as severity,
    count(*) as accident_count,
    sum(cast(distance_mi as double)) as total_distance_mi
from {{ source('silver', 'us_accidents') }}
group by 1, 2
