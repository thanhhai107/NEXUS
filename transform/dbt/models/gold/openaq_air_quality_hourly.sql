-- Hourly aggregate of OpenAQ air quality measurements for the Gold layer.
with standardized as (
    select
        date_trunc('hour', from_iso8601_timestamp(datetime)) as observed_hour,
        measurements.location,
        measurements.parameter,
        coalesce(unit_map.canonical_unit, measurements.unit) as unit,
        case
            when unit_map.scale_factor is not null then
                cast(measurements.value as double) * cast(unit_map.scale_factor as double)
                + coalesce(cast(unit_map.offset as double), 0)
            else cast(measurements.value as double)
        end as value_canonical
    from {{ source('silver', 'openaq_measurements') }} as measurements
    left join {{ ref('unit_mapping') }} as unit_map
        on lower(trim(measurements.unit)) = lower(trim(unit_map.source_unit))
        and unit_map.dimension_type = 'concentration'
        and lower(cast(unit_map.conversion_supported as varchar)) = 'true'
    where measurements.datetime is not null
)

select
    observed_hour,
    location,
    parameter,
    unit,
    count(*) as reading_count,
    avg(value_canonical) as avg_value,
    min(value_canonical) as min_value,
    max(value_canonical) as max_value
from standardized
group by 1, 2, 3, 4
