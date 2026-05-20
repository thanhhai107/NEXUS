-- Hourly aggregate of OpenAQ air quality measurements for the Gold layer.
select
    date_trunc(''hour'', cast(datetime as timestamp)) as observed_hour,
    location,
    parameter,
    unit,
    count(*) as reading_count,
    avg(cast(value as double)) as avg_value,
    min(cast(value as double)) as min_value,
    max(cast(value as double)) as max_value
from {{ source(''silver'', ''openaq_measurements'') }}
where datetime is not null
group by 1, 2, 3, 4