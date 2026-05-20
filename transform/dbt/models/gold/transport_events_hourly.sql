-- Hourly aggregate of streaming transport events for the Gold layer.
select
    date_trunc(''hour'', cast(event_time as timestamp)) as event_hour,
    state,
    event_type,
    count(*) as event_count,
    avg(cast(severity as double)) as avg_severity
from {{ source(''silver'', ''transport_events'') }}
where event_time is not null
group by 1, 2, 3