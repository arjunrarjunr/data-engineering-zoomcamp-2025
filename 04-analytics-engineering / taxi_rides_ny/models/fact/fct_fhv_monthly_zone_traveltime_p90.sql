with
    step1 as (
        select
            *,
            timestamp_diff(dropoff_datetime, pickup_datetime, second) as trip_duration
        from {{ ref("dim_fhv_trips") }}
    ),
    step2 as (select * from step1 where mn = 11 and yr = 2019),
    step3 as (
        select
            *,
            percentile_cont(trip_duration, 0.90) over (
                partition by yr, mn, pickup_zone, dropoff_zone
            ) as p90
        from step2
    ),
    ranked_durations as (
        select
            pickup_zone,
            dropoff_zone,
            p90,
            dense_rank() over (partition by pickup_zone order by p90 desc) as rank
        from step3
    )
select pickup_zone, dropoff_zone, p90
from ranked_durations
where rank = 2
order by 3 desc
