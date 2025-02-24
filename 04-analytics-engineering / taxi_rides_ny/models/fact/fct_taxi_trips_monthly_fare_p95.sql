with
    green as (
        select 'green' as ind, pickup_datetime as ts, fare_amount
        from {{ ref("green_fact") }}
        where
            fare_amount > 0
            and trip_distance > 0
            and payment_type_description in ('Cash', 'Credit card')
    ),
    yellow as (
        select 'yellow' as ind, pickup_datetime as ts, fare_amount
        from {{ ref("yellow_fact") }}
        where
            fare_amount > 0
            and trip_distance > 0
            and payment_type_description in ('Cash', 'Credit card')
    ),
    unioned as (
        select *
        from green
        union all
        select *
        from yellow
    ),
    time_eng_cte as (
        select *, extract(year from ts) as yr, extract(month from ts) as mn from unioned
    ),
    filtered as (select * from time_eng_cte where yr = 2020 and mn = 4),
    engg as (
        select
            *,
            percentile_cont(fare_amount, 0.97) over (partition by ind, yr, mn) as p97,
            PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY ind, yr, mn) AS p95,
            PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY ind, yr, mn) AS p90
        from filtered
    )
select *
from engg
