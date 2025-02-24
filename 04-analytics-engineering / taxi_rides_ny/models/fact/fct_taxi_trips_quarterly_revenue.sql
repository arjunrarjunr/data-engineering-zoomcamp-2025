with
    green as (
        select 'green' as ind, pickup_datetime as ts, total_amount as revenue
        from {{ ref("green_fact") }}
    ),
    yellow as (
        select 'yellow' as ind, pickup_datetime as ts, total_amount as revenue
        from {{ ref("yellow_fact") }}
    ),
    unioned as (
        select *
        from green
        union all
        select *
        from yellow
    ),
    time_eng_cte as (
        select *, extract(quarter from ts) as qr, extract(year from ts) as yr
        from unioned
    ),
    agg_cte as (
        select ind, yr, qr, sum(revenue) as revenue
        from time_eng_cte
        group by ind, yr, qr
    ),
    filtered as (select * from agg_cte where yr in (2019, 2020)),
    yoy as (
        select
            *,
            round(
                (
                    (revenue - lag(revenue) over (partition by ind, qr order by yr))
                    / lag(revenue) over (partition by ind, qr order by yr)
                )
                * 100,
                2
            ) as yoy_growth_percentage
        from filtered
    )
select ind, qr, yr, yoy_growth_percentage, revenue
from yoy
order by ind, yoy_growth_percentage
