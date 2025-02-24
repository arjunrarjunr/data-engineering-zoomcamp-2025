select
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    pickup_locationid,
    dropoff_locationid,
    sr_flag,
    affiliated_base_number,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.zone as dropoff_zone,
    extract(month from pickup_datetime) as mn,
    extract(year from pickup_datetime) as yr
from {{ ref("stg_fhv") }} fhv
inner join
    {{ ref("dim_zones") }} as pickup_zone
    on fhv.pickup_locationid = pickup_zone.locationid
inner join
    {{ ref("dim_zones") }} as dropoff_zone
    on fhv.dropoff_locationid = dropoff_zone.locationid
