select
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }}
    as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }}
    as dropoff_locationid,
    sr_flag,
    affiliated_base_number
from {{ source("nyc_taxi", "raw_fhv") }}
where dispatching_base_num is not null
{% if var("is_test_run", default=true) %} limit 100 {% endif %}
