{{
    config(
        materialized='table'
    )
}}

with valid_trips as (
    select 
        *
    from {{ ref('fact_trips') }}
    where fare_amount > 0 and trip_distance > 0 and payment_type_description in ('Cash', 'Credit card')
)
select distinct
    service_type,
    pickup_year,
    pickup_month,
    percentile_cont(fare_amount, 0.97) OVER (PARTITION BY service_type, pickup_year, pickup_month) as p97,
    percentile_cont(fare_amount, 0.95) OVER (PARTITION BY service_type, pickup_year, pickup_month) as p95,
    percentile_cont(fare_amount, 0.90) OVER (PARTITION BY service_type, pickup_year, pickup_month) as p90
from valid_trips
where pickup_year = 2020 and pickup_month = 4