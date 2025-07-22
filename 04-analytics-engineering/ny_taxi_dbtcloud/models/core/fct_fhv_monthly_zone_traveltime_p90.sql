{{
    config(
        materialized='table'
    )
}}

with fhvtrips as (
    select 
        *,
        timestamp_diff(dropoff_datetime, pickup_datetime, SECOND) as trip_duration 
    from {{ ref('dim_fhv_trips') }}
),
fhvtrips_p90 as (
    select distinct
        pickup_year,
        pickup_month,
        pulocationid,
        dolocationid,
        pickup_zone,
        dropoff_zone,
        percentile_cont(trip_duration, 0.9) over (partition by pickup_year, pickup_month, pulocationid, dolocationid) as p90
    from fhvtrips
    where pickup_zone in ('Newark Airport', 'SoHo', 'Yorkville East') and pickup_year = 2019 and pickup_month = 11
),
fhvtrips_p90_partitioned as (
    select 
        *,
        dense_rank() over (partition by pickup_zone order by p90 desc) as rank
    from fhvtrips_p90
)
select * from fhvtrips_p90_partitioned where rank = 2
