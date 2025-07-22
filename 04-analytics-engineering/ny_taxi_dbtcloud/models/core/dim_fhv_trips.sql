{{
    config(
        materialized='table'
    )
}}

with fhvtrips as (
    select * from {{ ref('stg_fhv_tripdata') }}
),
taxi_zones as (
    select * from {{ ref('taxi_zone_lookup') }}
    where borough <> 'Unknown'
)
select
    trips.dispatching_base_num,
    trips.pickup_datetime,
    extract(year from trips.pickup_datetime) as pickup_year,
    extract(month from trips.pickup_datetime) as pickup_month,
    trips.dropoff_datetime,
    trips.pulocationid,
    trips.dolocationid,
    trips.SR_Flag,
    trips.affiliated_base_number,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    pickup_zone.service_zone as pickup_service_zone,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    dropoff_zone.service_zone as dropoff_service_zone
    
from fhvtrips as trips
inner join taxi_zones as pickup_zone
on trips.PUlocationid = pickup_zone.locationid
inner join taxi_zones as dropoff_zone
on trips.DOlocationid = dropoff_zone.locationid