{{
    config(
        materialized='table'
    )
}}

with green_trips as (
    select 
        *,
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
),
yellow_trips as (
    select
        *,
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
),
all_trips as (
    select * from green_trips
    union all
    select * from yellow_trips
),
taxi_zones as (
    select 
        * 
    from {{ ref('taxi_zone_lookup') }}
    where borough <> 'Unknown'
)
select
    trips.tripid, 
    trips.vendorid, 
    trips.service_type,
    trips.ratecodeid, 
    trips.pickup_locationid, 
    pu_zones.borough as pickup_borough, 
    pu_zones.zone as pickup_zone, 
    trips.dropoff_locationid,
    do_zones.borough as dropoff_borough, 
    do_zones.zone as dropoff_zone,  
    trips.pickup_datetime, 
    trips.dropoff_datetime, 
    trips.store_and_fwd_flag, 
    trips.passenger_count, 
    trips.trip_distance, 
    trips.trip_type, 
    trips.fare_amount, 
    trips.extra, 
    trips.mta_tax, 
    trips.tip_amount, 
    trips.tolls_amount, 
    trips.ehail_fee, 
    trips.improvement_surcharge, 
    trips.total_amount, 
    trips.payment_type, 
    trips.payment_type_description,
    extract(year from trips.pickup_datetime) as pickup_year,
    extract(month from trips.pickup_datetime) as pickup_month,
    extract(quarter from trips.pickup_datetime) as pickup_quarter,
    concat (extract(year from trips.pickup_datetime), '/Q', extract(quarter from trips.pickup_datetime)) as pickup_year_quarter
from
    all_trips trips
inner join
    taxi_zones pu_zones on trips.pickup_locationid = pu_zones.locationid 
inner join
    taxi_zones do_zones on trips.dropoff_locationid = do_zones.locationid