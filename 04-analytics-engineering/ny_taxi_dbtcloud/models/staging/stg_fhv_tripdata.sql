with trip_data as (
    select * from {{ source('staging', 'fhv_tripdata') }}
    where dispatching_base_num is not null
)
select * from trip_data
