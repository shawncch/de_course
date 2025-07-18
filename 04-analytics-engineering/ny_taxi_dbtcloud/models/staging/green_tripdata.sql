{{
    config(
        materialized='view'
    )
}}

with green_trips as (
    select 
        *
    from 
        {{ source('staging', 'green_tripdata') }}
)
select * from green_trips