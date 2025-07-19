with trip_data as (
    select
        *,
        row_number() over (partition by vendorid, tpep_pickup_datetime) as rn
    from {{ source('staging', 'yellow_tripdata') }}
    where vendorid is not null
)
select
    --identifiers
    {{ dbt_utils.generate_surrogate_key(["vendorid", "tpep_pickup_datetime"])}} as trip_id,
    {{ dbt.safe_cast("vendorid", api.Column.translate_type("integer")) }} as vendorid,
    {{ dbt.safe_cast("ratecodeid", api.Column.translate_type("integer")) }} as ratecodeid,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,

    --timestampS
    {{ dbt.safe_cast("tpep_pickup_datetime", api.Column.translate_type("timestamp")) }} as pickup_datetime,
    {{ dbt.safe_cast("tpep_dropoff_datetime", api.Column.translate_type("timestamp")) }} as dropoff_datetime,
    
    --tripinfo
    store_and_fwd_flag,
    {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    1 as trip_type,


    --paymentinfo
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(0 as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    coalesce({{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }}, 0) as payment_type,
    {{ get_payment_type_description("payment_type") }} as payment_description
    -- cast(congestion_surcharge as numeric)

from trip_data
where rn = 1

{%- if var('is_test_run', default=True) %}
limit 100
{% endif %}