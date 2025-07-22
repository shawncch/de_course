{{
    config(
        materialized='table',
    )
}}

with all_trips as (
    select * from {{ ref('fact_trips') }}
),
trips_per_quarter as (
    select
        service_type, 
        pickup_quarter,
        pickup_year,
        pickup_year_quarter,
        sum(total_amount) as quarterly_revenue
    from all_trips
    group by 1, 2, 3, 4
),
trips_per_quarter_lagged as (
    select 
        *,
        case 
            when lag(pickup_year) over (partition by service_type, pickup_quarter order by pickup_year) = pickup_year - 1
                then lag(pickup_year_quarter) over (partition by service_type, pickup_quarter order by pickup_year)
            else null
        end as previous_year_quarter,

        case 
            when lag(pickup_year) over (partition by service_type, pickup_quarter order by pickup_year) = pickup_year - 1
                then lag(quarterly_revenue) over (partition by service_type, pickup_quarter order by pickup_year)
            else null
        end as previous_year_quarter_revenue,
        
    from trips_per_quarter
    -- order by pickup_year, pickup_quarter
)
select 
    *,
    round((quarterly_revenue - previous_year_quarter_revenue) / previous_year_quarter_revenue * 100, 2) as yoy_growth
from trips_per_quarter_lagged
where pickup_year = 2020
order by service_type, yoy_growth desc
