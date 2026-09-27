with transactions as (

    select *
    from {{ ref('int_property_transactions') }}

),

county_price_statistics as (

    select
        county,

        count(*) as transaction_count,

        avg(price) as average_sale_price,

        percentile_cont(0.5)
            within group (order by price) as median_sale_price

    from transactions

    group by county

)

select *
from county_price_statistics