with transactions as (

    select *
    from {{ ref('int_property_transactions') }}

),

county_property_type_summary as (

    select
        county,
        property_type_name,

        count(*) as transaction_count,

        avg(price) as average_sale_price,

        percentile_cont(0.5)
            within group (order by price) as median_sale_price

    from transactions

    group by
        county,
        property_type_name

)

select *
from county_property_type_summary