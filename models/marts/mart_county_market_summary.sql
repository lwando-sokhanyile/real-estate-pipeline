with transactions as (

    select *
    from {{ ref('int_property_transactions') }}

),

county_summary as (

    select
        county,
        COUNT(*) as transaction_count,
        AVG(price) as average_sale_price

    from transactions

    group by county

)

select *
from county_summary