with transactions as (

    select *
    from {{ ref('int_property_transactions') }}

),

monthly_market_trends as (

    select
        transfer_year,
        transfer_month,

        count(*) as transaction_count,

        avg(price) as average_sale_price,

        percentile_cont(0.5)
            within group (order by price) as median_sale_price

    from transactions

    group by
        transfer_year,
        transfer_month

)

select *
from monthly_market_trends

order by
    transfer_year,
    transfer_month