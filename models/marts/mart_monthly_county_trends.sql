with transactions as (

    select *
    from {{ ref('int_property_transactions') }}

),

monthly_county_trends as (

    select
        transfer_year,
        transfer_month,
        county,

        count(*) as transaction_count,

        avg(price) as average_sale_price,

        percentile_cont(0.5)
            within group (order by price) as median_sale_price

    from transactions

    group by
        transfer_year,
        transfer_month,
        county

)

select *
from monthly_county_trends

order by
    transfer_year,
    transfer_month,
    county