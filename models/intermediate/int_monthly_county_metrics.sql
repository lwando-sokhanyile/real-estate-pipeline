{{
    config(
        materialized='table'
    )
}}

select
    transfer_year,
    transfer_month,
    county,

    count(*) as transaction_count,

    avg(price) as average_sale_price,

    percentile_cont(0.5)
        within group (order by price) as median_sale_price

from {{ ref('int_property_transactions') }}

group by
    transfer_year,
    transfer_month,
    county
