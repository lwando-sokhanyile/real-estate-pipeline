with transactions as (

    select *
    from {{ ref('int_property_transactions') }}

),

property_type_trends as (

    select
        transfer_year,
        transfer_month,
        property_type_name,

        count(*) as transaction_count,

        avg(price) as average_sale_price,

        percentile_cont(0.5)
            within group (order by price) as median_sale_price

    from transactions

    group by
        transfer_year,
        transfer_month,
        property_type_name

)

select *
from property_type_trends

order by
    transfer_year,
    transfer_month,
    property_type_name