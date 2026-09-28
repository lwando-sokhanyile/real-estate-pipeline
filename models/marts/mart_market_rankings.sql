with monthly_county_metrics as (

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

),

ranked_markets as (

    select
        transfer_year,
        transfer_month,
        county,

        transaction_count,
        average_sale_price,
        median_sale_price,

        rank() over (
            partition by transfer_year, transfer_month
            order by transaction_count desc
        ) as transaction_rank,

        rank() over (
            partition by transfer_year, transfer_month
            order by average_sale_price desc
        ) as price_rank

    from monthly_county_metrics

)

select *
from ranked_markets

order by
    transfer_year,
    transfer_month,
    transaction_rank