with ranked_markets as (

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

    from {{ ref('int_monthly_county_metrics') }}

)

select *
from ranked_markets

order by
    transfer_year,
    transfer_month,
    transaction_rank
